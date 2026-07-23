package jobq

// The crash harness: the P2 proof. The test re-executes its own binary as a
// child worker pointed at a shared directory. The child enqueues and
// consumes under SyncAlways, reporting every ACKNOWLEDGED enqueue and every
// completed (acked) job on stdout, line-buffered by explicit flushes. The
// parent kills it cold — Process.Kill, no signal handling, no flush
// opportunity — at a random moment, then recovers the directory and asserts
// the at-least-once contract:
//
//	LOSS  (forbidden): an enqueue the child saw acknowledged is neither
//	       present after recovery nor accounted terminal by the child.
//	GHOST (forbidden): a job the child acked complete is delivered again.
//	DUPES (permitted): counted, reported, and expected.
//
// Runs several rounds with logged seeds; skipped under -short.

import (
	"bufio"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

const crashChildEnv = "JOBQ_CRASH_CHILD_DIR"

func TestMain(m *testing.M) {
	if dir := os.Getenv(crashChildEnv); dir != "" {
		crashChildMain(dir)
		return // unreachable; crashChildMain never returns
	}
	os.Exit(m.Run())
}

// crashChildMain is the worker the parent kills. It never exits voluntarily.
func crashChildMain(dir string) {
	out := bufio.NewWriter(os.Stdout)
	q, _, err := Open(dir, WithSync(SyncAlways))
	if err != nil {
		fmt.Fprintf(out, "FATAL open: %v\n", err)
		out.Flush()
		os.Exit(3)
	}
	seq := 0
	for {
		// Enqueue a job whose payload is its own sequence number.
		id, err := q.Enqueue("crash", []byte(fmt.Sprintf("seq-%d", seq)))
		if err != nil {
			fmt.Fprintf(out, "FATAL enqueue: %v\n", err)
			out.Flush()
			os.Exit(3)
		}
		// Only after Enqueue returned is the job acknowledged: report it.
		fmt.Fprintf(out, "ENQ %s seq-%d\n", id, seq)
		out.Flush()
		seq++

		// Consume a little slower than we produce so a backlog builds up
		// and the kill lands with jobs in every state.
		if seq%3 != 0 {
			continue
		}
		job, lease, err := q.Dequeue("crash", time.Minute)
		if errors.Is(err, ErrNoJob) {
			continue
		}
		if err != nil {
			fmt.Fprintf(out, "FATAL dequeue: %v\n", err)
			out.Flush()
			os.Exit(3)
		}
		if err := q.Ack(lease); err != nil {
			continue // lease raced expiry: fine, the job stays owed
		}
		// Acked: this job must NEVER be seen again.
		fmt.Fprintf(out, "ACK %s %s\n", job.ID, job.Payload)
		out.Flush()
	}
}

// durablyAckedIDs walks the WAL segments the same way replayDir does and
// returns every job ID with a durable recAck. Torn tails are tolerated: a
// partially-written trailing record simply ends the scan, exactly as
// recovery treats it.
func durablyAckedIDs(t *testing.T, walDir string) map[string]bool {
	t.Helper()
	segs, err := listSegments(walDir)
	if err != nil {
		t.Fatal(err)
	}
	acked := map[string]bool{}
	for _, seg := range segs {
		raw, err := os.ReadFile(seg.path)
		if err != nil {
			t.Fatal(err)
		}
		buf := raw[segmentHeaderSize:]
		for len(buf) > 0 {
			rec, n, err := readRecord(buf)
			if err != nil {
				break // torn tail: nothing after it is durable
			}
			if rec.Type == recAck {
				acked[rec.JobID] = true
			}
			buf = buf[n:]
		}
	}
	return acked
}

func TestCrashRecoveryLosesNothingAcknowledged(t *testing.T) {
	if testing.Short() {
		t.Skip("crash harness skipped under -short")
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	const rounds = 5
	for round := 0; round < rounds; round++ {
		seed := time.Now().UnixNano()
		rng := rand.New(rand.NewSource(seed))
		t.Run(fmt.Sprintf("round=%d", round), func(t *testing.T) {
			t.Logf("seed %d", seed)
			dir := t.TempDir()

			cmd := exec.Command(exe, "-test.run", "XXX_NONE")
			cmd.Env = append(os.Environ(), crashChildEnv+"="+dir)
			var buf strings.Builder
			cmd.Stdout = &buf
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			// Let it work, then kill it cold mid-stride.
			time.Sleep(time.Duration(150+rng.Intn(600)) * time.Millisecond)
			if err := cmd.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			_ = cmd.Wait()

			// Parse the child's acknowledged history.
			acked := map[string]bool{}      // jobID -> child completed it
			enqueued := map[string]string{} // jobID -> payload, acknowledged enqueues only
			for _, line := range strings.Split(buf.String(), "\n") {
				parts := strings.Fields(line)
				switch {
				case len(parts) == 3 && parts[0] == "ENQ":
					enqueued[parts[1]] = parts[2]
				case len(parts) == 3 && parts[0] == "ACK":
					acked[parts[1]] = true
				case len(parts) > 0 && parts[0] == "FATAL":
					t.Fatalf("child failed before the kill: %s", line)
				}
			}
			if len(enqueued) == 0 {
				t.Skip("child was killed before acknowledging anything; nothing to assert")
			}

			// Remove the crashed process's lock: the owner is provably dead.
			if err := os.Remove(dir + string(os.PathSeparator) + "jobq.lock"); err != nil && !os.IsNotExist(err) {
				t.Fatal(err)
			}

			q, rep, err := Open(dir, WithSync(SyncAlways))
			if err != nil {
				t.Fatalf("recovery failed: %v", err)
			}
			defer func() { _ = q.Close() }()
			t.Logf("child acked-enqueues=%d acked-complete=%d; recovery: %+v",
				len(enqueued), len(acked), rep)

			// Drain everything recoverable.
			recovered := map[string]int{}
			for _, topic := range []string{"crash", DeadLetterTopic("crash")} {
				for {
					job, lease, err := q.Dequeue(topic, time.Minute)
					if errors.Is(err, ErrNoJob) {
						break
					}
					if err != nil {
						t.Fatal(err)
					}
					recovered[job.ID]++
					if acked[job.ID] {
						t.Errorf("GHOST: job %s (%s) was acked by the child but delivered again",
							job.ID, job.Payload)
					}
					if err := q.Ack(lease); err != nil {
						t.Fatal(err)
					}
				}
			}
			// The child prints ACK only after Ack returns, and Ack returns
			// only after its record is fsynced (rule 2) - so a kill can land
			// BETWEEN the durable ack and the print. Such a job is neither
			// restored nor reported, but it is not lost: its ack is on disk.
			// Read the WAL directly and accept a durable recAck as proof of
			// settlement. (The mirror race is impossible in this direction:
			// a printed ACK implies a durable record, which is what makes
			// the GHOST check above sound.)
			durableAcks := durablyAckedIDs(t, walSubdir(dir))
			lost, unprinted := 0, 0
			for id, payload := range enqueued {
				if !acked[id] && recovered[id] == 0 {
					if durableAcks[id] {
						unprinted++ // durably settled; only the report was cut off
						continue
					}
					lost++
					t.Errorf("LOST: acknowledged enqueue %s (%s) is gone", id, payload)
				}
			}
			if lost == 0 {
				t.Logf("contract held: 0 lost, %d recovered, %d acked-but-unreported, duplicates permitted",
					len(recovered), unprinted)
			}
		})
	}
}
