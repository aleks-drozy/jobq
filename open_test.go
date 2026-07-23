package jobq

import (
	"errors"
	"testing"
	"time"
)

// The end-to-end durability contract: everything a live queue was holding
// comes back after Close+Open with payloads, order, attempt budgets and
// DLQ evidence intact — and nothing acked or dropped is resurrected.
func TestOpenRoundTripsLiveState(t *testing.T) {
	dir := t.TempDir()
	q, rep, err := Open(dir, WithSync(SyncAlways))
	if err != nil {
		t.Fatal(err)
	}
	if rep.Segments != 0 || rep.JobsRestored != 0 {
		t.Fatalf("fresh dir report: %+v", rep)
	}

	if _, err := q.Enqueue("mail", []byte("keep-1")); err != nil {
		t.Fatal(err)
	}
	ackID, err := q.Enqueue("mail", []byte("ack-me"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := q.Enqueue("mail", []byte("keep-2"), WithDelay(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Enqueue("mail", []byte("poison"), WithMaxAttempts(1)); err != nil {
		t.Fatal(err)
	}

	// Ack one...
	seen := map[string]string{}
	for i := 0; i < 3; i++ { // keep-1, ack-me, poison are visible; delayed is not
		job, lease, err := q.Dequeue("mail", time.Minute)
		if err != nil {
			t.Fatalf("dequeue %d: %v", i, err)
		}
		seen[string(job.Payload)] = job.ID
		switch string(job.Payload) {
		case "ack-me":
			if err := q.Ack(lease); err != nil {
				t.Fatal(err)
			}
		case "poison": // ...dead-letter another...
			if err := q.Nack(lease); err != nil {
				t.Fatal(err)
			}
		default: // ...and leave keep-1 in flight at "crash" time.
		}
	}
	if seen["ack-me"] != ackID {
		t.Fatalf("ack id bookkeeping broke")
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}

	q2, rep2, err := Open(dir, WithSync(SyncAlways))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = q2.Close() }()
	if !rep2.CleanShutdown {
		t.Errorf("clean close not detected: %+v", rep2)
	}

	// keep-1 was in flight: restart expires the lease, so it returns ready
	// with one attempt consumed. keep-2 is still delayed. ack-me is gone.
	got := map[string]*Job{}
	for {
		job, lease, err := q2.Dequeue("mail", time.Minute)
		if errors.Is(err, ErrNoJob) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		got[string(job.Payload)] = job
		if err := q2.Ack(lease); err != nil {
			t.Fatal(err)
		}
	}
	if len(got) != 1 || got["keep-1"] == nil {
		t.Fatalf("visible after reopen: %v (want only keep-1; keep-2 is delayed)", keysOf(got))
	}
	if got["keep-1"].Attempt != 2 {
		t.Errorf("keep-1 Attempt = %d, want 2 (one consumed before the restart)", got["keep-1"].Attempt)
	}

	dlqJob, dlqLease, err := q2.Dequeue(DeadLetterTopic("mail"), time.Minute)
	if err != nil {
		t.Fatalf("dlq after reopen: %v", err)
	}
	if string(dlqJob.Payload) != "poison" || dlqJob.DeadLetteredAttempts != 1 {
		t.Fatalf("dlq job after reopen: %q %+v", dlqJob.Payload, dlqJob)
	}
	if err := q2.Ack(dlqLease); err != nil {
		t.Fatal(err)
	}

	if st := q2.Stats("mail"); st.Ready != 1 {
		t.Errorf("delayed job lost: Stats = %+v", st)
	}
}

func keysOf(m map[string]*Job) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func TestOpenSurvivesRepeatedCycles(t *testing.T) {
	// State must be stable across many open/close cycles, not just one:
	// re-logging or double-restoring shows up as growth here.
	dir := t.TempDir()
	for cycle := 0; cycle < 5; cycle++ {
		q, rep, err := Open(dir, WithSync(SyncAlways))
		if err != nil {
			t.Fatalf("cycle %d: %v", cycle, err)
		}
		if cycle > 0 && rep.JobsRestored != 3 {
			t.Fatalf("cycle %d restored %d jobs, want 3", cycle, rep.JobsRestored)
		}
		if cycle == 0 {
			for i := 0; i < 3; i++ {
				if _, err := q.Enqueue("stable", []byte{byte(i)}); err != nil {
					t.Fatal(err)
				}
			}
		}
		if err := q.Close(); err != nil {
			t.Fatalf("cycle %d close: %v", cycle, err)
		}
	}
}

func TestOpenRefusesConcurrentProcess(t *testing.T) {
	dir := t.TempDir()
	q, _, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = q.Close() }()
	if _, _, err := Open(dir); err == nil {
		t.Fatal("second Open on a locked dir succeeded")
	}
}
