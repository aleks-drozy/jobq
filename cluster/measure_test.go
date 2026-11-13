package cluster

import (
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aleks-drozy/jobq"
	"github.com/aleks-drozy/jobq/raft"
)

// TestMeasureSnapshots is not a test of correctness. It produces the
// numbers quoted in docs/specs for what snapshots change: disk use, the
// time a node takes to come back, and how long taking one stops a node.
// It runs only when asked:
//
//	JOBQ_MEASURE=100000 go test ./cluster -run TestMeasureSnapshots -v
//
// The value is the number of jobs. One durable node, fsync on every
// batch: each job is enqueued, and nine in ten are then leased and acked,
// so the log is about 2.8 entries a job while the queue ends up holding a
// tenth of them.
func TestMeasureSnapshots(t *testing.T) {
	jobs, _ := strconv.Atoi(os.Getenv("JOBQ_MEASURE"))
	if jobs <= 0 {
		t.Skip("set JOBQ_MEASURE to a number of jobs to run")
	}
	payload := make([]byte, 256)
	for name, every := range map[string]int{"no snapshots": -1, "snapshot every 16384": 16384} {
		dir := t.TempDir()
		open := func() (*Node, *jobq.RaftLog, time.Duration) {
			start := time.Now()
			log, _, err := jobq.OpenRaftLog(dir)
			if err != nil {
				t.Fatal(err)
			}
			n, err := Start(Config{ID: 1, Peers: []raft.NodeID{1}, Storage: log,
				Tick: 2 * time.Millisecond, ElectionTicks: 5, HeartbeatTicks: 1, SnapshotEvery: every})
			if err != nil {
				t.Fatal(err)
			}
			// Ready to serve: leading, with everything it holds applied.
			eventually(t, "the node to lead", func() bool {
				st := n.Status()
				return st.State == raft.Leader && st.Applied == st.LastIndex
			})
			return n, log, time.Since(start)
		}

		n, log, _ := open()
		var next atomic.Int64
		var wg sync.WaitGroup
		var slowest atomic.Int64
		loadStart := time.Now()
		for w := 0; w < 128; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					i := next.Add(1)
					if i > int64(jobs) {
						return
					}
					began := time.Now()
					if _, err := n.Enqueue("work", payload); err != nil {
						t.Error(err)
						return
					}
					if i%10 != 0 {
						_, lease, err := n.Dequeue("work", time.Hour)
						if err == nil {
							err = n.Ack(lease)
						}
						if err != nil {
							t.Error(err)
							return
						}
					}
					if took := int64(time.Since(began)); took > slowest.Load() {
						slowest.Store(took)
					}
				}
			}()
		}
		wg.Wait()
		loadTook := time.Since(loadStart)
		st, m := n.Status(), n.Metrics()
		logBytes, snapBytes := log.DiskUsage()
		_ = n.Close()
		_ = log.Close()

		n, log, reopen := open()
		after := n.Stats("work")
		_ = n.Close()
		_ = log.Close()
		if after.Enqueued != jobs {
			t.Fatalf("%s: reopened with %d jobs enqueued, want %d", name, after.Enqueued, jobs)
		}
		t.Logf("%s: %d jobs, %d log entries in %s (%.0f entries/s); on disk: log %.1f MB, snapshot %.1f MB; "+
			"log kept from index %d; %d snapshots, the last %d bytes in %s; slowest job (enqueue, lease, ack) %s; "+
			"restart to serving: %s",
			name, jobs, st.LastIndex, loadTook.Round(time.Millisecond), float64(st.LastIndex)/loadTook.Seconds(),
			float64(logBytes)/1e6, float64(snapBytes)/1e6, st.FirstIndex, m.Snapshots, m.LastSnapshotBytes,
			m.LastSnapshotTook.Round(time.Millisecond), time.Duration(slowest.Load()).Round(time.Millisecond),
			reopen.Round(time.Millisecond))
	}
}
