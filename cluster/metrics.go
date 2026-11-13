package cluster

import (
	"sync/atomic"
	"time"
)

// Metrics is what a node has counted since it started. None of it is
// replicated: each node counts for itself and starts again from zero.
type Metrics struct {
	// LeaderChanges is how many times this node has learned of a leader
	// different from the last one it knew. The first leader it hears of
	// counts.
	LeaderChanges uint64
	// Snapshots is how many snapshots this node has taken of its own
	// queue, SnapshotsLoaded how many it has loaded: one at a restart from
	// a snapshot, and one each time a leader sent it one.
	Snapshots       uint64
	SnapshotsLoaded uint64
	// LastSnapshotBytes and LastSnapshotTook describe the newest snapshot
	// this node took. The node does nothing else for that long.
	LastSnapshotBytes uint64
	LastSnapshotTook  time.Duration
	// DedupedEnqueues is how many enqueues this node recognised by their
	// request ID as already done, and answered without adding a job.
	DedupedEnqueues uint64
	// Commit is how long operations proposed on this node while it led
	// took from proposal to being applied: the fsync, a round trip to a
	// majority, and the queue's own work.
	Commit Histogram
}

// Histogram is a cumulative latency histogram. Counts[i] is how many
// observations were at most Bounds[i]; observations above the last bound
// are in Count only.
type Histogram struct {
	Bounds []time.Duration
	Counts []uint64
	Count  uint64
	Sum    time.Duration
}

var commitBounds = []time.Duration{
	500 * time.Microsecond, time.Millisecond, 2 * time.Millisecond, 5 * time.Millisecond,
	10 * time.Millisecond, 25 * time.Millisecond, 50 * time.Millisecond, 100 * time.Millisecond,
	250 * time.Millisecond, 500 * time.Millisecond, time.Second, 5 * time.Second,
}

type metrics struct {
	leaderChanges   atomic.Uint64
	snapshots       atomic.Uint64
	snapshotsLoaded atomic.Uint64
	snapshotBytes   atomic.Uint64
	snapshotNanos   atomic.Uint64
	commit          histogram
}

// histogram counts each observation in the one bucket it falls in; the
// cumulative form Prometheus wants is built when it is read.
type histogram struct {
	buckets [13]atomic.Uint64 // one per bound, and one for everything above
	sum     atomic.Uint64     // nanoseconds
}

func (h *histogram) observe(d time.Duration) {
	if d < 0 {
		d = 0
	}
	i := 0
	for i < len(commitBounds) && d > commitBounds[i] {
		i++
	}
	h.buckets[i].Add(1)
	h.sum.Add(uint64(d))
}

func (h *histogram) read() Histogram {
	out := Histogram{Bounds: commitBounds, Counts: make([]uint64, len(commitBounds))}
	var running uint64
	for i := range commitBounds {
		running += h.buckets[i].Load()
		out.Counts[i] = running
	}
	out.Count = running + h.buckets[len(commitBounds)].Load()
	out.Sum = time.Duration(h.sum.Load())
	return out
}

// Metrics reports what the node has counted so far.
func (n *Node) Metrics() Metrics {
	return Metrics{
		LeaderChanges:     n.metrics.leaderChanges.Load(),
		Snapshots:         n.metrics.snapshots.Load(),
		SnapshotsLoaded:   n.metrics.snapshotsLoaded.Load(),
		LastSnapshotBytes: n.metrics.snapshotBytes.Load(),
		LastSnapshotTook:  time.Duration(n.metrics.snapshotNanos.Load()),
		DedupedEnqueues:   n.sm.deduped.Load(),
		Commit:            n.metrics.commit.read(),
	}
}
