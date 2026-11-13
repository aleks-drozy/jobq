package cluster

import (
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aleks-drozy/jobq"
	"github.com/aleks-drozy/jobq/raft"
)

func sampleState() state {
	at := time.Date(2026, 11, 12, 9, 30, 0, 123456789, time.UTC)
	job := func(id string, attempts int) *jobq.Job {
		return &jobq.Job{ID: id, Topic: "work", Payload: []byte("payload-" + id), Attempt: attempts,
			Attempts: attempts, MaxAttempts: 5, EnqueuedAt: at}
	}
	delayed := job("b", 0)
	delayed.NotBefore = at.Add(time.Minute)
	dead := &jobq.Job{ID: "d", Topic: "work.dlq", MaxAttempts: 5, EnqueuedAt: at,
		DeadLetteredAt: at.Add(time.Hour), DeadLetteredAttempts: 3}
	return state{
		clock: at.UnixNano(),
		dedup: []dedupEntry{{requestID: "req-1", jobID: "a", at: at.UnixNano()}, {requestID: "req-2", jobID: "b", at: at.UnixNano() + 5}},
		queue: jobq.QueueState{Topics: []jobq.TopicState{
			{
				Name:   "work",
				Ready:  []*jobq.Job{job("a", 1), delayed},
				Leased: []jobq.LeasedJob{{Job: job("c", 2), LeaseID: "lease-c", Deadline: at.Add(30 * time.Second)}},
				Stats:  jobq.Stats{Ready: 2, InFlight: 1, Enqueued: 9, Acked: 4, DeadLetters: 1, Dropped: 0},
			},
			{Name: "work.dlq", Ready: []*jobq.Job{dead}, Stats: jobq.Stats{Ready: 1, Enqueued: 1, Dropped: 2}},
			{Name: "empty"},
		}},
	}
}

func TestSnapshotRoundTrip(t *testing.T) {
	want := sampleState()
	got, err := decodeState(want.encode())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("round trip changed the state:\n got %+v\nwant %+v", got, want)
	}
	// The zero time, which means "not set" on several job fields, must
	// come back as the zero time and not as some instant in year 1.
	if nb := got.queue.Topics[0].Ready[0].NotBefore; !nb.IsZero() {
		t.Fatalf("an unset NotBefore came back as %v", nb)
	}

	empty, err := decodeState(state{}.encode())
	if err != nil || len(empty.queue.Topics) != 0 || len(empty.dedup) != 0 {
		t.Fatalf("empty state: %+v, %v", empty, err)
	}
}

func TestSnapshotDecoderSurvivesGarbage(t *testing.T) {
	good := sampleState().encode()
	for cut := 0; cut < len(good); cut++ {
		if _, err := decodeState(good[:cut]); err == nil {
			t.Fatalf("a snapshot cut to %d of %d bytes decoded", cut, len(good))
		}
	}
	if _, err := decodeState(append(append([]byte(nil), good...), 0)); err == nil {
		t.Fatal("a snapshot with a byte after its end decoded")
	}
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 20000; i++ {
		b := append([]byte(nil), good...)
		for k := 1 + rng.Intn(3); k > 0; k-- {
			b[rng.Intn(len(b))] = byte(rng.Intn(256))
		}
		_, _ = decodeState(b) // must not panic or allocate without bound
	}
	// A length that promises more items than there are bytes for.
	huge := append([]byte(snapshotMagic), make([]byte, 8)...)
	huge = append(huge, 0xff, 0xff, 0xff, 0xff, 0x0f)
	if _, err := decodeState(huge); err == nil {
		t.Fatal("a snapshot claiming four billion request IDs decoded")
	}
}

// A node that was down while the others compacted has to be sent the
// snapshot: the entries it is missing no longer exist anywhere.
func TestFollowerTooFarBehindIsCaughtUpBySnapshot(t *testing.T) {
	c := newTestCluster(t, 3)
	c.snapEvery, c.snapKeep = 25, 0
	c.startAll()
	lead := c.leader()
	var behind raft.NodeID
	for _, id := range c.ids {
		if id != lead.cfg.ID {
			behind = id
		}
	}
	mustEnqueue(t, c, "work", "before")
	c.converged()
	c.stop(behind)

	// Work the others do alone: enqueues, a lease left open, an ack, a
	// delayed job, and a job driven into the dead-letter topic.
	for i := 0; i < 60; i++ {
		mustEnqueue(t, c, "work", fmt.Sprintf("job-%d", i))
	}
	mustEnqueue(t, c, "work", "later", jobq.WithDelay(time.Hour))
	mustEnqueue(t, c, "poison", "bad", jobq.WithMaxAttempts(1))
	_, open, err := c.dequeue("work", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	_, done, err := c.dequeue("work", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.ack(done); err != nil {
		t.Fatal(err)
	}
	_, bad, err := c.dequeue("poison", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.do(func(n *Node) error { return n.Nack(bad) }); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the leader to compact past what the stopped node holds", func() bool {
		return c.leader().Status().FirstIndex > 10
	})

	n := c.start(behind)
	c.converged()
	if loaded := n.Metrics().SnapshotsLoaded; loaded == 0 {
		t.Fatal("the node caught up without loading a snapshot; the test proved nothing")
	}
	want := snapshot(c.leader())
	if got := snapshot(n); got != want {
		t.Fatalf("the node caught up by snapshot differs from the leader.\n--- leader\n%s\n--- node %d\n%s", want, behind, got)
	}
	if !strings.Contains(want, "poison.dlq") || !strings.Contains(want, "inflight") {
		t.Fatalf("history too thin to prove anything:\n%s", want)
	}
	// The lease taken before the snapshot is still live on the node that
	// never saw it issued.
	if err := c.ack(open); err != nil {
		t.Fatalf("acking a lease issued before the snapshot: %v", err)
	}
	c.converged()
	if st := n.Stats("work"); st.Acked != 2 || st.InFlight != 0 {
		t.Fatalf("after the ack the caught-up node has %+v", st)
	}
}

// Snapshots must not change what a restart means.
func TestDurableNodeRestartsFromItsSnapshot(t *testing.T) {
	dir := t.TempDir()
	open := func() (*Node, *jobq.RaftLog, jobq.RaftLogReport) {
		log, rep, err := jobq.OpenRaftLog(dir, jobq.WithSegmentSize(2048))
		if err != nil {
			t.Fatal(err)
		}
		n, err := Start(Config{ID: 1, Peers: []raft.NodeID{1}, Storage: log,
			Tick: 2 * time.Millisecond, ElectionTicks: 5, HeartbeatTicks: 1,
			SnapshotEvery: 40, SnapshotKeep: 5})
		if err != nil {
			t.Fatal(err)
		}
		return n, log, rep
	}
	lead := func(n *Node) {
		eventually(t, "the node to lead", func() bool {
			st := n.Status()
			return st.State == raft.Leader && st.Applied == st.LastIndex
		})
	}

	n, log, _ := open()
	lead(n)
	payload := []byte(strings.Repeat("x", 200))
	for i := 0; i < 300; i++ {
		if _, err := n.Enqueue("work", payload); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 250; i++ {
		_, lease, err := n.Dequeue("work", time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		if err := n.Ack(lease); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := n.Enqueue("work", []byte("once"), jobq.WithRequestID("req-1")); err != nil {
		t.Fatal(err)
	}
	before := snapshot(n)
	if m := n.Metrics(); m.Snapshots < 10 {
		t.Fatalf("took %d snapshots over 800 entries at one per 40", m.Snapshots)
	}
	logBytes, snapBytes := log.DiskUsage()
	// 800 entries, 300 of them carrying 200 bytes, is well over 60 KB of
	// log. What is left should be the last few entries and one snapshot of
	// the 51 jobs still queued.
	if logBytes > 20_000 || snapBytes == 0 || snapBytes > 30_000 {
		t.Fatalf("after compaction the log takes %d bytes and the snapshot %d", logBytes, snapBytes)
	}
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}

	n, log, rep := open()
	defer log.Close()
	defer n.Close()
	if rep.SnapshotIndex == 0 || rep.Entries > 50 {
		t.Fatalf("reopening found %+v, want a snapshot and a short log", rep)
	}
	lead(n)
	if n.Metrics().SnapshotsLoaded != 1 {
		t.Fatalf("restart loaded %d snapshots, want 1", n.Metrics().SnapshotsLoaded)
	}
	if after := snapshot(n); after != before {
		t.Fatalf("the queue changed across a restart from a snapshot.\n--- before\n%s\n--- after\n%s", before, after)
	}
	// The request ID table came back too.
	id, err := n.Enqueue("work", []byte("once"), jobq.WithRequestID("req-1"))
	if err != nil {
		t.Fatal(err)
	}
	if st := n.Stats("work"); st.Enqueued != 301 {
		t.Fatalf("a retried enqueue added a job after the restart: %+v (returned %s)", st, id)
	}
}

func TestRequestIDMakesAnEnqueueHappenOnce(t *testing.T) {
	c := newTestCluster(t, 3).startAll()
	c.leader()

	first := mustEnqueue(t, c, "work", "pay", jobq.WithRequestID("order-17"))
	again := mustEnqueue(t, c, "work", "pay", jobq.WithRequestID("order-17"))
	if again != first {
		t.Fatalf("the retry got job %s, the original got %s", again, first)
	}
	other := mustEnqueue(t, c, "work", "pay", jobq.WithRequestID("order-18"))
	plain1 := mustEnqueue(t, c, "work", "pay")
	plain2 := mustEnqueue(t, c, "work", "pay")
	if other == first || plain1 == plain2 {
		t.Fatal("enqueues that should be distinct were merged")
	}
	c.converged()
	for _, n := range c.live() {
		if st := n.Stats("work"); st.Enqueued != 4 {
			t.Fatalf("node %d has %d jobs, want 4 (one for the repeated ID)", n.cfg.ID, st.Enqueued)
		}
	}
	if n := c.leader(); n.Metrics().DedupedEnqueues != 1 {
		t.Fatalf("leader counted %d deduplicated enqueues, want 1", n.Metrics().DedupedEnqueues)
	}

	// The case it exists for: the leader takes the enqueue and dies before
	// the caller hears back. The retry lands on a different node.
	old := c.leader()
	c.stop(old.cfg.ID)
	retry := mustEnqueue(t, c, "work", "pay", jobq.WithRequestID("order-17"))
	if retry != first {
		t.Fatalf("after a change of leader the retry got job %s, want %s", retry, first)
	}
	// And across enough entries that the table has been through snapshots,
	// on a node that rebuilds from one.
	for i := 0; i < 80; i++ {
		mustEnqueue(t, c, "filler", fmt.Sprintf("f-%d", i))
	}
	back := c.start(old.cfg.ID)
	c.converged()
	if back.Metrics().SnapshotsLoaded == 0 {
		t.Fatal("the restarted node did not load a snapshot; the test proved nothing")
	}
	c.stop(c.leader().cfg.ID)
	if retry := mustEnqueue(t, c, "work", "pay", jobq.WithRequestID("order-17")); retry != first {
		t.Fatalf("after snapshots and another change of leader the retry got job %s, want %s", retry, first)
	}
	c.converged()
	for _, n := range c.live() {
		if st := n.Stats("work"); st.Enqueued != 4 {
			t.Fatalf("node %d has %d jobs on work, want 4", n.cfg.ID, st.Enqueued)
		}
	}

	if _, err := c.enqueue("work", nil, jobq.WithRequestID(strings.Repeat("x", jobq.MaxRequestIDLen+1))); !errors.Is(err, jobq.ErrRequestIDTooLong) {
		t.Fatalf("an oversized request ID: %v, want ErrRequestIDTooLong", err)
	}
}

// The table forgets. An ID reused after the window is a new enqueue, and
// every replica must agree on the moment it became one.
func TestRequestIDIsForgottenAfterTheWindow(t *testing.T) {
	c := newTestCluster(t, 3).startAll()
	c.leader()
	first := mustEnqueue(t, c, "work", "a", jobq.WithRequestID("r"))
	c.clock.Advance(DedupWindow - time.Second)
	if got := mustEnqueue(t, c, "work", "a", jobq.WithRequestID("r")); got != first {
		t.Fatalf("inside the window the retry got a new job %s", got)
	}
	c.clock.Advance(2 * time.Second)
	later := mustEnqueue(t, c, "work", "a", jobq.WithRequestID("r"))
	if later == first {
		t.Fatal("after the window the ID was still remembered")
	}
	// The ID now names the newer job, for a fresh window.
	if got := mustEnqueue(t, c, "work", "a", jobq.WithRequestID("r")); got != later {
		t.Fatalf("the reused ID resolves to %s, want the newer job %s", got, later)
	}
	c.converged()
	want := snapshot(c.live()[0])
	for _, n := range c.live()[1:] {
		if got := snapshot(n); got != want {
			t.Fatalf("replicas disagree about when the ID was forgotten.\n%s\n%s", want, got)
		}
	}
	if st := c.leader().Stats("work"); st.Enqueued != 2 {
		t.Fatalf("%d jobs, want 2", st.Enqueued)
	}
}

func TestDedupTableIsBounded(t *testing.T) {
	m := newMachine()
	defer m.queue().Close()
	at := time.Date(2026, 11, 12, 0, 0, 0, 0, time.UTC)
	apply := func(i uint64, c command) result {
		return m.apply(raft.Entry{Index: i, Term: 1, Data: c.encode()})
	}
	for i := uint64(1); i <= 1000; i++ {
		c := command{At: at.Add(time.Duration(i) * time.Second), Op: opEnqueue, Topic: "t", RequestID: fmt.Sprintf("r%d", i)}
		if r := apply(i, c); r.err != nil {
			t.Fatal(r.err)
		}
	}
	// One enqueue a second, a five-minute window: about 300 remembered.
	if n := len(m.order); n < 295 || n > 305 || len(m.seen) != n {
		t.Fatalf("table holds %d entries (%d in the index) after 1000 enqueues a second apart", len(m.order), len(m.seen))
	}
}

// Every node counts its own entries, so they snapshot at different points
// in the log. That must not show.
func TestNodesSnapshotIndependentlyAndAgree(t *testing.T) {
	c := newTestCluster(t, 3)
	c.snapEvery, c.snapKeep = 30, 10
	c.startAll()
	c.leader()
	for i := 0; i < 200; i++ {
		mustEnqueue(t, c, "work", fmt.Sprintf("job-%d", i))
		if i%3 == 0 {
			if _, lease, err := c.dequeue("work", time.Hour); err == nil && i%2 == 0 {
				_ = c.ack(lease)
			}
		}
	}
	c.converged()
	want := snapshot(c.live()[0])
	for _, n := range c.live() {
		st, m := n.Status(), n.Metrics()
		if m.Snapshots < 5 || st.SnapshotIndex == 0 || st.FirstIndex <= 1 {
			t.Fatalf("node %d took %d snapshots; its log still starts at %d", n.cfg.ID, m.Snapshots, st.FirstIndex)
		}
		if st.LastIndex-st.FirstIndex > 60 {
			t.Fatalf("node %d still holds %d entries after compacting every 30 keeping 10", n.cfg.ID, st.LastIndex-st.FirstIndex+1)
		}
		if got := snapshot(n); got != want {
			t.Fatalf("node %d differs.\n%s\n%s", n.cfg.ID, want, got)
		}
	}
}

// A snapshot from the network replaces a node's log for good, so one that
// would not load has to be turned away before it is stored. Otherwise a
// single damaged message leaves a node that stops, and stops again every
// time it is started.
func TestSnapshotThatWouldNotLoadIsRefused(t *testing.T) {
	c := newTestCluster(t, 3).startAll()
	lead := c.leader()
	var victim *Node
	for _, n := range c.live() {
		if n.cfg.ID != lead.cfg.ID {
			victim = n
		}
	}
	for i := 0; i < 10; i++ {
		mustEnqueue(t, c, "work", fmt.Sprintf("job-%d", i))
	}
	c.converged()
	before := snapshot(victim)
	term := lead.Status().Term

	// Well-formed and checksummed, but not a queue: one topic, twice.
	twice := state{queue: jobq.QueueState{Topics: []jobq.TopicState{{Name: "work"}, {Name: "work"}}}}.encode()
	flipped := sampleState().encode()
	flipped[len(flipped)/2] ^= 0x01
	bad := [][]byte{nil, []byte("not a snapshot"), flipped, twice}
	for _, data := range bad {
		victim.Step(raft.Message{Type: raft.MsgSnapshot, From: lead.cfg.ID, To: victim.cfg.ID, Term: term,
			Snapshot: &raft.Snapshot{Index: 1 << 40, Term: term, Data: data}})
	}
	eventually(t, "the node to refuse every bad snapshot", func() bool {
		return victim.Metrics().SnapshotsRefused == uint64(len(bad))
	})

	select {
	case <-victim.Done():
		t.Fatalf("the node stopped: %v", victim.Err())
	default:
	}
	if got := snapshot(victim); got != before {
		t.Fatalf("a refused snapshot changed the queue.\n--- before\n%s\n--- after\n%s", before, got)
	}
	if st := victim.Status(); st.SnapshotIndex >= 1<<40 || st.LastIndex >= 1<<40 {
		t.Fatalf("a refused snapshot moved the node's log: %+v", st)
	}
	// And it is still a working member.
	mustEnqueue(t, c, "work", "after")
	c.converged()
	if got, want := snapshot(victim), snapshot(c.leader()); got != want {
		t.Fatalf("the node no longer follows the leader.\n%s\n%s", want, got)
	}
}

func TestSnapshotChecksumCatchesAFlippedBit(t *testing.T) {
	good := sampleState().encode()
	for i := 0; i < len(good); i++ {
		b := append([]byte(nil), good...)
		b[i] ^= 0x10
		if _, err := decodeState(b); err == nil {
			t.Fatalf("a snapshot with byte %d of %d changed decoded", i, len(good))
		}
	}
}
