package jobq

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/aleks-drozy/jobq/raft"
)

func raftEntries(from, to, term uint64) []raft.Entry {
	var out []raft.Entry
	for i := from; i <= to; i++ {
		out = append(out, raft.Entry{Index: i, Term: term, Data: []byte(fmt.Sprintf("e%d", i))})
	}
	return out
}

func mustOpenRaftLog(t *testing.T, dir string) (*RaftLog, RaftLogReport) {
	t.Helper()
	l, rep, err := OpenRaftLog(dir)
	if err != nil {
		t.Fatalf("OpenRaftLog: %v", err)
	}
	return l, rep
}

func TestRaftLogSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	l, rep := mustOpenRaftLog(t, dir)
	if rep.Entries != 0 || rep.Term != 0 {
		t.Fatalf("fresh log reported %+v", rep)
	}
	if _, _, err := l.Load(); err != nil {
		t.Fatal(err)
	}
	if err := l.SaveHardState(raft.HardState{Term: 3, VotedFor: 2}); err != nil {
		t.Fatal(err)
	}
	if err := l.Append(raftEntries(1, 5, 3)); err != nil {
		t.Fatal(err)
	}
	noop := raft.Entry{Index: 6, Term: 4, Kind: raft.EntryNoop}
	if err := l.Append([]raft.Entry{noop}); err != nil {
		t.Fatal(err)
	}
	if err := l.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	l, rep = mustOpenRaftLog(t, dir)
	defer l.Close()
	hard, entries, err := l.Load()
	if err != nil {
		t.Fatal(err)
	}
	if hard != (raft.HardState{Term: 3, VotedFor: 2}) {
		t.Errorf("hard state = %+v", hard)
	}
	if len(entries) != 6 || rep.Entries != 6 {
		t.Fatalf("got %d entries (report %d), want 6", len(entries), rep.Entries)
	}
	for i, e := range entries[:5] {
		if e.Index != uint64(i+1) || e.Term != 3 || e.Kind != raft.EntryNormal ||
			!bytes.Equal(e.Data, []byte(fmt.Sprintf("e%d", i+1))) {
			t.Errorf("entry %d came back as %+v", i+1, e)
		}
	}
	if got := entries[5]; got.Index != 6 || got.Term != 4 || got.Kind != raft.EntryNoop || len(got.Data) != 0 {
		t.Errorf("noop came back as %+v", got)
	}
	// The reopened log must continue where the old one stopped.
	if err := l.Append(raftEntries(7, 7, 4)); err != nil {
		t.Fatalf("append after reopen: %v", err)
	}
}

// A truncation is a record, not an edit: the entries it removes are still
// on disk, and replay must come to the same shorter log every time.
func TestRaftLogTruncateIsReplayed(t *testing.T) {
	dir := t.TempDir()
	l, _ := mustOpenRaftLog(t, dir)
	if err := l.Append(raftEntries(1, 6, 1)); err != nil {
		t.Fatal(err)
	}
	if err := l.TruncateFrom(4); err != nil {
		t.Fatal(err)
	}
	if err := l.Append(raftEntries(4, 5, 2)); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	for pass := 0; pass < 2; pass++ {
		l, _ = mustOpenRaftLog(t, dir)
		_, entries, err := l.Load()
		if err != nil {
			t.Fatal(err)
		}
		var terms []uint64
		for _, e := range entries {
			terms = append(terms, e.Term)
		}
		if fmt.Sprint(terms) != "[1 1 1 2 2]" {
			t.Fatalf("pass %d: terms after replay = %v, want [1 1 1 2 2]", pass, terms)
		}
		if err := l.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRaftLogRejectsGapsAndBadTruncates(t *testing.T) {
	l, _ := mustOpenRaftLog(t, t.TempDir())
	defer l.Close()
	if err := l.TruncateFrom(1); err == nil {
		t.Error("truncating an empty log succeeded")
	}

	l2, _ := mustOpenRaftLog(t, t.TempDir())
	defer l2.Close()
	if err := l2.Append(raftEntries(2, 2, 1)); err == nil {
		t.Fatal("append with a gap succeeded")
	}
	// Fail closed: a log that was asked to do something impossible does
	// not carry on as if nothing happened.
	if err := l2.Append(raftEntries(1, 1, 1)); err == nil {
		t.Error("log accepted a write after failing")
	}
	if err := l2.Sync(); err == nil {
		t.Error("Sync succeeded on a failed log")
	}
}

// A crash mid-write leaves a partial record at the very end. Recovery must
// drop exactly that record and keep everything before it: the write was
// never synced, so nothing was ever promised on the strength of it.
func TestRaftLogTornTailIsDropped(t *testing.T) {
	dir := t.TempDir()
	l, _ := mustOpenRaftLog(t, dir)
	if err := l.SaveHardState(raft.HardState{Term: 2, VotedFor: 1}); err != nil {
		t.Fatal(err)
	}
	if err := l.Append(raftEntries(1, 4, 2)); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	segs, err := listSegments(walSubdir(dir))
	if err != nil || len(segs) == 0 {
		t.Fatalf("listSegments: %v (%d segments)", err, len(segs))
	}
	last := segs[len(segs)-1].path
	fi, err := os.Stat(last)
	if err != nil {
		t.Fatal(err)
	}
	// Close appended an empty CLEANSHUT frame (frameHeaderSize bytes).
	// Cutting that plus a few bytes more leaves entry 4 half-written.
	if err := os.Truncate(last, fi.Size()-frameHeaderSize-3); err != nil {
		t.Fatal(err)
	}

	l, rep := mustOpenRaftLog(t, dir)
	defer l.Close()
	if !rep.TornTail || rep.BytesTruncated == 0 {
		t.Errorf("report did not record the torn tail: %+v", rep)
	}
	hard, entries, err := l.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 3 {
		t.Fatalf("recovered %d entries, want 3", len(entries))
	}
	if hard.Term != 2 || hard.VotedFor != 1 {
		t.Errorf("hard state lost with the torn tail: %+v", hard)
	}
	if err := l.Append(raftEntries(4, 4, 2)); err != nil {
		t.Fatalf("could not rewrite the torn entry: %v", err)
	}
}

// One directory, one kind of log. Opening the wrong kind must fail loudly:
// the alternative is a node that starts empty and looks healthy.
func TestRaftLogAndQueueRefuseEachOthersDirectories(t *testing.T) {
	qdir := t.TempDir()
	q, _, err := Open(qdir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := q.Enqueue("t", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := OpenRaftLog(qdir); err == nil {
		t.Error("OpenRaftLog accepted a single-node queue directory")
	}
	if _, err := os.Stat(qdir + string(os.PathSeparator) + "jobq.lock"); !os.IsNotExist(err) {
		t.Error("a refused OpenRaftLog left the directory locked")
	}

	rdir := t.TempDir()
	l, _ := mustOpenRaftLog(t, rdir)
	if err := l.Append(raftEntries(1, 1, 1)); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Open(rdir); !errors.Is(err, errRaftLogDir) {
		t.Errorf("Open on a raft log directory: err = %v, want errRaftLogDir", err)
	}
}

func TestRaftLogLoadOnlyOnce(t *testing.T) {
	l, _ := mustOpenRaftLog(t, t.TempDir())
	defer l.Close()
	if _, _, err := l.Load(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := l.Load(); err == nil {
		t.Error("second Load succeeded; it would have returned an empty log")
	}
}

// The log must be usable as the storage of a real node: elect, propose,
// restart from disk, and still hold the committed entry.
func TestRaftLogDrivesANode(t *testing.T) {
	dir := t.TempDir()
	newNode := func() (*raft.Node, *RaftLog) {
		l, _ := mustOpenRaftLog(t, dir)
		n, err := raft.New(raft.Config{ID: 1, Peers: []raft.NodeID{1}, ElectionTicks: 3, HeartbeatTicks: 1, Storage: l})
		if err != nil {
			t.Fatal(err)
		}
		return n, l
	}
	n, l := newNode()
	for i := 0; i < 10 && n.Status().State != raft.Leader; i++ {
		if err := n.Tick(); err != nil {
			t.Fatal(err)
		}
	}
	if n.Status().State != raft.Leader {
		t.Fatal("single node never elected itself")
	}
	if _, _, err := n.Propose([]byte("job")); err != nil {
		t.Fatal(err)
	}
	if err := l.Sync(); err != nil {
		t.Fatal(err)
	}
	term := n.Status().Term
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	n, l = newNode()
	defer l.Close()
	st := n.Status()
	if st.Term != term || st.LastIndex != 2 {
		t.Fatalf("after restart: term %d last index %d, want term %d last index 2 (noop + proposal)",
			st.Term, st.LastIndex, term)
	}
}

// --- the queue hooks replication relies on ------------------------------

func TestExpireLeasesReturnsEveryLeasedJob(t *testing.T) {
	clk := newTestClock()
	q := New(WithClock(clk.Now))
	defer q.Close()
	for i := 0; i < 3; i++ {
		if _, err := q.Enqueue("a", []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := q.Enqueue("b", []byte("once"), WithMaxAttempts(1)); err != nil {
		t.Fatal(err)
	}
	var stale Lease
	for i := 0; i < 2; i++ {
		_, lease, err := q.Dequeue("a", time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		stale = lease
	}
	if _, _, err := q.Dequeue("b", time.Hour); err != nil {
		t.Fatal(err)
	}

	q.ExpireLeases()

	if st := q.Stats("a"); st.InFlight != 0 || st.Ready != 3 {
		t.Errorf("topic a after ExpireLeases: %+v, want 3 ready and none in flight", st)
	}
	if err := q.Ack(stale); !errors.Is(err, ErrUnknownLease) {
		t.Errorf("ack on an expired lease: err = %v, want ErrUnknownLease", err)
	}
	// b's only attempt is spent, so its job dead-letters, as it would on
	// an ordinary expiry.
	if st := q.Stats("b"); st.DeadLetters != 1 || st.Ready != 0 {
		t.Errorf("topic b after ExpireLeases: %+v, want its job dead-lettered", st)
	}
	if st := q.Stats(DeadLetterTopic("b")); st.Ready != 1 {
		t.Errorf("b.dlq after ExpireLeases: %+v, want 1 ready", st)
	}
}

func TestPendingReadsWithoutObserving(t *testing.T) {
	clk := newTestClock()
	q := New(WithClock(clk.Now), WithPassiveReads())
	defer q.Close()
	now := clk.Now()

	if q.Pending("never-used", now) {
		t.Error("unknown topic reported pending work")
	}
	if _, err := q.Enqueue("t", []byte("later"), WithDelay(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if q.Pending("t", now) {
		t.Error("a delayed job counted as pending before its delay passed")
	}
	if !q.Pending("t", now.Add(time.Minute)) {
		t.Error("a delayed job did not count as pending once its delay passed")
	}

	clk.Advance(time.Minute)
	if _, _, err := q.Dequeue("t", 10*time.Second); err != nil {
		t.Fatal(err)
	}
	now = clk.Now()
	if q.Pending("t", now.Add(9*time.Second)) {
		t.Error("a live lease counted as pending work")
	}
	if !q.Pending("t", now.Add(10*time.Second)) {
		t.Error("an expired lease did not count as pending work")
	}
	// Asking must not have expired it: the queue's own clock has not moved.
	if st := q.Stats("t"); st.InFlight != 1 {
		t.Errorf("Pending changed state: %+v", st)
	}
}

// With passive reads, Stats and Peek must not move an expired job back to
// ready. Dequeue still does.
func TestPassiveReadsDoNotObserveExpiry(t *testing.T) {
	clk := newTestClock()
	q := New(WithClock(clk.Now), WithPassiveReads())
	defer q.Close()
	if _, err := q.Enqueue("t", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := q.Dequeue("t", time.Second); err != nil {
		t.Fatal(err)
	}
	clk.Advance(time.Minute)

	if st := q.Stats("t"); st.InFlight != 1 || st.Ready != 0 {
		t.Errorf("Stats observed the expiry: %+v", st)
	}
	ready, _, err := q.Peek("t", 10, false)
	if err != nil || len(ready) != 0 {
		t.Errorf("Peek observed the expiry: %d ready, err %v", len(ready), err)
	}
	if _, _, err := q.Dequeue("t", time.Second); err != nil {
		t.Errorf("Dequeue did not redeliver the expired job: %v", err)
	}
}

func TestIDSourceIsUsedForJobsAndLeases(t *testing.T) {
	n := 0
	next := func() string {
		n++
		return fmt.Sprintf("%032x", n)
	}
	q := New(WithIDSource(next))
	defer q.Close()
	id, err := q.Enqueue("t", []byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	_, lease, err := q.Dequeue("t", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if id != fmt.Sprintf("%032x", 1) || lease.ID != fmt.Sprintf("%032x", 2) {
		t.Errorf("job id %s, lease id %s: not from the supplied source", id, lease.ID)
	}
}

// Leases that expire at the same instant must come back in an order that
// depends only on their IDs, never on how the heap happened to be built.
func TestSimultaneousExpiryOrderIsByLeaseID(t *testing.T) {
	for _, order := range [][]int{{1, 2, 3, 4, 5}, {5, 4, 3, 2, 1}, {3, 1, 5, 2, 4}} {
		clk := newTestClock()
		seq := 0
		ids := append([]int{101, 102, 103, 104, 105}, order...)
		next := func() string {
			id := ids[seq]
			seq++
			return fmt.Sprintf("%032x", id)
		}
		q := New(WithClock(clk.Now), WithIDSource(next))
		for i := 0; i < 5; i++ {
			if _, err := q.Enqueue("t", []byte{byte(i)}); err != nil {
				t.Fatal(err)
			}
		}
		leaseOf := map[string]int{} // job id -> lease number
		for i := 0; i < 5; i++ {
			job, _, err := q.Dequeue("t", time.Second)
			if err != nil {
				t.Fatal(err)
			}
			leaseOf[job.ID] = order[i]
		}
		clk.Advance(time.Minute)
		ready, _, err := q.Peek("t", 10, false)
		if err != nil {
			t.Fatal(err)
		}
		var got []int
		for _, j := range ready {
			got = append(got, leaseOf[j.ID])
		}
		if fmt.Sprint(got) != "[1 2 3 4 5]" {
			t.Errorf("leases issued in order %v expired in order %v, want [1 2 3 4 5]", order, got)
		}
		q.Close()
	}
}
