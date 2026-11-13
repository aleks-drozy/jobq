package raft

import (
	"bytes"
	"errors"
	"math/rand"
	"testing"
)

const (
	testElection = 10
	testBeat     = 2
)

func newTestNode(t *testing.T, id NodeID, peers []NodeID, st Storage) *Node {
	t.Helper()
	n, err := New(Config{
		ID:             id,
		Peers:          peers,
		ElectionTicks:  testElection,
		HeartbeatTicks: testBeat,
		Storage:        st,
		Rand:           rand.New(rand.NewSource(int64(id))),
	})
	if err != nil {
		t.Fatalf("New(%d): %v", id, err)
	}
	return n
}

// testCluster is a perfect network: every message is delivered, in order,
// until nobody has anything left to say.
type testCluster struct {
	t       *testing.T
	nodes   map[NodeID]*Node
	applied map[NodeID][]Entry
	loaded  map[NodeID][]Snapshot // snapshots handed out through Ready
	down    map[NodeID]bool       // nodes that hear nothing
	lose    func(Message) bool    // messages the network drops
	sent    []Message             // every message delivered or dropped
}

func newTestCluster(t *testing.T, size int) *testCluster {
	t.Helper()
	c := &testCluster{t: t, nodes: map[NodeID]*Node{}, applied: map[NodeID][]Entry{},
		loaded: map[NodeID][]Snapshot{}, down: map[NodeID]bool{}}
	var peers []NodeID
	for i := 1; i <= size; i++ {
		peers = append(peers, NodeID(i))
	}
	for _, id := range peers {
		c.nodes[id] = newTestNode(t, id, peers, NewMemoryStorage())
	}
	return c
}

func (c *testCluster) settle() {
	c.t.Helper()
	for {
		var queue []Message
		for id, n := range c.nodes {
			rd := n.Ready()
			queue = append(queue, rd.Messages...)
			if rd.Snapshot != nil {
				c.loaded[id] = append(c.loaded[id], *rd.Snapshot)
			}
			c.applied[id] = append(c.applied[id], rd.Committed...)
		}
		if len(queue) == 0 {
			return
		}
		c.sent = append(c.sent, queue...)
		for _, m := range queue {
			if dst := c.nodes[m.To]; dst != nil && !c.down[m.To] && !c.down[m.From] && (c.lose == nil || !c.lose(m)) {
				if err := dst.Step(m); err != nil {
					c.t.Fatalf("Step: %v", err)
				}
			}
		}
	}
}

// elect ticks one node until it stands for election, then lets the
// cluster settle.
func (c *testCluster) elect(id NodeID) *Node {
	c.t.Helper()
	n := c.nodes[id]
	for i := 0; i < 2*testElection && n.state == Follower; i++ {
		if err := n.Tick(); err != nil {
			c.t.Fatalf("Tick: %v", err)
		}
	}
	c.settle()
	if n.state != Leader {
		c.t.Fatalf("node %d is %v after campaigning, want leader", id, n.state)
	}
	return n
}

func mustStep(t *testing.T, n *Node, m Message) {
	t.Helper()
	if err := n.Step(m); err != nil {
		t.Fatalf("Step(%+v): %v", m, err)
	}
}

func TestConfigValidation(t *testing.T) {
	good := Config{ID: 1, Peers: []NodeID{1, 2, 3}, ElectionTicks: 10, HeartbeatTicks: 2, Storage: NewMemoryStorage()}
	if _, err := New(good); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	cases := map[string]func(*Config){
		"zero ID":              func(c *Config) { c.ID = 0 },
		"ID not in peers":      func(c *Config) { c.Peers = []NodeID{2, 3} },
		"repeated peer":        func(c *Config) { c.Peers = []NodeID{1, 2, 2} },
		"zero peer":            func(c *Config) { c.Peers = []NodeID{1, 0} },
		"no storage":           func(c *Config) { c.Storage = nil },
		"zero heartbeat":       func(c *Config) { c.HeartbeatTicks = 0 },
		"election <= beat":     func(c *Config) { c.ElectionTicks = 2 },
		"negative batch limit": func(c *Config) { c.MaxEntriesPerMsg = -1 },
	}
	for name, mutate := range cases {
		cfg := good
		mutate(&cfg)
		if _, err := New(cfg); !errors.Is(err, ErrBadConfig) {
			t.Errorf("%s: err = %v, want ErrBadConfig", name, err)
		}
	}
}

func TestSingleNodeElectsItselfAndCommits(t *testing.T) {
	c := newTestCluster(t, 1)
	n := c.elect(1)
	idx, term, err := n.Propose([]byte("a"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	c.settle()
	if idx != 2 || term != 1 {
		t.Fatalf("Propose = (%d, %d), want (2, 1): the no-op takes index 1", idx, term)
	}
	got := c.applied[1]
	if len(got) != 2 || got[0].Kind != EntryNoop || !bytes.Equal(got[1].Data, []byte("a")) {
		t.Fatalf("applied = %+v, want [noop, a]", got)
	}
}

func TestThreeNodesElectOneLeaderAndReplicate(t *testing.T) {
	c := newTestCluster(t, 3)
	leader := c.elect(1)
	for _, id := range []NodeID{2, 3} {
		if st := c.nodes[id].Status(); st.State != Follower || st.Leader != 1 || st.Term != 1 {
			t.Fatalf("node %d status = %+v, want follower of 1 in term 1", id, st)
		}
	}
	for _, p := range []string{"a", "b", "c"} {
		if _, _, err := leader.Propose([]byte(p)); err != nil {
			t.Fatalf("Propose: %v", err)
		}
	}
	c.settle()
	// Followers learn the final commit index from the next heartbeat.
	for i := 0; i < testBeat; i++ {
		_ = leader.Tick()
	}
	c.settle()
	for id := range c.nodes {
		got := c.applied[id]
		if len(got) != 4 {
			t.Fatalf("node %d applied %d entries, want 4 (noop + 3)", id, len(got))
		}
		for i, want := range []string{"a", "b", "c"} {
			if string(got[i+1].Data) != want {
				t.Fatalf("node %d applied[%d] = %q, want %q", id, i+1, got[i+1].Data, want)
			}
		}
	}
}

func TestProposeOnFollowerFails(t *testing.T) {
	c := newTestCluster(t, 3)
	c.elect(1)
	if _, _, err := c.nodes[2].Propose([]byte("x")); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("Propose on follower = %v, want ErrNotLeader", err)
	}
}

func TestProposeCopiesData(t *testing.T) {
	c := newTestCluster(t, 1)
	n := c.elect(1)
	buf := []byte("original")
	if _, _, err := n.Propose(buf); err != nil {
		t.Fatalf("Propose: %v", err)
	}
	copy(buf, "mutated!")
	c.settle()
	if got := c.applied[1][1].Data; string(got) != "original" {
		t.Fatalf("committed data = %q: the caller's buffer leaked into the log", got)
	}
}

func TestVoteDeniedToCandidateWithStaleLog(t *testing.T) {
	st := NewMemoryStorage()
	_ = st.SaveHardState(HardState{Term: 2})
	_ = st.Append([]Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}})
	n := newTestNode(t, 1, []NodeID{1, 2, 3}, st)

	cases := []struct {
		name               string
		logIndex, logTerm  uint64
		wantReject         bool
		candidate, forTerm uint64
	}{
		{"older last term", 5, 1, true, 2, 3},
		{"same term, shorter", 1, 2, true, 2, 4},
		{"same term, same length", 2, 2, false, 2, 5},
		{"newer last term, shorter", 1, 3, false, 3, 6},
	}
	for _, tc := range cases {
		mustStep(t, n, Message{Type: MsgVote, From: NodeID(tc.candidate), To: 1, Term: tc.forTerm, LogIndex: tc.logIndex, LogTerm: tc.logTerm})
		msgs := n.Ready().Messages
		if len(msgs) != 1 || msgs[0].Type != MsgVoteResp || msgs[0].Reject != tc.wantReject {
			t.Errorf("%s: response = %+v, want reject=%v", tc.name, msgs, tc.wantReject)
		}
	}
}

func TestOneVotePerTermSurvivesRestart(t *testing.T) {
	st := NewMemoryStorage()
	peers := []NodeID{1, 2, 3}
	n := newTestNode(t, 1, peers, st)
	mustStep(t, n, Message{Type: MsgVote, From: 2, To: 1, Term: 1})
	if m := n.Ready().Messages; len(m) != 1 || m[0].Reject {
		t.Fatalf("first vote = %+v, want granted", m)
	}

	// Crash and come back: the vote must still be on record.
	n = newTestNode(t, 1, peers, st)
	mustStep(t, n, Message{Type: MsgVote, From: 3, To: 1, Term: 1})
	if m := n.Ready().Messages; len(m) != 1 || !m[0].Reject {
		t.Fatalf("second candidate in the same term got %+v, want rejected", m)
	}
	// Asking again as the original candidate is still a yes.
	mustStep(t, n, Message{Type: MsgVote, From: 2, To: 1, Term: 1})
	if m := n.Ready().Messages; len(m) != 1 || m[0].Reject {
		t.Fatalf("repeat request from the voted-for candidate = %+v, want granted", m)
	}
}

func TestFollowerTruncatesConflictingSuffix(t *testing.T) {
	st := NewMemoryStorage()
	_ = st.SaveHardState(HardState{Term: 2})
	_ = st.Append([]Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}, {Index: 3, Term: 2}})
	n := newTestNode(t, 1, []NodeID{1, 2, 3}, st)

	// The term-3 leader has a different entry at index 2.
	mustStep(t, n, Message{
		Type: MsgAppend, From: 2, To: 1, Term: 3,
		LogIndex: 1, LogTerm: 1,
		Entries: []Entry{{Index: 2, Term: 3, Data: []byte("new")}},
		Commit:  2,
	})
	msgs := n.Ready().Messages
	if len(msgs) != 1 || msgs[0].Reject || msgs[0].LogIndex != 2 {
		t.Fatalf("response = %+v, want accept with match index 2", msgs)
	}
	_, _, log, _ := st.Load()
	if len(log) != 2 || log[1].Term != 3 || string(log[1].Data) != "new" {
		t.Fatalf("stored log = %+v, want index 2 replaced and index 3 gone", log)
	}
	if n.commit != 2 {
		t.Fatalf("commit = %d, want 2", n.commit)
	}
}

func TestFollowerRejectsAppendWithMissingPrefix(t *testing.T) {
	st := NewMemoryStorage()
	_ = st.Append([]Entry{{Index: 1, Term: 1}})
	n := newTestNode(t, 1, []NodeID{1, 2, 3}, st)
	mustStep(t, n, Message{Type: MsgAppend, From: 2, To: 1, Term: 1, LogIndex: 5, LogTerm: 1,
		Entries: []Entry{{Index: 6, Term: 1}}})
	msgs := n.Ready().Messages
	if len(msgs) != 1 || !msgs[0].Reject || msgs[0].LogIndex != 5 || msgs[0].Hint != 1 {
		t.Fatalf("response = %+v, want reject echoing 5 with hint 1", msgs)
	}
}

// A follower whose log runs past what the leader just verified must not
// commit the unverified part, even if the leader's commit index covers it.
func TestCommitNeverRunsPastVerifiedPrefix(t *testing.T) {
	st := NewMemoryStorage()
	_ = st.SaveHardState(HardState{Term: 2})
	_ = st.Append([]Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1}, {Index: 3, Term: 1}})
	n := newTestNode(t, 1, []NodeID{1, 2, 3}, st)
	mustStep(t, n, Message{Type: MsgAppend, From: 2, To: 1, Term: 2, LogIndex: 1, LogTerm: 1, Commit: 3})
	if n.commit != 1 {
		t.Fatalf("commit = %d, want 1: indexes 2 and 3 were never checked against the leader", n.commit)
	}
}

// Figure 8 of the Raft paper: an entry from an earlier term stored on a
// majority is still not committed until an entry of the leader's own term
// is.
func TestLeaderDoesNotCommitOldTermEntryByCounting(t *testing.T) {
	st := NewMemoryStorage()
	_ = st.SaveHardState(HardState{Term: 2})
	_ = st.Append([]Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}})
	n := newTestNode(t, 1, []NodeID{1, 2, 3}, st)
	for n.state == Follower {
		_ = n.Tick()
	}
	mustStep(t, n, Message{Type: MsgVoteResp, From: 2, To: 1, Term: 3})
	if n.state != Leader || n.lastIndex() != 3 {
		t.Fatalf("state = %v, last = %d; want leader with its no-op at index 3", n.state, n.lastIndex())
	}
	n.Ready()

	// Node 2 confirms it has index 2 (term 2). That is a majority.
	mustStep(t, n, Message{Type: MsgAppendResp, From: 2, To: 1, Term: 3, LogIndex: 2})
	if n.commit != 0 {
		t.Fatalf("commit = %d after a majority holds the term-2 entry, want 0", n.commit)
	}
	// Once the term-3 no-op is on a majority, everything before it commits.
	mustStep(t, n, Message{Type: MsgAppendResp, From: 2, To: 1, Term: 3, LogIndex: 3})
	if n.commit != 3 {
		t.Fatalf("commit = %d, want 3", n.commit)
	}
}

func TestLeaderStepsDownOnHigherTerm(t *testing.T) {
	c := newTestCluster(t, 3)
	n := c.elect(1)
	mustStep(t, n, Message{Type: MsgAppendResp, From: 2, To: 1, Term: 9, Reject: true})
	if st := n.Status(); st.State != Follower || st.Term != 9 || st.Leader != None {
		t.Fatalf("status = %+v, want leaderless follower in term 9", st)
	}
	if hard, _, _, _ := n.storage.Load(); hard.Term != 9 || hard.VotedFor != None {
		t.Fatalf("stored hard state = %+v, want term 9 with no vote", hard)
	}
}

func TestStaleAndDuplicateResponsesAreIgnored(t *testing.T) {
	c := newTestCluster(t, 3)
	n := c.elect(1)
	if _, _, err := n.Propose([]byte("a")); err != nil {
		t.Fatalf("Propose: %v", err)
	}
	c.settle()
	next, match, commit := n.next[2], n.match[2], n.commit

	msgs := []Message{
		{Type: MsgAppendResp, From: 2, To: 1, Term: 1, LogIndex: 1},               // old ack
		{Type: MsgAppendResp, From: 2, To: 1, Term: 1, LogIndex: 1, Reject: true}, // old rejection
		{Type: MsgAppendResp, From: 2, To: 1, Term: 1, LogIndex: 99},              // beyond our log
		{Type: MsgVoteResp, From: 2, To: 1, Term: 1},                              // vote after winning
		{Type: MsgAppendResp, From: 7, To: 1, Term: 1, LogIndex: 2},               // not a member
		{Type: MsgAppendResp, From: 2, To: 3, Term: 1, LogIndex: 2},               // not for us
	}
	for _, m := range msgs {
		mustStep(t, n, m)
	}
	if n.next[2] != next || n.match[2] != match || n.commit != commit || n.state != Leader {
		t.Fatalf("leader state moved: next %d->%d match %d->%d commit %d->%d state %v",
			next, n.next[2], match, n.match[2], commit, n.commit, n.state)
	}
}

func TestLeaderBacksUpToRepairDivergedFollower(t *testing.T) {
	c := newTestCluster(t, 3)
	// Node 3 carries entries from a term-1 leader nobody else heard from.
	st := NewMemoryStorage()
	_ = st.SaveHardState(HardState{Term: 1})
	_ = st.Append([]Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1}, {Index: 3, Term: 1}})
	c.nodes[3] = newTestNode(t, 3, []NodeID{1, 2, 3}, st)
	// Nodes 1 and 2 saw term 1 come and go without receiving anything.
	for _, id := range []NodeID{1, 2} {
		seen := NewMemoryStorage()
		_ = seen.SaveHardState(HardState{Term: 1})
		c.nodes[id] = newTestNode(t, id, []NodeID{1, 2, 3}, seen)
	}

	// Node 1 wins term 2 with node 2's vote; node 3's log is newer and it
	// says no, which is the election restriction doing its job.
	leader := c.nodes[1]
	for leader.state == Follower {
		_ = leader.Tick()
	}
	mustStep(t, leader, Message{Type: MsgVoteResp, From: 2, To: 1, Term: 2})
	if leader.state != Leader {
		t.Fatalf("node 1 is %v, want leader", leader.state)
	}
	c.settle()
	for i := 0; i < testBeat; i++ {
		_ = leader.Tick()
	}
	c.settle()

	_, _, got, _ := st.Load()
	if len(got) != len(leader.log) {
		t.Fatalf("node 3 log has %d entries, leader has %d", len(got), len(leader.log))
	}
	for i := range got {
		if got[i].Term != leader.log[i].Term {
			t.Fatalf("node 3 index %d has term %d, leader has %d", i+1, got[i].Term, leader.log[i].Term)
		}
	}
}

func TestLargeLogIsSentInBoundedBatches(t *testing.T) {
	st := NewMemoryStorage()
	n, err := New(Config{ID: 1, Peers: []NodeID{1, 2}, ElectionTicks: 10, HeartbeatTicks: 2,
		MaxEntriesPerMsg: 4, Storage: st})
	if err != nil {
		t.Fatal(err)
	}
	for n.state == Follower {
		_ = n.Tick()
	}
	mustStep(t, n, Message{Type: MsgVoteResp, From: 2, To: 1, Term: 1})
	for i := 0; i < 10; i++ {
		if _, _, err := n.Propose([]byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	for _, m := range n.Ready().Messages {
		if m.Type == MsgAppend && len(m.Entries) > 4 {
			t.Fatalf("MsgAppend carries %d entries, limit is 4", len(m.Entries))
		}
	}
}

type failingStorage struct {
	*MemoryStorage
	fail error
}

func (s *failingStorage) SaveHardState(h HardState) error {
	if s.fail != nil {
		return s.fail
	}
	return s.MemoryStorage.SaveHardState(h)
}

func TestStorageFailureFailsClosed(t *testing.T) {
	boom := errors.New("disk on fire")
	st := &failingStorage{MemoryStorage: NewMemoryStorage()}
	n := newTestNode(t, 1, []NodeID{1, 2, 3}, st)

	st.fail = boom
	err := n.Step(Message{Type: MsgVote, From: 2, To: 1, Term: 1})
	if !errors.Is(err, boom) {
		t.Fatalf("Step = %v, want the storage error", err)
	}
	if msgs := n.Ready().Messages; len(msgs) != 0 {
		t.Fatalf("node sent %+v after failing to persist its vote", msgs)
	}
	// The disk recovering does not un-fail the node: it stays stopped.
	st.fail = nil
	if err := n.Tick(); !errors.Is(err, boom) {
		t.Fatalf("Tick after failure = %v, want the original error", err)
	}
	if _, _, err := n.Propose(nil); !errors.Is(err, boom) {
		t.Fatalf("Propose after failure = %v, want the original error", err)
	}
}

// proposeN proposes n entries on the leader and lets the cluster settle.
func (c *testCluster) proposeN(leader *Node, n int) {
	c.t.Helper()
	for i := 0; i < n; i++ {
		if _, _, err := leader.Propose([]byte{byte(i)}); err != nil {
			c.t.Fatalf("Propose: %v", err)
		}
	}
	c.settle()
}

func (c *testCluster) beat(leader *Node) {
	c.t.Helper()
	for i := 0; i < testBeat; i++ {
		if err := leader.Tick(); err != nil {
			c.t.Fatalf("Tick: %v", err)
		}
	}
	c.settle()
}

func TestCompactDropsTheLogAndSurvivesRestart(t *testing.T) {
	st := NewMemoryStorage()
	n := newTestNode(t, 1, []NodeID{1}, st)
	for n.state != Leader {
		_ = n.Tick()
	}
	for i := 0; i < 9; i++ {
		if _, _, err := n.Propose([]byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	if got := len(n.Ready().Committed); got != 10 { // the no-op and nine proposals
		t.Fatalf("applied %d entries, want 10", got)
	}

	if err := n.Compact(8, []byte("state@8"), 2); err != nil {
		t.Fatal(err)
	}
	if st := n.Status(); st.SnapshotIndex != 8 || st.FirstIndex != 7 || st.LastIndex != 10 {
		t.Fatalf("after compacting at 8 keeping 2: %+v", st)
	}
	_, snap, log, _ := st.Load()
	if snap.Index != 8 || snap.Term != 1 || string(snap.Data) != "state@8" || len(log) != 4 || log[0].Index != 7 {
		t.Fatalf("stored snapshot %+v with %d entries from %d", snap, len(log), log[0].Index)
	}

	// Compacting backwards, or past what was applied, is a caller's bug.
	if err := n.Compact(8, nil, 0); err == nil {
		t.Fatal("compacting at the snapshot's own index succeeded")
	}
	if err := n.Compact(11, nil, 0); err == nil {
		t.Fatal("compacting past the applied index succeeded")
	}

	// A restart hands the snapshot back first, then only what follows it.
	n2 := newTestNode(t, 1, []NodeID{1}, st)
	rd := n2.Ready()
	if rd.Snapshot == nil || rd.Snapshot.Index != 8 || string(rd.Snapshot.Data) != "state@8" {
		t.Fatalf("restart handed out snapshot %+v", rd.Snapshot)
	}
	if len(rd.Committed) != 0 {
		t.Fatalf("restart handed out %d entries before hearing from anyone", len(rd.Committed))
	}
	for n2.state != Leader {
		_ = n2.Tick()
	}
	rd = n2.Ready()
	if rd.Snapshot != nil {
		t.Fatal("the snapshot was handed out twice")
	}
	if len(rd.Committed) != 3 || rd.Committed[0].Index != 9 || rd.Committed[2].Index != 11 {
		t.Fatalf("after restart applied %+v, want indexes 9, 10 and the new leader's no-op at 11", rd.Committed)
	}
}

func TestFollowerBehindCompactionIsSentTheSnapshot(t *testing.T) {
	c := newTestCluster(t, 3)
	leader := c.elect(1)
	c.down[3] = true
	c.proposeN(leader, 10) // indexes 2..11, committed by nodes 1 and 2

	if err := leader.Compact(11, []byte("state@11"), 0); err != nil {
		t.Fatal(err)
	}
	c.down[3] = false
	c.proposeN(leader, 2) // indexes 12, 13
	c.beat(leader)
	c.beat(leader)

	if got := c.loaded[3]; len(got) != 1 || got[0].Index != 11 || string(got[0].Data) != "state@11" {
		t.Fatalf("node 3 loaded snapshots %+v, want the one at 11", got)
	}
	// It applied the one entry it saw before it went quiet, nothing else
	// the snapshot covers, and everything after.
	var got []uint64
	for _, e := range c.applied[3] {
		got = append(got, e.Index)
	}
	if len(got) != 3 || got[0] != 1 || got[1] != 12 || got[2] != 13 {
		t.Fatalf("node 3 applied indexes %v, want [1 12 13]", got)
	}
	n3 := c.nodes[3]
	if n3.commit != 13 || n3.lastIndex() != 13 || n3.snap.Index != 11 || len(n3.log) != 2 {
		t.Fatalf("node 3: commit %d last %d snapshot %d log %d", n3.commit, n3.lastIndex(), n3.snap.Index, len(n3.log))
	}
	if leader.match[3] != 13 {
		t.Fatalf("leader thinks node 3 holds up to %d, want 13", leader.match[3])
	}
}

// Entries kept behind a snapshot are for exactly this: a follower only a
// little behind is caught up from the log.
func TestFollowerWithinKeptEntriesIsNotSentTheSnapshot(t *testing.T) {
	c := newTestCluster(t, 3)
	leader := c.elect(1)
	c.proposeN(leader, 5) // 2..6 everywhere
	c.down[3] = true
	c.proposeN(leader, 3) // 7..9, node 3 misses them
	if err := leader.Compact(9, []byte("state@9"), 4); err != nil {
		t.Fatal(err)
	}
	c.down[3] = false
	c.sent = nil
	c.beat(leader)
	c.beat(leader)

	for _, m := range c.sent {
		if m.Type == MsgSnapshot {
			t.Fatal("leader sent a snapshot to a follower it could catch up from the log")
		}
	}
	if len(c.loaded[3]) != 0 || c.nodes[3].lastIndex() != 9 || c.nodes[3].commit != 9 {
		t.Fatalf("node 3: loaded %d snapshots, last %d, commit %d", len(c.loaded[3]), c.nodes[3].lastIndex(), c.nodes[3].commit)
	}
}

func TestSnapshotOlderThanCommitIsNotInstalled(t *testing.T) {
	st := NewMemoryStorage()
	n := newTestNode(t, 2, []NodeID{1, 2, 3}, st)
	var entries []Entry
	for i := uint64(1); i <= 6; i++ {
		entries = append(entries, Entry{Index: i, Term: 1, Data: []byte{byte(i)}})
	}
	mustStep(t, n, Message{Type: MsgAppend, From: 1, To: 2, Term: 1, Entries: entries, Commit: 6})
	if got := len(n.Ready().Committed); got != 6 {
		t.Fatalf("applied %d, want 6", got)
	}

	mustStep(t, n, Message{Type: MsgSnapshot, From: 1, To: 2, Term: 1, Snapshot: &Snapshot{Index: 4, Term: 1, Data: []byte("old")}})
	rd := n.Ready()
	if rd.Snapshot != nil {
		t.Fatal("a snapshot behind the commit index was handed to the state machine")
	}
	if len(rd.Messages) != 1 || rd.Messages[0].Reject || rd.Messages[0].LogIndex != 6 {
		t.Fatalf("response = %+v, want an acknowledgement at the commit index", rd.Messages)
	}
	if n.lastIndex() != 6 || n.commit != 6 || n.snap.Index != 0 {
		t.Fatalf("node changed: last %d commit %d snapshot %d", n.lastIndex(), n.commit, n.snap.Index)
	}
}

// A follower whose log already runs through the snapshot's last entry
// keeps its log, entries after the snapshot included.
func TestSnapshotThatMatchesTheLogOnlyAdvancesCommit(t *testing.T) {
	n := newTestNode(t, 2, []NodeID{1, 2, 3}, NewMemoryStorage())
	var entries []Entry
	for i := uint64(1); i <= 6; i++ {
		entries = append(entries, Entry{Index: i, Term: 1})
	}
	mustStep(t, n, Message{Type: MsgAppend, From: 1, To: 2, Term: 1, Entries: entries, Commit: 2})
	n.Ready()

	mustStep(t, n, Message{Type: MsgSnapshot, From: 1, To: 2, Term: 1, Snapshot: &Snapshot{Index: 4, Term: 1}})
	rd := n.Ready()
	if rd.Snapshot != nil {
		t.Fatal("the state machine was reset although the log already held the snapshot's entries")
	}
	if len(rd.Committed) != 2 || rd.Committed[0].Index != 3 || rd.Committed[1].Index != 4 {
		t.Fatalf("applied %+v, want indexes 3 and 4", rd.Committed)
	}
	if n.lastIndex() != 6 {
		t.Fatalf("log now ends at %d, want 6: entries after the snapshot were dropped", n.lastIndex())
	}
}

// A follower that disagrees with the snapshot at its last index holds
// entries from a leader that lost. All of them go.
func TestSnapshotReplacesAConflictingLog(t *testing.T) {
	st := NewMemoryStorage()
	n := newTestNode(t, 2, []NodeID{1, 2, 3}, st)
	mustStep(t, n, Message{Type: MsgAppend, From: 1, To: 2, Term: 1, Entries: []Entry{
		{Index: 1, Term: 1}, {Index: 2, Term: 1}, {Index: 3, Term: 1}, {Index: 4, Term: 1},
	}, Commit: 1})
	n.Ready()

	mustStep(t, n, Message{Type: MsgSnapshot, From: 3, To: 2, Term: 2, Snapshot: &Snapshot{Index: 3, Term: 2, Data: []byte("s")}})
	rd := n.Ready()
	if rd.Snapshot == nil || rd.Snapshot.Index != 3 {
		t.Fatalf("snapshot handed out: %+v", rd.Snapshot)
	}
	if n.lastIndex() != 3 || len(n.log) != 0 || n.commit != 3 {
		t.Fatalf("after install: last %d, %d log entries, commit %d", n.lastIndex(), len(n.log), n.commit)
	}
	_, snap, log, _ := st.Load()
	if snap.Index != 3 || len(log) != 0 {
		t.Fatalf("stored snapshot at %d with %d entries", snap.Index, len(log))
	}
	// The next append builds on the snapshot's last entry.
	mustStep(t, n, Message{Type: MsgAppend, From: 3, To: 2, Term: 2, LogIndex: 3, LogTerm: 2,
		Entries: []Entry{{Index: 4, Term: 2}}, Commit: 4})
	if rd := n.Ready(); len(rd.Committed) != 1 || rd.Committed[0].Index != 4 || rd.Committed[0].Term != 2 {
		t.Fatalf("applied %+v after the snapshot, want index 4 of term 2", rd.Committed)
	}
}

// An append that starts inside what the snapshot covers is accepted from
// the snapshot's edge on, not refused.
func TestAppendOverlappingTheSnapshotIsAccepted(t *testing.T) {
	n := newTestNode(t, 2, []NodeID{1, 2, 3}, NewMemoryStorage())
	mustStep(t, n, Message{Type: MsgSnapshot, From: 1, To: 2, Term: 1, Snapshot: &Snapshot{Index: 5, Term: 1}})
	n.Ready()

	mustStep(t, n, Message{Type: MsgAppend, From: 1, To: 2, Term: 1, LogIndex: 3, LogTerm: 1, Entries: []Entry{
		{Index: 4, Term: 1}, {Index: 5, Term: 1}, {Index: 6, Term: 1}, {Index: 7, Term: 1},
	}, Commit: 7})
	rd := n.Ready()
	if len(rd.Messages) != 1 || rd.Messages[0].Reject || rd.Messages[0].LogIndex != 7 {
		t.Fatalf("response = %+v, want accept with match index 7", rd.Messages)
	}
	if len(rd.Committed) != 2 || rd.Committed[0].Index != 6 {
		t.Fatalf("applied %+v, want indexes 6 and 7", rd.Committed)
	}

	// One that lies wholly inside the snapshot is acknowledged at its edge.
	mustStep(t, n, Message{Type: MsgAppend, From: 1, To: 2, Term: 1, LogIndex: 1, LogTerm: 1, Entries: []Entry{
		{Index: 2, Term: 1}, {Index: 3, Term: 1},
	}})
	if msgs := n.Ready().Messages; len(msgs) != 1 || msgs[0].Reject || msgs[0].LogIndex != 5 {
		t.Fatalf("response = %+v, want accept with match index 5", msgs)
	}
}

// A snapshot can take much longer to arrive than a heartbeat. The leader
// must not send it again on every heartbeat in between.
func TestSnapshotIsNotResentWhileOneIsInFlight(t *testing.T) {
	c := newTestCluster(t, 3)
	leader := c.elect(1)
	c.down[3] = true
	c.proposeN(leader, 6)
	if err := leader.Compact(7, []byte("s"), 0); err != nil {
		t.Fatal(err)
	}

	// Node 3 is back and refuses every append, since it lacks the entry
	// they build on, but the snapshot itself keeps getting lost.
	c.down[3] = false
	c.lose = func(m Message) bool { return m.Type == MsgSnapshot }
	c.sent = nil
	for i := 0; i < testElection-1; i++ {
		_ = leader.Tick()
		c.settle()
	}
	count := 0
	for _, m := range c.sent {
		if m.Type == MsgSnapshot && m.To == 3 {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("leader sent %d snapshots in less than one election timeout, want 1", count)
	}
	// And it does try again once the pause is over.
	c.lose = nil
	for i := 0; i < 2*testElection; i++ {
		_ = leader.Tick()
		c.settle()
	}
	if len(c.loaded[3]) != 1 || c.nodes[3].commit != leader.commit {
		t.Fatalf("node 3 never caught up: loaded %d snapshots, commit %d, leader commit %d",
			len(c.loaded[3]), c.nodes[3].commit, leader.commit)
	}
}

// A leader that can reach nobody cannot commit anything, and the rest of
// the cluster has probably elected someone else. It must stop saying it
// leads, or everything that asks it "who leads?" is told the wrong thing
// for as long as the partition lasts.
func TestLeaderCutOffFromAMajorityStepsDown(t *testing.T) {
	c := newTestCluster(t, 3)
	leader := c.elect(1)
	term := leader.term

	// With one follower still answering it has a majority and stays.
	c.down[3] = true
	for i := 0; i < 3*testElection; i++ {
		_ = leader.Tick()
		c.settle()
	}
	if leader.state != Leader {
		t.Fatalf("leader stepped down while a majority still answered it (now %v)", leader.state)
	}

	// With neither, it gives up within two election timeouts.
	c.down[2] = true
	for i := 0; i < 2*testElection && leader.state == Leader; i++ {
		_ = leader.Tick()
		c.settle()
	}
	if leader.state != Follower {
		t.Fatalf("leader is still %v after two election timeouts without a majority", leader.state)
	}
	st := leader.Status()
	if st.Leader != None || st.Term != term {
		t.Fatalf("after stepping down: leader %d, term %d; want no known leader and term %d unchanged", st.Leader, st.Term, term)
	}
	if _, _, err := leader.Propose([]byte("x")); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("Propose after stepping down: %v, want ErrNotLeader", err)
	}
}

// A cluster of one is its own majority.
func TestSingleNodeLeaderNeverStepsDown(t *testing.T) {
	n := newTestNode(t, 1, []NodeID{1}, NewMemoryStorage())
	for i := 0; i < 10*testElection; i++ {
		if err := n.Tick(); err != nil {
			t.Fatal(err)
		}
	}
	if n.state != Leader || n.term != 1 {
		t.Fatalf("single node is %v in term %d, want leader in term 1", n.state, n.term)
	}
}

// Installing a snapshot discards the log, so one the state machine could
// not load must be turned away before anything is written.
func TestSnapshotThatFailsItsCheckIsNotInstalled(t *testing.T) {
	st := NewMemoryStorage()
	n, err := New(Config{ID: 2, Peers: []NodeID{1, 2, 3}, ElectionTicks: testElection, HeartbeatTicks: testBeat,
		Storage: st, CheckSnapshot: func(s Snapshot) error {
			if string(s.Data) != "good" {
				return errors.New("unreadable")
			}
			return nil
		}})
	if err != nil {
		t.Fatal(err)
	}
	mustStep(t, n, Message{Type: MsgAppend, From: 1, To: 2, Term: 1, Entries: []Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1}}, Commit: 1})
	n.Ready()

	mustStep(t, n, Message{Type: MsgSnapshot, From: 1, To: 2, Term: 1, Snapshot: &Snapshot{Index: 9, Term: 1, Data: []byte("garbage")}})
	rd := n.Ready()
	if rd.Snapshot != nil {
		t.Fatal("a snapshot that failed its check was handed to the state machine")
	}
	if len(rd.Messages) != 0 {
		t.Fatalf("a refused snapshot was answered: %+v", rd.Messages)
	}
	if n.lastIndex() != 2 || n.commit != 1 || n.snap.Index != 0 {
		t.Fatalf("node changed: last %d commit %d snapshot %d", n.lastIndex(), n.commit, n.snap.Index)
	}
	if _, snap, log, _ := st.Load(); snap.Index != 0 || len(log) != 2 {
		t.Fatalf("storage changed: snapshot at %d, %d entries", snap.Index, len(log))
	}

	// A good one from the same leader still goes in.
	mustStep(t, n, Message{Type: MsgSnapshot, From: 1, To: 2, Term: 1, Snapshot: &Snapshot{Index: 9, Term: 1, Data: []byte("good")}})
	if rd := n.Ready(); rd.Snapshot == nil || rd.Snapshot.Index != 9 {
		t.Fatalf("a snapshot that passes its check was not installed: %+v", rd.Snapshot)
	}
}
