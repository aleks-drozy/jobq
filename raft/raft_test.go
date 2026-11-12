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
}

func newTestCluster(t *testing.T, size int) *testCluster {
	t.Helper()
	c := &testCluster{t: t, nodes: map[NodeID]*Node{}, applied: map[NodeID][]Entry{}}
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
			c.applied[id] = append(c.applied[id], rd.Committed...)
		}
		if len(queue) == 0 {
			return
		}
		for _, m := range queue {
			if dst := c.nodes[m.To]; dst != nil {
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
	_, log, _ := st.Load()
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
	if hard, _, _ := n.storage.Load(); hard.Term != 9 || hard.VotedFor != None {
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

	_, got, _ := st.Load()
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
