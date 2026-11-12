// Package raft is a from-scratch implementation of the Raft consensus
// algorithm (Ongaro and Ousterhout, "In Search of an Understandable
// Consensus Algorithm"), standard library only.
//
// A Node is a deterministic state machine with no goroutines, no timers and
// no I/O of its own. Time arrives as Tick calls, the network arrives as
// Step calls, and everything the node wants done comes back out of Ready.
// That shape is what lets the simulator in this package drive a whole
// cluster from one seeded random source and replay any failure exactly.
//
// Scope of this package today: leader election, log replication, the commit
// rule, and persistence of term, vote and log. Not here yet: snapshots and
// log compaction, membership changes, pre-vote, and linearizable reads.
package raft

import (
	"errors"
	"fmt"
	"math/rand"
	"sort"
)

// NodeID identifies a node. Zero is reserved to mean "nobody".
type NodeID uint64

// None is the absent NodeID: no vote cast, no known leader.
const None NodeID = 0

// State is a node's role in the current term.
type State uint8

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	}
	return "unknown"
}

// EntryKind separates entries a client proposed from entries the algorithm
// wrote for itself.
type EntryKind uint8

const (
	// EntryNormal carries data from Propose.
	EntryNormal EntryKind = iota
	// EntryNoop is appended by every new leader. A leader may only count
	// replicas for entries of its own term, so without one a leader with
	// no fresh proposals could never commit what it inherited. State
	// machines skip these.
	EntryNoop
)

// Entry is one slot in the replicated log. Data is shared, not copied, on
// its way through messages and Ready: treat it as read-only.
type Entry struct {
	Index uint64
	Term  uint64
	Kind  EntryKind
	Data  []byte
}

// MessageType names the four messages of the algorithm.
type MessageType uint8

const (
	MsgVote MessageType = iota + 1
	MsgVoteResp
	MsgAppend
	MsgAppendResp
)

// Message is everything nodes say to each other. Which fields carry meaning
// depends on Type:
//
//	MsgVote        LogIndex/LogTerm: the candidate's last entry.
//	MsgVoteResp    Reject: the vote was refused.
//	MsgAppend      LogIndex/LogTerm: the entry just before Entries.
//	               Commit: the leader's commit index.
//	MsgAppendResp  Accepted: LogIndex is the highest index known to match.
//	               Rejected: LogIndex echoes the refused MsgAppend, and
//	               Hint is the highest index worth retrying from.
type Message struct {
	Type     MessageType
	From, To NodeID
	Term     uint64
	LogIndex uint64
	LogTerm  uint64
	Entries  []Entry
	Commit   uint64
	Reject   bool
	Hint     uint64
}

// Ready is what a node needs its driver to do. Messages are sent only
// after Storage has made the promises in them durable, which is already
// true by the time Ready returns them.
type Ready struct {
	Messages []Message
	// Committed are newly committed entries, in log order, each handed
	// out once per process lifetime. After a restart the node hands out
	// the log again from index 1 as it relearns the commit index, so a
	// state machine either rebuilds from scratch or skips what it has
	// already applied.
	Committed []Entry
}

// Status is a snapshot of a node for observers.
type Status struct {
	ID        NodeID
	Term      uint64
	State     State
	Leader    NodeID
	Commit    uint64
	LastIndex uint64
}

var (
	// ErrNotLeader means Propose was called on a node that is not the
	// leader. Status().Leader says who the node believes is.
	ErrNotLeader = errors.New("raft: not the leader")
	// ErrBadConfig wraps every Config validation failure.
	ErrBadConfig = errors.New("raft: bad config")
)

const defaultMaxEntriesPerMsg = 64

// Config describes one node of a fixed-membership cluster.
type Config struct {
	ID NodeID
	// Peers is every node in the cluster, this one included.
	Peers []NodeID
	// ElectionTicks is the minimum number of ticks without hearing from a
	// leader before a follower stands for election. The actual timeout is
	// randomized in [ElectionTicks, 2*ElectionTicks) each time it is
	// armed, which is what breaks split votes.
	ElectionTicks int
	// HeartbeatTicks is how often a leader sends to every follower. It
	// must be smaller than ElectionTicks.
	HeartbeatTicks int
	// MaxEntriesPerMsg bounds one MsgAppend. Zero means 64.
	MaxEntriesPerMsg int
	Storage          Storage
	// Rand randomizes election timeouts. Nil seeds one from ID.
	Rand *rand.Rand
}

// faults each break one rule of the algorithm on purpose. They exist so the
// simulator's safety checks can be shown to catch real bugs rather than
// assumed to; only this package's tests can set them.
type faults struct {
	doubleVote      bool // grant a vote even after voting for someone else
	voteForStaleLog bool // skip the log up-to-date check when voting
	commitOldTerm   bool // commit an inherited entry by counting replicas
}

// Node is one Raft participant. It is not safe for concurrent use: one
// goroutine owns it and feeds it Tick, Step and Propose.
type Node struct {
	id       NodeID
	peers    []NodeID // everyone except this node
	quorum   int
	storage  Storage
	rng      *rand.Rand
	election int
	beat     int
	maxBatch int

	term     uint64
	votedFor NodeID
	log      []Entry // log[i].Index == i+1

	state   State
	leader  NodeID
	commit  uint64
	applied uint64 // highest index handed out through Ready

	elapsed int
	timeout int

	votes map[NodeID]bool   // candidate only
	next  map[NodeID]uint64 // leader only: next index to send
	match map[NodeID]uint64 // leader only: highest index known replicated

	msgs []Message
	err  error // sticky: a node that could not persist stops

	faults faults
}

// New builds a node from its storage. A node that has run before resumes
// with its term, vote and log, as a follower.
func New(cfg Config) (*Node, error) {
	if cfg.ID == None {
		return nil, fmt.Errorf("%w: ID must not be zero", ErrBadConfig)
	}
	if cfg.Storage == nil {
		return nil, fmt.Errorf("%w: Storage is required", ErrBadConfig)
	}
	if cfg.HeartbeatTicks <= 0 || cfg.ElectionTicks <= cfg.HeartbeatTicks {
		return nil, fmt.Errorf("%w: need 0 < HeartbeatTicks < ElectionTicks", ErrBadConfig)
	}
	if cfg.MaxEntriesPerMsg < 0 {
		return nil, fmt.Errorf("%w: MaxEntriesPerMsg must not be negative", ErrBadConfig)
	}
	seen := map[NodeID]bool{}
	var peers []NodeID
	for _, p := range cfg.Peers {
		if p == None || seen[p] {
			return nil, fmt.Errorf("%w: Peers has a zero or repeated ID", ErrBadConfig)
		}
		seen[p] = true
		if p != cfg.ID {
			peers = append(peers, p)
		}
	}
	if !seen[cfg.ID] {
		return nil, fmt.Errorf("%w: Peers must include ID", ErrBadConfig)
	}

	hard, log, err := cfg.Storage.Load()
	if err != nil {
		return nil, fmt.Errorf("raft: load: %w", err)
	}
	for i, e := range log {
		if e.Index != uint64(i)+1 {
			return nil, fmt.Errorf("raft: stored log is not contiguous at position %d (index %d)", i, e.Index)
		}
	}

	n := &Node{
		id:       cfg.ID,
		peers:    peers,
		quorum:   len(cfg.Peers)/2 + 1,
		storage:  cfg.Storage,
		rng:      cfg.Rand,
		election: cfg.ElectionTicks,
		beat:     cfg.HeartbeatTicks,
		maxBatch: cfg.MaxEntriesPerMsg,
		term:     hard.Term,
		votedFor: hard.VotedFor,
		log:      log,
	}
	if n.rng == nil {
		n.rng = rand.New(rand.NewSource(int64(cfg.ID)))
	}
	if n.maxBatch == 0 {
		n.maxBatch = defaultMaxEntriesPerMsg
	}
	n.resetElection()
	return n, nil
}

// Status reports where the node stands.
func (n *Node) Status() Status {
	return Status{
		ID:        n.id,
		Term:      n.term,
		State:     n.state,
		Leader:    n.leader,
		Commit:    n.commit,
		LastIndex: n.lastIndex(),
	}
}

// Ready drains the node's outbox and its newly committed entries.
func (n *Node) Ready() Ready {
	rd := Ready{Messages: n.msgs}
	n.msgs = nil
	if n.applied < n.commit {
		rd.Committed = append([]Entry(nil), n.log[n.applied:n.commit]...)
		n.applied = n.commit
	}
	return rd
}

// Tick advances the node's clock by one unit. Followers and candidates
// count toward an election; leaders count toward a heartbeat.
func (n *Node) Tick() error {
	if n.err != nil {
		return n.err
	}
	n.elapsed++
	if n.state == Leader {
		if n.elapsed >= n.beat {
			n.elapsed = 0
			n.broadcastAppend()
		}
		return nil
	}
	if n.elapsed >= n.timeout {
		return n.campaign()
	}
	return nil
}

// Propose appends data to the log if this node is the leader, and returns
// the index and term it was written at. That is not a commit: the entry is
// committed only when it comes back out of Ready, and a proposal made to a
// leader that is then deposed may never do so.
func (n *Node) Propose(data []byte) (index, term uint64, err error) {
	if n.err != nil {
		return 0, 0, n.err
	}
	if n.state != Leader {
		return 0, 0, ErrNotLeader
	}
	e := Entry{Index: n.lastIndex() + 1, Term: n.term, Data: append([]byte(nil), data...)}
	if err := n.appendEntries([]Entry{e}); err != nil {
		return 0, 0, err
	}
	n.maybeCommit()
	n.broadcastAppend()
	return e.Index, e.Term, nil
}

// Step feeds the node one message from a peer.
func (n *Node) Step(m Message) error {
	if n.err != nil {
		return n.err
	}
	if m.To != n.id || !n.isPeer(m.From) {
		return nil
	}
	if m.Term > n.term {
		// Someone is ahead of us: whatever we were, we are a follower of
		// that term now. Who leads it is learned from the first MsgAppend.
		if err := n.becomeFollower(m.Term, None); err != nil {
			return err
		}
	}
	switch m.Type {
	case MsgVote:
		return n.handleVote(m)
	case MsgVoteResp:
		return n.handleVoteResp(m)
	case MsgAppend:
		return n.handleAppend(m)
	case MsgAppendResp:
		n.handleAppendResp(m)
	}
	return nil
}

func (n *Node) isPeer(id NodeID) bool {
	for _, p := range n.peers {
		if p == id {
			return true
		}
	}
	return false
}

func (n *Node) lastIndex() uint64 { return uint64(len(n.log)) }

// termAt returns the term of the entry at index, with index 0 standing for
// the empty prefix every log shares.
func (n *Node) termAt(index uint64) uint64 {
	if index == 0 {
		return 0
	}
	return n.log[index-1].Term
}

func (n *Node) send(m Message) {
	m.From = n.id
	m.Term = n.term
	n.msgs = append(n.msgs, m)
}

func (n *Node) fail(err error) error {
	n.err = err
	return err
}

func (n *Node) resetElection() {
	n.elapsed = 0
	n.timeout = n.election + n.rng.Intn(n.election)
}

// saveHardState persists before the in-memory copy changes, so a storage
// failure never leaves the node believing a promise it did not record.
func (n *Node) saveHardState(term uint64, votedFor NodeID) error {
	if err := n.storage.SaveHardState(HardState{Term: term, VotedFor: votedFor}); err != nil {
		return n.fail(fmt.Errorf("raft: save hard state: %w", err))
	}
	n.term, n.votedFor = term, votedFor
	return nil
}

func (n *Node) appendEntries(entries []Entry) error {
	if err := n.storage.Append(entries); err != nil {
		return n.fail(fmt.Errorf("raft: append: %w", err))
	}
	n.log = append(n.log, entries...)
	return nil
}

func (n *Node) becomeFollower(term uint64, leader NodeID) error {
	if term != n.term {
		if err := n.saveHardState(term, None); err != nil {
			return err
		}
	}
	n.state = Follower
	n.leader = leader
	n.votes, n.next, n.match = nil, nil, nil
	n.resetElection()
	return nil
}

func (n *Node) campaign() error {
	if err := n.saveHardState(n.term+1, n.id); err != nil {
		return err
	}
	n.state = Candidate
	n.leader = None
	n.next, n.match = nil, nil
	n.votes = map[NodeID]bool{n.id: true}
	n.resetElection()
	if n.quorum == 1 {
		return n.becomeLeader()
	}
	for _, p := range n.peers {
		n.send(Message{Type: MsgVote, To: p, LogIndex: n.lastIndex(), LogTerm: n.termAt(n.lastIndex())})
	}
	return nil
}

func (n *Node) becomeLeader() error {
	n.state = Leader
	n.leader = n.id
	n.votes = nil
	n.elapsed = 0
	n.next = make(map[NodeID]uint64, len(n.peers))
	n.match = make(map[NodeID]uint64, len(n.peers))
	for _, p := range n.peers {
		n.next[p] = n.lastIndex() + 1
	}
	noop := Entry{Index: n.lastIndex() + 1, Term: n.term, Kind: EntryNoop}
	if err := n.appendEntries([]Entry{noop}); err != nil {
		return err
	}
	n.maybeCommit()
	n.broadcastAppend()
	return nil
}

func (n *Node) handleVote(m Message) error {
	if m.Term < n.term {
		n.send(Message{Type: MsgVoteResp, To: m.From, Reject: true})
		return nil
	}
	last := n.lastIndex()
	lastTerm := n.termAt(last)
	// The election restriction: only vote for a log at least as complete
	// as ours. A majority holds every committed entry, any winning
	// majority overlaps it, so a winner must hold them all too.
	upToDate := m.LogTerm > lastTerm || (m.LogTerm == lastTerm && m.LogIndex >= last)
	canVote := n.votedFor == None || n.votedFor == m.From

	if (canVote || n.faults.doubleVote) && (upToDate || n.faults.voteForStaleLog) {
		if n.votedFor != m.From {
			if err := n.saveHardState(n.term, m.From); err != nil {
				return err
			}
		}
		n.elapsed = 0
		n.send(Message{Type: MsgVoteResp, To: m.From})
		return nil
	}
	n.send(Message{Type: MsgVoteResp, To: m.From, Reject: true})
	return nil
}

func (n *Node) handleVoteResp(m Message) error {
	if n.state != Candidate || m.Term != n.term {
		return nil
	}
	n.votes[m.From] = !m.Reject
	granted := 0
	for _, yes := range n.votes {
		if yes {
			granted++
		}
	}
	if granted >= n.quorum {
		return n.becomeLeader()
	}
	return nil
}

func (n *Node) handleAppend(m Message) error {
	if m.Term < n.term {
		n.send(Message{Type: MsgAppendResp, To: m.From, Reject: true, LogIndex: m.LogIndex, Hint: n.lastIndex()})
		return nil
	}
	if n.state == Leader {
		return n.fail(fmt.Errorf("raft: node %d is leader of term %d and so is node %d", n.id, n.term, m.From))
	}
	// A valid MsgAppend in our term is the leader speaking. A candidate
	// that hears it has lost the election.
	n.state = Follower
	n.leader = m.From
	n.votes = nil
	n.elapsed = 0

	for i, e := range m.Entries {
		if e.Index != m.LogIndex+1+uint64(i) {
			return nil // malformed: not a contiguous run after LogIndex
		}
	}

	last := n.lastIndex()
	if m.LogIndex > last || n.termAt(m.LogIndex) != m.LogTerm {
		// Our log does not contain the entry the leader is building on.
		// Tell it the highest index that could still match.
		hint := m.LogIndex - 1
		if last < hint {
			hint = last
		}
		n.send(Message{Type: MsgAppendResp, To: m.From, Reject: true, LogIndex: m.LogIndex, Hint: hint})
		return nil
	}

	for i, e := range m.Entries {
		if e.Index <= n.lastIndex() {
			if n.termAt(e.Index) == e.Term {
				continue // already have it
			}
			// Same index, different term: our entry and everything after
			// it came from a leader that lost. Committed entries are never
			// in that position; if one is, stop rather than destroy it.
			if e.Index <= n.commit {
				return n.fail(fmt.Errorf("raft: node %d asked to overwrite committed index %d", n.id, e.Index))
			}
			if err := n.storage.TruncateFrom(e.Index); err != nil {
				return n.fail(fmt.Errorf("raft: truncate: %w", err))
			}
			n.log = n.log[:e.Index-1]
		}
		if err := n.appendEntries(append([]Entry(nil), m.Entries[i:]...)); err != nil {
			return err
		}
		break
	}

	// Only what this message verified is known to match the leader, so the
	// commit index may not run past it even if our log is longer.
	matched := m.LogIndex + uint64(len(m.Entries))
	if c := min(m.Commit, matched); c > n.commit {
		n.commit = c
	}
	n.send(Message{Type: MsgAppendResp, To: m.From, LogIndex: matched})
	return nil
}

func (n *Node) handleAppendResp(m Message) {
	if n.state != Leader || m.Term != n.term {
		return
	}
	if m.Reject {
		// Act only on a rejection of what we would send right now; an old
		// one that arrives late must not walk next backwards again.
		if m.LogIndex == 0 || m.LogIndex != n.next[m.From]-1 {
			return
		}
		next := min(m.Hint, m.LogIndex-1) + 1
		if next <= n.match[m.From] {
			next = n.match[m.From] + 1
		}
		n.next[m.From] = next
		n.sendAppend(m.From)
		return
	}
	if m.LogIndex <= n.match[m.From] || m.LogIndex > n.lastIndex() {
		return // duplicate, stale, or nonsense
	}
	n.match[m.From] = m.LogIndex
	if n.next[m.From] <= m.LogIndex {
		n.next[m.From] = m.LogIndex + 1
	}
	if n.maybeCommit() {
		n.broadcastAppend() // tell followers the new commit index now
	} else if n.next[m.From] <= n.lastIndex() {
		n.sendAppend(m.From) // still catching up: send the next batch
	}
}

// maybeCommit advances the commit index to the highest entry stored on a
// majority, and reports whether it moved.
func (n *Node) maybeCommit() bool {
	matched := make([]uint64, 0, len(n.peers)+1)
	matched = append(matched, n.lastIndex())
	for _, p := range n.peers {
		matched = append(matched, n.match[p])
	}
	sort.Slice(matched, func(i, j int) bool { return matched[i] > matched[j] })
	idx := matched[n.quorum-1]
	if idx <= n.commit {
		return false
	}
	// A leader counts replicas only for entries of its own term. An entry
	// from an earlier term can sit on a majority and still be overwritten
	// (Figure 8 of the paper); it becomes committed when an entry of the
	// current term is committed after it.
	if n.termAt(idx) != n.term && !n.faults.commitOldTerm {
		return false
	}
	n.commit = idx
	return true
}

func (n *Node) broadcastAppend() {
	for _, p := range n.peers {
		n.sendAppend(p)
	}
}

func (n *Node) sendAppend(to NodeID) {
	prev := n.next[to] - 1
	end := min(prev+uint64(n.maxBatch), n.lastIndex())
	n.send(Message{
		Type:     MsgAppend,
		To:       to,
		LogIndex: prev,
		LogTerm:  n.termAt(prev),
		// Copied: a later truncation must not rewrite a message in flight.
		Entries: append([]Entry(nil), n.log[prev:end]...),
		Commit:  n.commit,
	})
}
