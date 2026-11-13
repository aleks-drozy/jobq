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
// rule, persistence of term, vote and log, snapshots with log compaction,
// and a leader that steps down when it loses touch with a majority. Not
// here yet: membership changes, pre-vote, and linearizable reads.
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

// MessageType names the messages of the algorithm.
type MessageType uint8

const (
	MsgVote MessageType = iota + 1
	MsgVoteResp
	MsgAppend
	MsgAppendResp
	// MsgSnapshot replaces a follower's log with a snapshot. A leader
	// sends it when the entries the follower needs have been compacted
	// away. It is answered with a MsgAppendResp.
	MsgSnapshot
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
//	MsgSnapshot    Snapshot: the leader's snapshot. Shared, read-only.
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
	Snapshot *Snapshot
}

// Ready is what a node needs its driver to do. Messages are sent only
// after Storage has made the promises in them durable, which is already
// true by the time Ready returns them.
type Ready struct {
	Messages []Message
	// Snapshot, when set, replaces the state machine: discard whatever it
	// holds, load Snapshot.Data, and treat every index up to
	// Snapshot.Index as applied. It is set once after a restart from a
	// stored snapshot and whenever a leader's snapshot is installed. Any
	// Committed entries in the same Ready come after it.
	Snapshot *Snapshot
	// Committed are newly committed entries, in log order, each handed
	// out once per process lifetime. After a restart the node hands out
	// the log again, from its snapshot or from index 1, as it relearns
	// the commit index, so a state machine rebuilds from scratch.
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
	// SnapshotIndex is the index the node's snapshot covers, and
	// FirstIndex the oldest entry still in its log. With no snapshot they
	// are 0 and 1.
	SnapshotIndex uint64
	FirstIndex    uint64
	// Match is, on a leader, the highest index each follower is known to
	// hold. Nil on anyone else.
	Match map[NodeID]uint64
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
	installStale    bool // install a snapshot older than what is committed
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
	// snap covers the log up to snap.Index. log holds what is kept beside
	// it: contiguous, starting no later than snap.Index+1. Entries at or
	// below snap.Index are kept only to bring slow followers up to date
	// without sending them the whole snapshot.
	snap        Snapshot
	log         []Entry
	pendingSnap *Snapshot // to hand out through the next Ready

	state   State
	leader  NodeID
	commit  uint64
	applied uint64 // highest index handed out through Ready

	elapsed int
	timeout int

	votes map[NodeID]bool   // candidate only
	next  map[NodeID]uint64 // leader only: next index to send
	match map[NodeID]uint64 // leader only: highest index known replicated
	// snapPause is, per follower, how many ticks to wait before sending
	// it another snapshot. A snapshot can take far longer to arrive than a
	// heartbeat; without the pause every heartbeat in between would send
	// it again.
	snapPause map[NodeID]int
	// heard is who has answered this leader since it last checked, and
	// sinceCheck how many ticks ago that was. A leader that goes a whole
	// election timeout without hearing from a majority cannot commit
	// anything and may well have been replaced; it steps down so that it
	// stops claiming to lead and its callers go and find who does.
	heard      map[NodeID]bool
	sinceCheck int

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

	hard, snap, log, err := cfg.Storage.Load()
	if err != nil {
		return nil, fmt.Errorf("raft: load: %w", err)
	}
	if err := checkTail(snap, log); err != nil {
		return nil, fmt.Errorf("raft: stored state is unusable: %w", err)
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
		snap:     snap,
		log:      log,
		// A snapshot holds only committed state, so that much is known
		// to be committed before hearing from anyone.
		commit:  snap.Index,
		applied: snap.Index,
	}
	if snap.Index > 0 {
		n.pendingSnap = &snap
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
	st := Status{
		ID:            n.id,
		Term:          n.term,
		State:         n.state,
		Leader:        n.leader,
		Commit:        n.commit,
		LastIndex:     n.lastIndex(),
		SnapshotIndex: n.snap.Index,
		FirstIndex:    n.firstIndex(),
	}
	if n.state == Leader {
		st.Match = make(map[NodeID]uint64, len(n.match))
		for id, m := range n.match {
			st.Match[id] = m
		}
	}
	return st
}

// Ready drains the node's outbox, a snapshot to load if there is one, and
// its newly committed entries.
func (n *Node) Ready() Ready {
	rd := Ready{Messages: n.msgs, Snapshot: n.pendingSnap}
	n.msgs = nil
	n.pendingSnap = nil
	if n.applied < n.commit {
		rd.Committed = append([]Entry(nil), n.slice(n.applied+1, n.commit)...)
		n.applied = n.commit
	}
	return rd
}

// Compact records that data is the state machine after applying every
// entry up to index, and lets go of the log before it. keep is how many
// entries at or below index to hold on to anyway, so that a follower only
// a little behind can still be caught up entry by entry instead of being
// sent the whole snapshot.
//
// index must be one the node has already handed out through Ready, and
// newer than the snapshot it has. Any node may compact, leader or not;
// each does so on its own schedule.
func (n *Node) Compact(index uint64, data []byte, keep int) error {
	if n.err != nil {
		return n.err
	}
	if index <= n.snap.Index || index > n.applied {
		return fmt.Errorf("raft: compact at %d, want an index in (%d, %d]", index, n.snap.Index, n.applied)
	}
	if keep < 0 {
		keep = 0
	}
	from := uint64(1)
	if uint64(keep) < index {
		from = index + 1 - uint64(keep)
	}
	if first := n.firstIndex(); from < first {
		from = first
	}
	snap := Snapshot{Index: index, Term: n.termAt(index), Data: data}
	// Copied, so the entries let go of can be collected.
	tail := append([]Entry(nil), n.slice(from, n.lastIndex())...)
	if err := n.storage.SaveSnapshot(snap, tail); err != nil {
		return n.fail(fmt.Errorf("raft: save snapshot: %w", err))
	}
	n.snap, n.log = snap, tail
	return nil
}

// Tick advances the node's clock by one unit. Followers and candidates
// count toward an election; leaders count toward a heartbeat.
func (n *Node) Tick() error {
	if n.err != nil {
		return n.err
	}
	n.elapsed++
	if n.state == Leader {
		for p, left := range n.snapPause {
			if left > 0 {
				n.snapPause[p] = left - 1
			}
		}
		if n.sinceCheck++; n.sinceCheck >= n.election {
			inTouch := 1 + len(n.heard) // itself, and whoever answered
			n.heard, n.sinceCheck = map[NodeID]bool{}, 0
			if inTouch < n.quorum {
				// Same term, so nothing to persist. Whoever leads now, if
				// anyone, will say so with its first message.
				return n.becomeFollower(n.term, None)
			}
		}
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
	case MsgSnapshot:
		return n.handleSnapshot(m)
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

func (n *Node) lastIndex() uint64 {
	if k := len(n.log); k > 0 {
		return n.log[k-1].Index
	}
	return n.snap.Index
}

// firstIndex is the oldest entry still in the log, or where the next one
// will go if the log is empty.
func (n *Node) firstIndex() uint64 {
	if len(n.log) > 0 {
		return n.log[0].Index
	}
	return n.snap.Index + 1
}

// hasTerm reports whether the term at index is still known: the entry is
// in the log, or it is the one the snapshot ends on. Index 0 with no
// snapshot is the empty prefix every log shares.
func (n *Node) hasTerm(index uint64) bool {
	return index == n.snap.Index || (index >= n.firstIndex() && index <= n.lastIndex())
}

// termAt returns the term at index, which must satisfy hasTerm.
func (n *Node) termAt(index uint64) uint64 {
	if index == n.snap.Index {
		return n.snap.Term
	}
	return n.log[index-n.log[0].Index].Term
}

// slice returns the log entries from index lo to index hi inclusive, both
// of which must be in the log unless the range is empty.
func (n *Node) slice(lo, hi uint64) []Entry {
	if lo > hi {
		return nil
	}
	base := n.log[0].Index
	return n.log[lo-base : hi-base+1]
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
	n.votes, n.next, n.match, n.snapPause, n.heard = nil, nil, nil, nil, nil
	n.resetElection()
	return nil
}

func (n *Node) campaign() error {
	if err := n.saveHardState(n.term+1, n.id); err != nil {
		return err
	}
	n.state = Candidate
	n.leader = None
	n.next, n.match, n.snapPause = nil, nil, nil
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
	n.snapPause = make(map[NodeID]int, len(n.peers))
	n.heard, n.sinceCheck = map[NodeID]bool{}, 0
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
	n.hearLeader(m.From)

	for i, e := range m.Entries {
		if e.Index != m.LogIndex+1+uint64(i) {
			return nil // malformed: not a contiguous run after LogIndex
		}
	}

	if m.LogIndex < n.snap.Index {
		// The message builds on an entry our snapshot has swallowed, so
		// its term can no longer be checked. It does not need to be:
		// everything up to the snapshot is committed, and the leader of
		// this term holds every committed entry. Skip what the snapshot
		// covers and carry on from its edge.
		skip := n.snap.Index - m.LogIndex
		if skip >= uint64(len(m.Entries)) {
			n.send(Message{Type: MsgAppendResp, To: m.From, LogIndex: n.snap.Index})
			return nil
		}
		m.Entries = m.Entries[skip:]
		m.LogIndex, m.LogTerm = n.snap.Index, n.snap.Term
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
			n.log = n.log[:e.Index-n.log[0].Index]
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

// hearLeader records a valid message from the leader of our term. A
// candidate that hears one has lost the election.
func (n *Node) hearLeader(from NodeID) {
	n.state = Follower
	n.leader = from
	n.votes = nil
	n.elapsed = 0
}

func (n *Node) handleSnapshot(m Message) error {
	if m.Snapshot == nil {
		return nil // malformed
	}
	s := *m.Snapshot
	if m.Term < n.term {
		n.send(Message{Type: MsgAppendResp, To: m.From, Reject: true, LogIndex: s.Index, Hint: n.lastIndex()})
		return nil
	}
	if n.state == Leader {
		return n.fail(fmt.Errorf("raft: node %d is leader of term %d and so is node %d", n.id, n.term, m.From))
	}
	n.hearLeader(m.From)

	if s.Index <= n.commit && !n.faults.installStale {
		// Nothing in it we do not already know to be committed. Installing
		// it would throw away committed entries after it.
		n.send(Message{Type: MsgAppendResp, To: m.From, LogIndex: n.commit})
		return nil
	}
	if s.Index > n.commit && n.hasTerm(s.Index) && n.termAt(s.Index) == s.Term {
		// Our log already runs through the snapshot's last entry, so it
		// matches the leader's that far and the snapshot only tells us
		// that much is committed. Keep the log, and what follows it.
		n.commit = s.Index
		n.send(Message{Type: MsgAppendResp, To: m.From, LogIndex: s.Index})
		return nil
	}
	// Our log does not reach the snapshot, or disagrees with it there.
	// Whatever we hold past our commit index came from a leader that lost,
	// so the whole log goes.
	if err := n.storage.SaveSnapshot(s, nil); err != nil {
		return n.fail(fmt.Errorf("raft: install snapshot: %w", err))
	}
	n.snap, n.log = s, nil
	n.commit, n.applied = s.Index, s.Index
	n.pendingSnap = &s
	n.send(Message{Type: MsgAppendResp, To: m.From, LogIndex: s.Index})
	return nil
}

func (n *Node) handleAppendResp(m Message) {
	if n.state != Leader || m.Term != n.term {
		return
	}
	// An answer in our own term, accepting or refusing, is a follower
	// that still takes us for the leader.
	n.heard[m.From] = true
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
	if !n.hasTerm(prev) {
		// The entry this follower needs next has been compacted away.
		n.sendSnapshot(to)
		return
	}
	end := min(prev+uint64(n.maxBatch), n.lastIndex())
	n.send(Message{
		Type:     MsgAppend,
		To:       to,
		LogIndex: prev,
		LogTerm:  n.termAt(prev),
		// Copied: a later truncation must not rewrite a message in flight.
		Entries: append([]Entry(nil), n.slice(prev+1, end)...),
		Commit:  n.commit,
	})
}

// sendSnapshot sends the leader's snapshot to a follower too far behind
// for the log, then assumes it will arrive: the next append builds on the
// snapshot's last entry. If the snapshot is lost the follower refuses that
// append and the leader ends up back here.
func (n *Node) sendSnapshot(to NodeID) {
	if n.snapPause[to] > 0 {
		// One was sent recently and may still be in flight. Say only
		// that we are still the leader; the follower will refuse this,
		// which is enough to keep it from standing for election.
		n.send(Message{Type: MsgAppend, To: to, LogIndex: n.snap.Index, LogTerm: n.snap.Term, Commit: n.commit})
		return
	}
	n.snapPause[to] = n.election
	n.next[to] = n.snap.Index + 1
	snap := n.snap // a copy: a later Compact must not change a message in flight
	n.send(Message{Type: MsgSnapshot, To: to, Snapshot: &snap})
}
