// Package cluster runs a jobq queue as a Raft-replicated state machine: a
// fixed set of nodes that agree on one log of queue operations and each
// apply it to their own in-memory queue.
//
// A Node is the piece that touches the real world for the raft package:
// it owns the timer, the network and the disk, and feeds a raft.Node that
// owns none of them. It implements jobq.Client, so code written against a
// local *jobq.Queue or a jobqhttp.Client runs against a cluster unchanged.
//
// The contract is the single-node one, cluster-wide:
//
//  1. An enqueue acknowledged to the caller survives the loss of any
//     minority of nodes.
//  2. An acked job never comes back.
//
// Delivery is still at-least-once. A change of leader ends every lease, so
// a job in flight at that moment is delivered again.
package cluster

import (
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aleks-drozy/jobq"
	"github.com/aleks-drozy/jobq/raft"
)

// Transport carries Raft messages to other nodes. Send must not block: a
// transport that cannot deliver right now drops the message, which Raft is
// built to survive. Incoming messages are handed to Node.Step.
type Transport interface {
	Send(raft.Message)
}

// Config describes one node. Only ID, Peers and Storage are required; a
// cluster of more than one node also needs a Transport.
type Config struct {
	ID raft.NodeID
	// Peers is every node in the cluster, this one included.
	Peers []raft.NodeID
	// Storage holds the Raft log. Use jobq.OpenRaftLog for a durable node.
	// The node does not close it.
	Storage   raft.Storage
	Transport Transport

	// Tick is the unit of Raft time. A follower stands for election after
	// ElectionTicks to 2*ElectionTicks of silence; a leader speaks every
	// HeartbeatTicks. Defaults: 10ms, 15, 3.
	Tick           time.Duration
	ElectionTicks  int
	HeartbeatTicks int
	// MaxEntriesPerMsg bounds one append message. Default 512.
	MaxEntriesPerMsg int

	// SnapshotEvery is how many applied entries a node lets build up past
	// its last snapshot before it takes another and drops the log behind
	// it. Each node counts for itself. Zero means 16384; negative means
	// never, and the log then grows without bound.
	SnapshotEvery int
	// SnapshotKeep is how many entries a node holds on to behind a
	// snapshot, so that a follower only that far behind is caught up from
	// the log instead of being sent the whole snapshot. Zero means a
	// quarter of SnapshotEvery; negative means none.
	SnapshotKeep int

	// Timeout is how long an operation waits for a majority to commit it
	// before giving up with jobq.ErrUnavailable. Default 5s.
	Timeout time.Duration
	// Clock stamps commands when this node leads. Default time.Now.
	Clock func() time.Time
	// ClientAddr returns the address clients should use for a node, for
	// the hint in a not-leader error. May be nil.
	ClientAddr func(raft.NodeID) string
	Logger     *log.Logger
}

// Status is a node's view of itself and the cluster.
type Status struct {
	raft.Status
	// Applied is the highest log index this node's queue reflects.
	Applied uint64
}

// Node is one member of a replicated queue. It is safe for concurrent use.
type Node struct {
	cfg  Config
	rn   *raft.Node
	sm   *machine
	sync raft.Syncer // nil if the storage is durable on return

	proposals chan *proposal
	inbox     chan raft.Message
	stop      chan struct{}
	done      chan struct{}
	closeOnce sync.Once

	status  atomic.Pointer[Status]
	failed  atomic.Pointer[error]
	metrics metrics
	// lastLeader is the last leader this node knew of. Run loop only.
	lastLeader raft.NodeID
}

type proposal struct {
	cmd   command
	reply chan result
}

// waiter is a caller parked until its entry commits. The term tells the
// entry it proposed apart from a different one that took the same index.
type waiter struct {
	term  uint64
	since time.Time
	reply chan result
}

const (
	inboxSize = 4096
	// maxBurst bounds how much input one loop iteration takes before it
	// syncs and answers, so a flood of proposals cannot starve heartbeats.
	maxBurst = 512
)

// Start builds a node from its storage and begins running it.
func Start(cfg Config) (*Node, error) {
	if cfg.Tick <= 0 {
		cfg.Tick = 10 * time.Millisecond
	}
	if cfg.ElectionTicks <= 0 {
		cfg.ElectionTicks = 15
	}
	if cfg.HeartbeatTicks <= 0 {
		cfg.HeartbeatTicks = 3
	}
	if cfg.MaxEntriesPerMsg <= 0 {
		cfg.MaxEntriesPerMsg = 512
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 5 * time.Second
	}
	if cfg.SnapshotEvery == 0 {
		cfg.SnapshotEvery = 16384
	}
	if cfg.SnapshotKeep == 0 {
		cfg.SnapshotKeep = cfg.SnapshotEvery / 4
	}
	if cfg.SnapshotKeep < 0 {
		cfg.SnapshotKeep = 0
	}
	if cfg.Clock == nil {
		cfg.Clock = time.Now
	}
	if cfg.Logger == nil {
		cfg.Logger = log.New(discard{}, "", 0)
	}
	if cfg.Transport == nil {
		if len(cfg.Peers) > 1 {
			return nil, errors.New("cluster: a Transport is required for more than one node")
		}
		cfg.Transport = nullTransport{}
	}

	rn, err := raft.New(raft.Config{
		ID:               cfg.ID,
		Peers:            cfg.Peers,
		ElectionTicks:    cfg.ElectionTicks,
		HeartbeatTicks:   cfg.HeartbeatTicks,
		MaxEntriesPerMsg: cfg.MaxEntriesPerMsg,
		Storage:          cfg.Storage,
	})
	if err != nil {
		return nil, err
	}
	n := &Node{
		cfg:       cfg,
		rn:        rn,
		sm:        newMachine(),
		proposals: make(chan *proposal),
		inbox:     make(chan raft.Message, inboxSize),
		stop:      make(chan struct{}),
		done:      make(chan struct{}),
	}
	n.sync, _ = cfg.Storage.(raft.Syncer)
	n.publish(n.rn.Status())
	go n.run()
	return n, nil
}

// Step hands the node a message from a peer. It never blocks; when the
// node is saturated the message is dropped, like any lost packet.
func (n *Node) Step(m raft.Message) {
	select {
	case n.inbox <- m:
	default:
	}
}

// Status reports the node's current view. It is a snapshot taken at the
// end of the node's last loop iteration.
func (n *Node) Status() Status { return *n.status.Load() }

// Done is closed when the node has stopped, whether by Close or because it
// could no longer write to its storage.
func (n *Node) Done() <-chan struct{} { return n.done }

// Err reports why the node stopped on its own, or nil if it has not.
func (n *Node) Err() error {
	if err := n.failed.Load(); err != nil {
		return *err
	}
	return nil
}

// LeaderAddr returns the client address of the node this one believes
// leads (itself included), or "" when it knows of no leader or was not
// given addresses.
func (n *Node) LeaderAddr() string {
	st := n.Status()
	if st.Leader == raft.None || n.cfg.ClientAddr == nil {
		return ""
	}
	return n.cfg.ClientAddr(st.Leader)
}

// Close stops the node. It does not close the Storage it was given.
func (n *Node) Close() error {
	n.closeOnce.Do(func() { close(n.stop) })
	<-n.done
	return n.sm.queue().Close()
}

// run is the only goroutine that touches the raft.Node.
func (n *Node) run() {
	defer close(n.done)
	ticker := time.NewTicker(n.cfg.Tick)
	defer ticker.Stop()
	waiters := map[uint64]waiter{}
	defer func() {
		err := n.deadErr()
		for _, w := range waiters {
			w.reply <- result{err: err}
		}
	}()

	for {
		var err error
		select {
		case <-n.stop:
			return
		case <-ticker.C:
			err = n.rn.Tick()
		case m := <-n.inbox:
			err = n.rn.Step(m)
		case p := <-n.proposals:
			err = n.propose(p, waiters)
		}
		for i := 0; err == nil && i < maxBurst; i++ {
			select {
			case m := <-n.inbox:
				err = n.rn.Step(m)
			case p := <-n.proposals:
				err = n.propose(p, waiters)
			default:
				i = maxBurst
			}
		}
		// Nothing leaves this node until what it rests on is on disk:
		// not a vote, not an acknowledgement, not a reply to a client.
		// One Sync covers everything the burst above wrote.
		if err == nil && n.sync != nil {
			err = n.sync.Sync()
		}
		if err != nil {
			n.fail(err)
			return
		}

		rd := n.rn.Ready()
		n.send(rd.Messages)
		if snap := rd.Snapshot; snap != nil {
			// The queue is replaced whole: on a restart by this node's own
			// snapshot, otherwise by the leader's because this node fell
			// too far behind for the log.
			if err := n.sm.restore(snap.Data, snap.Index); err != nil {
				n.fail(err)
				return
			}
			n.metrics.snapshotsLoaded.Add(1)
			n.cfg.Logger.Printf("cluster: node %d loaded a snapshot at index %d (%d bytes)", n.cfg.ID, snap.Index, len(snap.Data))
			for index, w := range waiters {
				if index <= snap.Index {
					// Proposed here while this node led, and never seen
					// to commit before the snapshot covered that index.
					// Whether it is in there cannot be told.
					delete(waiters, index)
					w.reply <- result{err: jobq.ErrUnavailable}
				}
			}
		}
		for _, e := range rd.Committed {
			res := n.sm.apply(e)
			w, ok := waiters[e.Index]
			if !ok {
				continue
			}
			delete(waiters, e.Index)
			if w.term != e.Term {
				// Another leader's entry took this index, so ours was
				// discarded and can never commit. That is a definite
				// "did not happen", which is safe to retry.
				res = result{err: n.notLeader()}
			} else {
				n.metrics.commit.observe(time.Since(w.since))
			}
			w.reply <- res
		}

		st := n.rn.Status()
		if every := n.cfg.SnapshotEvery; every > 0 {
			if applied := n.sm.applied.Load(); applied >= st.SnapshotIndex+uint64(every) {
				if err := n.snapshot(applied); err != nil {
					n.fail(err)
					return
				}
				st = n.rn.Status()
			}
		}
		n.publish(st)
	}
}

// fail records why the node is stopping on its own.
func (n *Node) fail(err error) {
	werr := fmt.Errorf("cluster: node %d stopped: %w", n.cfg.ID, err)
	n.failed.Store(&werr)
	n.cfg.Logger.Printf("%v", werr)
}

// snapshot captures the queue as it stands after the entry at applied and
// drops the log behind it. It runs on the node's own goroutine, between
// entries, so the picture is of exactly one log index; the price is that
// the node does nothing else, heartbeats included, until the snapshot is
// on disk.
func (n *Node) snapshot(applied uint64) error {
	start := time.Now()
	data := n.sm.snapshot()
	if err := n.rn.Compact(applied, data, n.cfg.SnapshotKeep); err != nil {
		return err
	}
	took := time.Since(start)
	n.metrics.snapshots.Add(1)
	n.metrics.snapshotBytes.Store(uint64(len(data)))
	n.metrics.snapshotNanos.Store(uint64(took))
	n.cfg.Logger.Printf("cluster: node %d snapshotted at index %d (%d bytes, %s)", n.cfg.ID, applied, len(data), took.Round(time.Microsecond))
	return nil
}

// propose stamps and appends one command if this node leads.
func (n *Node) propose(p *proposal, waiters map[uint64]waiter) error {
	if n.rn.Status().State != raft.Leader {
		p.reply <- result{err: n.notLeader()}
		return nil
	}
	// The stamp never runs behind what the queue has already seen, so a
	// new leader whose wall clock trails the old one's cannot move the
	// queue's clock backwards.
	at := n.cfg.Clock().UnixNano()
	if seen := n.sm.clock.Load(); at < seen {
		at = seen
	}
	p.cmd.At = time.Unix(0, at)

	index, term, err := n.rn.Propose(p.cmd.encode())
	if errors.Is(err, raft.ErrNotLeader) {
		p.reply <- result{err: n.notLeader()}
		return nil
	}
	if err != nil {
		p.reply <- result{err: err}
		return err
	}
	waiters[index] = waiter{term: term, since: time.Now(), reply: p.reply}
	return nil
}

// send forwards a batch of outgoing messages. A leader that took several
// proposals in one burst produced one append per proposal for each
// follower, each a superset of the one before (same starting point, more
// entries). Only the last of each run is worth sending.
func (n *Node) send(msgs []raft.Message) {
	type key struct {
		to   raft.NodeID
		from uint64
	}
	last := map[key]int{}
	for i, m := range msgs {
		if m.Type == raft.MsgAppend {
			last[key{m.To, m.LogIndex}] = i
		}
	}
	for i, m := range msgs {
		if m.Type == raft.MsgAppend && last[key{m.To, m.LogIndex}] != i {
			continue
		}
		n.cfg.Transport.Send(m)
	}
}

func (n *Node) publish(rs raft.Status) {
	if rs.Leader != raft.None && rs.Leader != n.lastLeader {
		n.lastLeader = rs.Leader
		n.metrics.leaderChanges.Add(1)
	}
	st := Status{Status: rs, Applied: n.sm.applied.Load()}
	n.status.Store(&st)
}

func (n *Node) notLeader() error {
	st := n.rn.Status()
	hint := ""
	if st.State != raft.Leader && st.Leader != raft.None && n.cfg.ClientAddr != nil {
		hint = n.cfg.ClientAddr(st.Leader)
	}
	return &jobq.NotLeaderError{Leader: hint}
}

func (n *Node) deadErr() error {
	if err := n.failed.Load(); err != nil {
		return *err
	}
	return jobq.ErrClosed
}

// do proposes one command and waits for it to commit and apply.
func (n *Node) do(cmd command) result {
	p := &proposal{cmd: cmd, reply: make(chan result, 1)}
	timer := time.NewTimer(n.cfg.Timeout)
	defer timer.Stop()
	select {
	case n.proposals <- p:
	case <-n.done:
		return result{err: n.deadErr()}
	case <-timer.C:
		return result{err: jobq.ErrUnavailable}
	}
	select {
	case r := <-p.reply:
		return r
	case <-n.done:
		return result{err: n.deadErr()}
	case <-timer.C:
		// The entry is in this node's log and may yet commit. The caller
		// is told the truth: the outcome is unknown.
		return result{err: jobq.ErrUnavailable}
	}
}

var _ jobq.Client = (*Node)(nil)

// Enqueue implements jobq.Client. It returns once a majority has the job
// on disk.
func (n *Node) Enqueue(topic string, payload []byte, opts ...jobq.EnqueueOption) (string, error) {
	// Refused here rather than in the log: an entry that can only fail is
	// a wasted round of replication.
	if strings.TrimSpace(topic) == "" {
		return "", jobq.ErrEmptyTopic
	}
	if strings.HasSuffix(topic, jobq.DeadLetterSuffix) {
		return "", jobq.ErrReservedTopic
	}
	var o jobq.EnqueueOptions
	for _, opt := range opts {
		opt(&o)
	}
	if len(o.RequestID) > jobq.MaxRequestIDLen {
		return "", jobq.ErrRequestIDTooLong
	}
	r := n.do(command{Op: opEnqueue, Topic: topic, Payload: payload, Delay: o.Delay, MaxAttempts: o.MaxAttempts, RequestID: o.RequestID})
	return r.id, r.err
}

// Dequeue implements jobq.Client.
//
// An empty poll is answered by the leader from its own state and never
// reaches the log; otherwise every idle consumer would grow the log with
// each poll. That shortcut can only say "nothing ready". Handing out a job
// always takes a committed entry.
func (n *Node) Dequeue(topic string, visibility time.Duration) (*jobq.Job, jobq.Lease, error) {
	if visibility <= 0 {
		return nil, jobq.Lease{}, jobq.ErrNonPositiveDuration
	}
	select {
	case <-n.done:
		return nil, jobq.Lease{}, n.deadErr()
	default:
	}
	if st := n.Status(); st.State != raft.Leader {
		return nil, jobq.Lease{}, n.notLeaderFrom(st)
	}
	// "Now" is the later of this node's clock and the queue's, the same
	// stamp the command would carry.
	at := n.cfg.Clock()
	if seen := n.sm.now(); at.Before(seen) {
		at = seen
	}
	if !n.sm.queue().Pending(topic, at) {
		return nil, jobq.Lease{}, jobq.ErrNoJob
	}
	r := n.do(command{Op: opDequeue, Topic: topic, Duration: visibility})
	return r.job, r.lease, r.err
}

// notLeaderFrom is notLeader for callers outside the run loop, which may
// read only the published snapshot.
func (n *Node) notLeaderFrom(st Status) error {
	hint := ""
	if st.Leader != raft.None && st.Leader != n.cfg.ID && n.cfg.ClientAddr != nil {
		hint = n.cfg.ClientAddr(st.Leader)
	}
	return &jobq.NotLeaderError{Leader: hint}
}

// Ack implements jobq.Client. It returns once a majority has recorded the
// ack, after which the job cannot be delivered again.
func (n *Node) Ack(lease jobq.Lease) error {
	return n.do(command{Op: opAck, Topic: lease.Topic, LeaseID: lease.ID}).err
}

// Nack implements jobq.Client.
func (n *Node) Nack(lease jobq.Lease) error {
	return n.do(command{Op: opNack, Topic: lease.Topic, LeaseID: lease.ID}).err
}

// Extend implements jobq.Client.
func (n *Node) Extend(lease jobq.Lease, d time.Duration) (time.Time, error) {
	if d <= 0 {
		return time.Time{}, jobq.ErrNonPositiveDuration
	}
	r := n.do(command{Op: opExtend, Topic: lease.Topic, LeaseID: lease.ID, Duration: d})
	return r.deadline, r.err
}

// Stats, Topics and Peek read this node's own copy of the queue without
// consulting the cluster. A follower's copy trails the leader's by however
// far replication is behind, and a lease past its deadline is counted as
// in flight until the next operation on its topic observes the expiry.

// Stats returns this node's counts for one topic.
func (n *Node) Stats(topic string) jobq.Stats { return n.sm.queue().Stats(topic) }

// Topics lists the topics this node knows.
func (n *Node) Topics() []string { return n.sm.queue().Topics() }

// Peek lists jobs on this node without leasing them.
func (n *Node) Peek(topic string, limit int, includeInflight bool) (ready, inflight []*jobq.Job, err error) {
	return n.sm.queue().Peek(topic, limit, includeInflight)
}

type nullTransport struct{}

func (nullTransport) Send(raft.Message) {}

type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }
