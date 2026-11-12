package cluster

import (
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aleks-drozy/jobq"
	"github.com/aleks-drozy/jobq/raft"
)

// These tests run real nodes on a real ticker, so unlike the rest of the
// repository they wait on wall-clock time. The algorithm itself is proven
// without clocks in raft/sim_test.go; what is tested here is the layer
// above it, where the timer, the goroutines and the queue meet.

// testNet is an in-memory network: a message is delivered by stepping the
// destination node directly, unless the link is cut or the node is down.
type testNet struct {
	mu    sync.Mutex
	nodes map[raft.NodeID]*Node
	cut   map[[2]raft.NodeID]bool
}

func newTestNet() *testNet {
	return &testNet{nodes: map[raft.NodeID]*Node{}, cut: map[[2]raft.NodeID]bool{}}
}

type testLink struct{ net *testNet }

func (l testLink) Send(m raft.Message) {
	l.net.mu.Lock()
	dst := l.net.nodes[m.To]
	blocked := l.net.cut[[2]raft.NodeID{m.From, m.To}]
	l.net.mu.Unlock()
	if dst != nil && !blocked {
		dst.Step(m)
	}
}

// isolate cuts every link to and from id.
func (tn *testNet) isolate(id raft.NodeID, peers []raft.NodeID) {
	tn.mu.Lock()
	defer tn.mu.Unlock()
	for _, p := range peers {
		if p != id {
			tn.cut[[2]raft.NodeID{id, p}] = true
			tn.cut[[2]raft.NodeID{p, id}] = true
		}
	}
}

func (tn *testNet) heal() {
	tn.mu.Lock()
	defer tn.mu.Unlock()
	tn.cut = map[[2]raft.NodeID]bool{}
}

// testClock is the wall clock plus an offset tests can push forward.
type testClock struct{ offset atomic.Int64 }

func (c *testClock) Now() time.Time          { return time.Now().Add(time.Duration(c.offset.Load())) }
func (c *testClock) Advance(d time.Duration) { c.offset.Add(int64(d)) }

type testCluster struct {
	t       *testing.T
	net     *testNet
	clock   *testClock
	ids     []raft.NodeID
	stores  map[raft.NodeID]*raft.MemoryStorage
	timeout time.Duration

	mu    sync.Mutex
	nodes map[raft.NodeID]*Node
}

func newTestCluster(t *testing.T, size int) *testCluster {
	t.Helper()
	c := &testCluster{
		t:       t,
		net:     newTestNet(),
		clock:   &testClock{},
		stores:  map[raft.NodeID]*raft.MemoryStorage{},
		nodes:   map[raft.NodeID]*Node{},
		timeout: 5 * time.Second,
	}
	for i := 1; i <= size; i++ {
		id := raft.NodeID(i)
		c.ids = append(c.ids, id)
		c.stores[id] = raft.NewMemoryStorage()
	}
	t.Cleanup(func() {
		for _, id := range c.ids {
			c.stop(id)
		}
	})
	return c
}

func (c *testCluster) startAll() *testCluster {
	for _, id := range c.ids {
		c.start(id)
	}
	return c
}

func (c *testCluster) start(id raft.NodeID) *Node {
	c.t.Helper()
	n, err := Start(Config{
		ID:             id,
		Peers:          c.ids,
		Storage:        c.stores[id],
		Transport:      testLink{c.net},
		Tick:           5 * time.Millisecond,
		ElectionTicks:  10,
		HeartbeatTicks: 2,
		Timeout:        c.timeout,
		Clock:          c.clock.Now,
		ClientAddr:     func(id raft.NodeID) string { return fmt.Sprintf("node-%d", id) },
	})
	if err != nil {
		c.t.Fatalf("starting node %d: %v", id, err)
	}
	c.mu.Lock()
	c.nodes[id] = n
	c.mu.Unlock()
	c.net.mu.Lock()
	c.net.nodes[id] = n
	c.net.mu.Unlock()
	return n
}

// stop takes a node off the network and shuts it down. Its storage is
// kept, so start brings it back the way a restart would.
func (c *testCluster) stop(id raft.NodeID) {
	c.net.mu.Lock()
	delete(c.net.nodes, id)
	c.net.mu.Unlock()
	c.mu.Lock()
	n := c.nodes[id]
	delete(c.nodes, id)
	c.mu.Unlock()
	if n != nil {
		_ = n.Close()
	}
}

func (c *testCluster) live() []*Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []*Node
	for _, id := range c.ids {
		if n := c.nodes[id]; n != nil {
			out = append(out, n)
		}
	}
	return out
}

func (c *testCluster) node(id raft.NodeID) *Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[id]
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// leaderAmong waits until one of the given nodes leads and the rest of
// them agree that it does.
func (c *testCluster) leaderAmong(ids ...raft.NodeID) *Node {
	c.t.Helper()
	var found *Node
	eventually(c.t, "a leader", func() bool {
		found = nil
		var nodes []*Node
		for _, id := range ids {
			if n := c.node(id); n != nil {
				nodes = append(nodes, n)
			}
		}
		for _, n := range nodes {
			if n.Status().State == raft.Leader {
				found = n
			}
		}
		if found == nil {
			return false
		}
		lead := found.Status()
		for _, n := range nodes {
			if st := n.Status(); st.Leader != lead.ID || st.Term != lead.Term {
				return false
			}
		}
		return true
	})
	return found
}

func (c *testCluster) leader() *Node {
	c.t.Helper()
	var ids []raft.NodeID
	for _, n := range c.live() {
		ids = append(ids, n.cfg.ID)
	}
	return c.leaderAmong(ids...)
}

// converged waits until every live node has applied the same, complete log.
func (c *testCluster) converged() {
	c.t.Helper()
	eventually(c.t, "replicas to converge", func() bool {
		nodes := c.live()
		if len(nodes) == 0 {
			return false
		}
		first := nodes[0].Status()
		if first.LastIndex == 0 || first.Applied != first.LastIndex {
			return false
		}
		for _, n := range nodes[1:] {
			st := n.Status()
			if st.Applied != first.Applied || st.LastIndex != first.LastIndex || st.Term != first.Term {
				return false
			}
		}
		return true
	})
}

// do runs op against whichever node leads, moving on when a node says it
// does not, the way a real client follows the cluster.
func (c *testCluster) do(op func(*Node) error) error {
	deadline := time.Now().Add(20 * time.Second)
	for {
		var target *Node
		for _, n := range c.live() {
			if target == nil || n.Status().State == raft.Leader {
				target = n
			}
		}
		var err error = jobq.ErrNotLeader
		if target != nil {
			err = op(target)
		}
		if !errors.Is(err, jobq.ErrNotLeader) && !errors.Is(err, jobq.ErrClosed) {
			return err
		}
		if time.Now().After(deadline) {
			return err
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func (c *testCluster) enqueue(topic string, payload []byte, opts ...jobq.EnqueueOption) (string, error) {
	var id string
	err := c.do(func(n *Node) (err error) {
		id, err = n.Enqueue(topic, payload, opts...)
		return err
	})
	return id, err
}

func (c *testCluster) dequeue(topic string, vis time.Duration) (*jobq.Job, jobq.Lease, error) {
	var job *jobq.Job
	var lease jobq.Lease
	err := c.do(func(n *Node) (err error) {
		job, lease, err = n.Dequeue(topic, vis)
		return err
	})
	return job, lease, err
}

func (c *testCluster) ack(l jobq.Lease) error {
	return c.do(func(n *Node) error { return n.Ack(l) })
}

// snapshot renders everything observable about a node's queue: per topic,
// its counters and every job in ready and in-flight order, IDs included.
func snapshot(n *Node) string {
	topics := n.Topics()
	sort.Strings(topics)
	out := ""
	for _, topic := range topics {
		ready, inflight, _ := n.Peek(topic, 0, true)
		out += fmt.Sprintf("%s %+v\n", topic, n.Stats(topic))
		for _, j := range ready {
			out += fmt.Sprintf("  ready    %+v\n", *j)
		}
		for _, j := range inflight {
			out += fmt.Sprintf("  inflight %+v\n", *j)
		}
	}
	return out
}

func mustEnqueue(t *testing.T, c *testCluster, topic, payload string, opts ...jobq.EnqueueOption) string {
	t.Helper()
	id, err := c.enqueue(topic, []byte(payload), opts...)
	if err != nil {
		t.Fatalf("enqueue %q: %v", payload, err)
	}
	return id
}

// --- behaviour -----------------------------------------------------------

func TestSingleNodeBehavesLikeAQueue(t *testing.T) {
	c := newTestCluster(t, 1).startAll()
	n := c.leader()

	if _, _, err := n.Dequeue("work", time.Minute); !errors.Is(err, jobq.ErrNoJob) {
		t.Fatalf("dequeue from an empty topic: %v, want ErrNoJob", err)
	}
	id, err := n.Enqueue("work", []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	job, lease, err := n.Dequeue("work", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if job.ID != id || string(job.Payload) != "hello" || job.Attempt != 1 || lease.JobID != id || lease.Topic != "work" {
		t.Fatalf("got job %+v lease %+v", job, lease)
	}
	deadline, err := n.Extend(lease, time.Hour)
	if err != nil || !deadline.After(lease.Deadline) {
		t.Fatalf("extend: deadline %v (was %v), err %v", deadline, lease.Deadline, err)
	}
	if err := n.Ack(lease); err != nil {
		t.Fatal(err)
	}
	if err := n.Ack(lease); !errors.Is(err, jobq.ErrUnknownLease) {
		t.Errorf("second ack: %v, want ErrUnknownLease", err)
	}
	if err := n.Nack(jobq.Lease{ID: "nope", Topic: "never"}); !errors.Is(err, jobq.ErrUnknownLease) {
		t.Errorf("nack of a made-up lease: %v, want ErrUnknownLease", err)
	}
	if st := n.Stats("work"); st.Enqueued != 1 || st.Acked != 1 || st.Ready != 0 || st.InFlight != 0 {
		t.Errorf("stats %+v", st)
	}

	// The same refusals a local queue gives, and none of them may cost a
	// log entry.
	c.converged()
	before := n.Status().LastIndex
	if _, err := n.Enqueue(" ", nil); !errors.Is(err, jobq.ErrEmptyTopic) {
		t.Errorf("empty topic: %v", err)
	}
	if _, err := n.Enqueue("work.dlq", nil); !errors.Is(err, jobq.ErrReservedTopic) {
		t.Errorf("reserved topic: %v", err)
	}
	if _, _, err := n.Dequeue("work", 0); !errors.Is(err, jobq.ErrNonPositiveDuration) {
		t.Errorf("zero visibility: %v", err)
	}
	if _, err := n.Extend(lease, -time.Second); !errors.Is(err, jobq.ErrNonPositiveDuration) {
		t.Errorf("negative extend: %v", err)
	}
	c.converged()
	if after := n.Status().LastIndex; after != before {
		t.Errorf("refused calls grew the log by %d entries", after-before)
	}
}

func TestFollowerRefusesAndNamesTheLeader(t *testing.T) {
	c := newTestCluster(t, 3).startAll()
	lead := c.leader()
	for _, n := range c.live() {
		if n == lead {
			continue
		}
		_, err := n.Enqueue("work", []byte("x"))
		var nl *jobq.NotLeaderError
		if !errors.As(err, &nl) || !errors.Is(err, jobq.ErrNotLeader) {
			t.Fatalf("follower enqueue: %v, want a NotLeaderError", err)
		}
		if want := fmt.Sprintf("node-%d", lead.cfg.ID); nl.Leader != want {
			t.Errorf("hint = %q, want %q", nl.Leader, want)
		}
		if _, _, err := n.Dequeue("work", time.Minute); !errors.Is(err, jobq.ErrNotLeader) {
			t.Errorf("follower dequeue: %v, want ErrNotLeader", err)
		}
	}
	c.converged()
	for _, n := range c.live() {
		if st := n.Stats("work"); st.Enqueued != 0 {
			t.Errorf("node %d holds a job a follower refused: %+v", n.cfg.ID, st)
		}
	}
}

// An idle consumer polls forever. If every empty poll were a log entry the
// log would grow without anyone doing any work.
func TestIdlePollsDoNotGrowTheLog(t *testing.T) {
	c := newTestCluster(t, 3).startAll()
	lead := c.leader()
	mustEnqueue(t, c, "work", "only")
	c.converged()
	before := lead.Status().LastIndex

	for i := 0; i < 200; i++ {
		if _, _, err := lead.Dequeue("idle", time.Minute); !errors.Is(err, jobq.ErrNoJob) {
			t.Fatalf("poll %d: %v, want ErrNoJob", i, err)
		}
	}
	if after := lead.Status().LastIndex; after != before {
		t.Fatalf("200 empty polls grew the log from %d to %d", before, after)
	}

	// The shortcut must not hide work that time has made available: a
	// lease that has run out is work, even though nothing has been
	// written since it was issued.
	job, _, err := lead.Dequeue("work", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := lead.Dequeue("work", time.Second); !errors.Is(err, jobq.ErrNoJob) {
		t.Fatalf("job leased twice: %v", err)
	}
	c.clock.Advance(2 * time.Second)
	again, _, err := lead.Dequeue("work", time.Second)
	if err != nil {
		t.Fatalf("expired lease was not redelivered: %v", err)
	}
	if again.ID != job.ID || again.Attempt != 2 {
		t.Errorf("redelivery was %+v, want job %s on attempt 2", again, job.ID)
	}
}

// The reason replicas can be trusted to agree: drive one cluster through a
// long random history, restart a follower halfway so it has to rebuild from
// the log alone, and require every node to hold exactly the same queue,
// down to job IDs, lease order and counters.
func TestReplicasEndIdentical(t *testing.T) {
	c := newTestCluster(t, 3).startAll()
	c.leader()
	rng := rand.New(rand.NewSource(7))
	topics := []string{"alpha", "beta"}
	var leases []jobq.Lease

	step := func(i int) {
		topic := topics[rng.Intn(len(topics))]
		switch r := rng.Intn(100); {
		case r < 35:
			opts := []jobq.EnqueueOption{jobq.WithMaxAttempts(1 + rng.Intn(3))}
			if rng.Intn(5) == 0 {
				opts = append(opts, jobq.WithDelay(time.Duration(1+rng.Intn(50))*time.Millisecond))
			}
			mustEnqueue(t, c, topic, fmt.Sprintf("job-%d", i), opts...)
		case r < 65:
			if rng.Intn(4) == 0 {
				topic = jobq.DeadLetterTopic(topic)
			}
			vis := time.Duration(1+rng.Intn(2000)) * time.Millisecond
			_, lease, err := c.dequeue(topic, vis)
			if err == nil {
				leases = append(leases, lease)
			} else if !errors.Is(err, jobq.ErrNoJob) {
				t.Fatalf("dequeue: %v", err)
			}
		case r < 90 && len(leases) > 0:
			k := rng.Intn(len(leases))
			lease := leases[k]
			leases = append(leases[:k], leases[k+1:]...)
			var err error
			switch rng.Intn(3) {
			case 0:
				err = c.ack(lease)
			case 1:
				err = c.do(func(n *Node) error { return n.Nack(lease) })
			default:
				err = c.do(func(n *Node) error {
					_, err := n.Extend(lease, time.Duration(1+rng.Intn(3000))*time.Millisecond)
					return err
				})
				leases = append(leases, lease)
			}
			if err != nil && !errors.Is(err, jobq.ErrUnknownLease) {
				t.Fatalf("settle: %v", err)
			}
		default:
			c.clock.Advance(time.Duration(rng.Intn(1500)) * time.Millisecond)
		}
		// Ask one node, and only one, about its queue. If a read could
		// change what a node holds, that node would now differ from the
		// others.
		if nodes := c.live(); rng.Intn(3) == 0 {
			n := nodes[rng.Intn(len(nodes))]
			n.Stats(topic)
			_, _, _ = n.Peek(topic, 5, true)
		}
	}

	for i := 0; i < 250; i++ {
		step(i)
	}
	// A follower that restarts has no queue at all until it replays.
	var follower raft.NodeID
	lead := c.leader()
	for _, id := range c.ids {
		if id != lead.cfg.ID {
			follower = id
		}
	}
	c.stop(follower)
	for i := 250; i < 400; i++ {
		step(i)
	}
	c.start(follower)
	for i := 400; i < 500; i++ {
		step(i)
	}

	c.converged()
	nodes := c.live()
	want := snapshot(nodes[0])
	for _, n := range nodes[1:] {
		if got := snapshot(n); got != want {
			t.Fatalf("node %d and node %d disagree.\n--- node %d\n%s\n--- node %d\n%s",
				nodes[0].cfg.ID, n.cfg.ID, nodes[0].cfg.ID, want, n.cfg.ID, got)
		}
	}
	// Guard against agreeing on nothing.
	total := jobq.Stats{}
	for _, topic := range nodes[0].Topics() {
		st := nodes[0].Stats(topic)
		total.Enqueued += st.Enqueued
		total.Acked += st.Acked
		total.DeadLetters += st.DeadLetters
	}
	if total.Enqueued < 100 || total.Acked < 10 || total.DeadLetters < 5 {
		t.Fatalf("history too thin to prove anything: %+v", total)
	}
}

// Rule 1: an enqueue the caller saw acknowledged survives losing the node
// that acknowledged it.
func TestAcknowledgedEnqueuesSurviveLosingTheLeader(t *testing.T) {
	c := newTestCluster(t, 3).startAll()
	want := map[string]bool{}
	for i := 0; i < 40; i++ {
		want[mustEnqueue(t, c, "work", fmt.Sprintf("job-%d", i))] = true
	}
	old := c.leader()
	c.stop(old.cfg.ID)

	lead := c.leader()
	if lead.cfg.ID == old.cfg.ID {
		t.Fatal("the stopped node still leads")
	}
	for len(want) > 0 {
		job, lease, err := c.dequeue("work", time.Minute)
		if err != nil {
			t.Fatalf("%d acknowledged jobs missing after the leader was lost: %v", len(want), err)
		}
		if !want[job.ID] {
			t.Fatalf("job %s delivered twice or never enqueued", job.ID)
		}
		delete(want, job.ID)
		if err := c.ack(lease); err != nil {
			t.Fatal(err)
		}
	}
}

// Rule 2: an acked job never comes back, whoever leads next.
func TestAckedJobsStayGoneAfterLosingTheLeader(t *testing.T) {
	c := newTestCluster(t, 3).startAll()
	for i := 0; i < 30; i++ {
		mustEnqueue(t, c, "work", fmt.Sprintf("job-%d", i))
	}
	acked := map[string]bool{}
	for i := 0; i < 20; i++ {
		job, lease, err := c.dequeue("work", time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if err := c.ack(lease); err != nil {
			t.Fatal(err)
		}
		acked[job.ID] = true
	}
	c.stop(c.leader().cfg.ID)
	c.leader()

	seen := 0
	for {
		job, lease, err := c.dequeue("work", time.Minute)
		if errors.Is(err, jobq.ErrNoJob) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if acked[job.ID] {
			t.Fatalf("acked job %s was delivered again by the new leader", job.ID)
		}
		seen++
		if err := c.ack(lease); err != nil {
			t.Fatal(err)
		}
	}
	if seen != 10 {
		t.Fatalf("new leader delivered %d jobs, want the 10 that were never acked", seen)
	}
}

// A change of leader ends every lease: the old lease is dead, and the job
// is delivered again with its attempt counted.
func TestLeaderChangeEndsLeases(t *testing.T) {
	c := newTestCluster(t, 3).startAll()
	id := mustEnqueue(t, c, "work", "in-flight")
	_, lease, err := c.dequeue("work", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	c.stop(c.leader().cfg.ID)
	c.leader()

	if err := c.ack(lease); !errors.Is(err, jobq.ErrUnknownLease) {
		t.Fatalf("ack with the old leader's lease: %v, want ErrUnknownLease", err)
	}
	job, _, err := c.dequeue("work", time.Hour)
	if err != nil {
		t.Fatalf("job was not redelivered after the leader changed: %v", err)
	}
	if job.ID != id || job.Attempt != 2 {
		t.Fatalf("redelivered %+v, want job %s on attempt 2", job, id)
	}
}

// The reason Dequeue goes through the log. A leader cut off from the
// cluster still believes it leads. If it could lease jobs on its own word,
// it would hand out a job the real leader is also handing out, and its
// ack would be worth nothing.
func TestCutOffLeaderCannotHandOutJobs(t *testing.T) {
	c := newTestCluster(t, 3)
	c.timeout = 400 * time.Millisecond
	c.startAll()
	id := mustEnqueue(t, c, "work", "contested")
	c.converged()

	old := c.leader()
	var rest []raft.NodeID
	for _, nid := range c.ids {
		if nid != old.cfg.ID {
			rest = append(rest, nid)
		}
	}
	c.net.isolate(old.cfg.ID, c.ids)

	// The cut-off node still thinks it leads and still sees the job.
	if st := old.Status(); st.State != raft.Leader {
		t.Fatalf("isolated node is %v, expected it to still believe it leads", st.State)
	}
	if job, _, err := old.Dequeue("work", time.Hour); !errors.Is(err, jobq.ErrUnavailable) {
		t.Fatalf("cut-off leader's dequeue returned job %+v, err %v; want ErrUnavailable", job, err)
	}

	lead := c.leaderAmong(rest...)
	job, lease, err := lead.Dequeue("work", time.Hour)
	if err != nil {
		t.Fatalf("the real leader could not lease the job: %v", err)
	}
	if job.ID != id || job.Attempt != 1 {
		t.Fatalf("real leader delivered %+v, want %s on its first attempt", job, id)
	}
	if err := lead.Ack(lease); err != nil {
		t.Fatal(err)
	}

	// Back on the network, the old leader gives up its unconfirmed entry
	// and ends up holding exactly what everyone else holds.
	c.net.heal()
	c.converged()
	want := snapshot(lead)
	for _, n := range c.live() {
		if got := snapshot(n); got != want {
			t.Fatalf("node %d diverged after the partition healed:\n%s\nwant:\n%s", n.cfg.ID, got, want)
		}
	}
	if st := old.Stats("work"); st.Acked != 1 || st.Ready != 0 || st.InFlight != 0 {
		t.Fatalf("old leader's queue after healing: %+v", st)
	}
}

// A whole-cluster restart rebuilds every queue from the log: acked jobs
// stay gone, waiting jobs are still waiting, and leased jobs are waiting
// again.
func TestWholeClusterRestartRebuildsFromTheLog(t *testing.T) {
	c := newTestCluster(t, 3).startAll()
	for i := 0; i < 6; i++ {
		mustEnqueue(t, c, "work", fmt.Sprintf("job-%d", i))
	}
	acked := map[string]bool{}
	for i := 0; i < 2; i++ {
		job, lease, err := c.dequeue("work", time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		if err := c.ack(lease); err != nil {
			t.Fatal(err)
		}
		acked[job.ID] = true
	}
	if _, _, err := c.dequeue("work", time.Hour); err != nil { // left in flight
		t.Fatal(err)
	}
	c.converged()

	for _, id := range c.ids {
		c.stop(id)
	}
	c.startAll()
	c.leader()
	c.converged()

	for _, n := range c.live() {
		if st := n.Stats("work"); st.Enqueued != 6 || st.Acked != 2 || st.Ready != 4 || st.InFlight != 0 {
			t.Fatalf("node %d after restart: %+v, want 6 enqueued, 2 acked, 4 ready", n.cfg.ID, st)
		}
	}
	for i := 0; i < 4; i++ {
		job, _, err := c.dequeue("work", time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		if acked[job.ID] {
			t.Fatalf("acked job %s came back after a restart", job.ID)
		}
	}
}

// A node on a real disk: what it acknowledged is there after it is closed
// and opened again.
func TestDurableNodeRestartsFromDisk(t *testing.T) {
	dir := t.TempDir()
	open := func() (*Node, *jobq.RaftLog) {
		log, _, err := jobq.OpenRaftLog(dir)
		if err != nil {
			t.Fatal(err)
		}
		n, err := Start(Config{ID: 1, Peers: []raft.NodeID{1}, Storage: log,
			Tick: 2 * time.Millisecond, ElectionTicks: 5, HeartbeatTicks: 1})
		if err != nil {
			t.Fatal(err)
		}
		return n, log
	}
	lead := func(n *Node) {
		eventually(t, "the node to lead", func() bool {
			st := n.Status()
			return st.State == raft.Leader && st.Applied == st.LastIndex
		})
	}

	n, log := open()
	lead(n)
	keep, err := n.Enqueue("work", []byte("keep"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := n.Enqueue("work", []byte("done")); err != nil {
		t.Fatal(err)
	}
	// FIFO: "keep" is leased first and nacked to the back; "done" is acked.
	_, first, err := n.Dequeue("work", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Nack(first); err != nil {
		t.Fatal(err)
	}
	_, second, err := n.Dequeue("work", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Ack(second); err != nil {
		t.Fatal(err)
	}
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}

	n, log = open()
	defer log.Close()
	defer n.Close()
	lead(n)
	if st := n.Stats("work"); st.Enqueued != 2 || st.Acked != 1 || st.Ready != 1 {
		t.Fatalf("after reopening: %+v, want 2 enqueued, 1 acked, 1 ready", st)
	}
	job, _, err := n.Dequeue("work", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if job.ID != keep || string(job.Payload) != "keep" || job.Attempt != 2 {
		t.Fatalf("after reopening got %+v, want the nacked job on attempt 2", job)
	}
}
