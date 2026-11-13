package main

// The cluster crash harness: the R2 proof, and the three-node counterpart
// of the single-node harness in the root package's crash_test.go.
//
// The test re-executes its own binary three times as real jobqd nodes, each
// with its own data directory, talking Raft to each other over loopback
// HTTP. The parent is the client: producers and consumers running against
// the cluster through jobqhttp.Client. While they run, the parent finds the
// leader and kills it cold (Process.Kill: no signal handler, no flush, no
// goodbye), lets the survivors elect, restarts it from its directory, and
// does it again. The last round kills all three at once.
//
// It then asserts the contract, from the client's side of the wire:
//
//	LOSS  (forbidden): an enqueue the client saw acknowledged is never
//	       delivered and completed afterwards.
//	GHOST (forbidden): a job whose ack the client saw acknowledged is
//	       delivered again.
//	DUPES (permitted): counted and reported.
//
// and, from the nodes' side, that all three end up with identical counts
// that add up. Skipped under -short.

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aleks-drozy/jobq"
	"github.com/aleks-drozy/jobq/jobqhttp"
)

const clusterChildEnv = "JOBQD_CRASH_CHILD_ARGS"

func TestMain(m *testing.M) {
	if args := os.Getenv(clusterChildEnv); args != "" {
		// A node. It runs until it is killed; it never exits by choice.
		err := run(context.Background(), strings.Split(args, "\n"), os.Stdout)
		fmt.Fprintln(os.Stdout, "jobqd exited:", err)
		os.Exit(3)
	}
	os.Exit(m.Run())
}

type crashNode struct {
	id         int
	dir        string
	clientURL  string
	peerURL    string
	clientAddr string
	peerAddr   string
	cmd        *exec.Cmd
	out        *lockedBuffer
	// direct talks to this node and no other, for asking it questions.
	direct *jobqhttp.Client
}

type lockedBuffer struct {
	mu sync.Mutex
	b  strings.Builder
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

type crashCluster struct {
	t     *testing.T
	exe   string
	nodes []*crashNode
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

func newCrashCluster(t *testing.T) *crashCluster {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	c := &crashCluster{t: t, exe: exe}
	root := t.TempDir()
	for id := 1; id <= 3; id++ {
		n := &crashNode{id: id, dir: filepath.Join(root, fmt.Sprintf("node%d", id)),
			clientAddr: freeAddr(t), peerAddr: freeAddr(t), out: &lockedBuffer{}}
		n.clientURL, n.peerURL = "http://"+n.clientAddr, "http://"+n.peerAddr
		n.direct = jobqhttp.NewClient(n.clientURL, jobqhttp.WithRetryBudget(0), jobqhttp.WithTimeout(2*time.Second))
		c.nodes = append(c.nodes, n)
	}
	t.Cleanup(func() {
		for _, n := range c.nodes {
			c.kill(n)
		}
		if t.Failed() {
			for _, n := range c.nodes {
				t.Logf("--- node %d output (tail)\n%s", n.id, tail(n.out.String(), 2000))
			}
		}
	})
	return c
}

func tail(s string, n int) string {
	if len(s) > n {
		return s[len(s)-n:]
	}
	return s
}

func (c *crashCluster) start(n *crashNode) {
	c.t.Helper()
	// The previous owner of this directory is provably dead: we killed it
	// and waited for it. Its lock file is all that is left of it.
	if err := os.Remove(filepath.Join(n.dir, "jobq.lock")); err != nil && !os.IsNotExist(err) {
		c.t.Fatal(err)
	}
	var peers, advertise []string
	for _, p := range c.nodes {
		peers = append(peers, fmt.Sprintf("%d=%s", p.id, p.peerURL))
		advertise = append(advertise, fmt.Sprintf("%d=%s", p.id, p.clientURL))
	}
	args := []string{
		"-id", fmt.Sprint(n.id),
		"-dir", n.dir,
		"-addr", n.clientAddr,
		"-peer-addr", n.peerAddr,
		"-peers", strings.Join(peers, ","),
		"-advertise", strings.Join(advertise, ","),
		"-sync", "always",
		// Small, so that the run crosses many snapshots: nodes restart
		// from one, and a killed leader comes back to a cluster that has
		// compacted past what it holds.
		"-snapshot-every", "300",
		"-snapshot-keep", "50",
	}
	cmd := exec.Command(c.exe, "-test.run", "XXX_NONE")
	cmd.Env = append(os.Environ(), clusterChildEnv+"="+strings.Join(args, "\n"))
	cmd.Stdout, cmd.Stderr = n.out, n.out
	if err := cmd.Start(); err != nil {
		c.t.Fatal(err)
	}
	n.cmd = cmd
}

// kill ends a node the way a power cut would.
func (c *crashCluster) kill(n *crashNode) {
	if n.cmd == nil {
		return
	}
	_ = n.cmd.Process.Kill()
	_ = n.cmd.Wait()
	n.cmd = nil
}

func (c *crashCluster) status(n *crashNode) (jobqhttp.ClusterStatus, error) {
	return n.direct.ClusterStatus()
}

// leader waits until one running node leads and every running node agrees.
func (c *crashCluster) leader() *crashNode {
	c.t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		var lead *crashNode
		agree, running := true, 0
		views := map[int]uint64{}
		for _, n := range c.nodes {
			if n.cmd == nil {
				continue
			}
			running++
			st, err := c.status(n)
			if err != nil {
				agree = false
				continue
			}
			views[n.id] = st.Leader
			if st.State == "leader" {
				lead = n
			}
		}
		for _, l := range views {
			if lead == nil || l != uint64(lead.id) {
				agree = false
			}
		}
		if lead != nil && agree && len(views) == running {
			return lead
		}
		if time.Now().After(deadline) {
			c.t.Fatalf("no agreed leader among %d running nodes; views: %v", running, views)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (c *crashCluster) addrs() string {
	var out []string
	for _, n := range c.nodes {
		out = append(out, n.clientURL)
	}
	return strings.Join(out, ",")
}

func TestClusterLosesNothingAcknowledgedAcrossLeaderKills(t *testing.T) {
	if testing.Short() {
		t.Skip("cluster crash harness skipped under -short")
	}
	const (
		topic       = "crash"
		producers   = 3
		consumers   = 3
		leaderKills = 3
	)
	dlq := jobq.DeadLetterTopic(topic)
	seed := time.Now().UnixNano()
	rng := rand.New(rand.NewSource(seed))
	t.Logf("seed %d", seed)

	c := newCrashCluster(t)
	for _, n := range c.nodes {
		c.start(n)
	}
	c.leader()

	// One client shared by every producer and consumer, as a real worker
	// pool would share one.
	cl := jobqhttp.NewClient(c.addrs(), jobqhttp.WithRetryBudget(20*time.Second), jobqhttp.WithTimeout(8*time.Second))

	var (
		mu         sync.Mutex
		enqueued   = map[string]string{} // job ID -> payload, acknowledged enqueues only
		completed  = map[string]bool{}   // job IDs whose ack was acknowledged
		maybeAcked = map[string]bool{}   // job IDs with an ack whose outcome is unknown
		delivered  = map[string]int{}    // job ID -> deliveries
		ghosts     []string
		ambiguous  atomic.Int64 // enqueues that failed without a clean refusal
	)
	stopProducing := make(chan struct{})
	stopConsuming := make(chan struct{})
	var producing, consuming sync.WaitGroup

	for p := 0; p < producers; p++ {
		producing.Add(1)
		go func(p int) {
			defer producing.Done()
			for i := 0; ; i++ {
				select {
				case <-stopProducing:
					return
				default:
				}
				payload := fmt.Sprintf("p%d-%d", p, i)
				id, err := cl.Enqueue(topic, []byte(payload), jobq.WithMaxAttempts(4))
				if err != nil {
					// Not acknowledged, so not owed. It may exist anyway.
					ambiguous.Add(1)
					continue
				}
				// Only now is the job acknowledged, and so owed.
				mu.Lock()
				enqueued[id] = payload
				mu.Unlock()
				time.Sleep(2 * time.Millisecond)
			}
		}(p)
	}

	consume := func(from string) {
		defer consuming.Done()
		for {
			select {
			case <-stopConsuming:
				return
			default:
			}
			job, lease, err := cl.Dequeue(from, 30*time.Second)
			if err != nil {
				time.Sleep(5 * time.Millisecond)
				continue
			}
			mu.Lock()
			delivered[job.ID]++
			if completed[job.ID] {
				ghosts = append(ghosts, job.ID)
			}
			mu.Unlock()

			time.Sleep(3 * time.Millisecond) // the work, during which a leader may die
			err = cl.Ack(lease)
			mu.Lock()
			if err == nil {
				// Acknowledged: this job must never be seen again.
				completed[job.ID] = true
			} else {
				// The lease died with a leader, or the ack landed and its
				// reply was lost. Either the job comes round again or it
				// is already settled; the node-side counts below decide.
				maybeAcked[job.ID] = true
			}
			mu.Unlock()
		}
	}
	for i := 0; i < consumers; i++ {
		consuming.Add(2)
		go consume(topic)
		go consume(dlq)
	}

	// Kill the leader, cold, while work is in every state.
	for k := 0; k < leaderKills; k++ {
		time.Sleep(time.Duration(300+rng.Intn(500)) * time.Millisecond)
		lead := c.leader()
		t.Logf("kill %d: node %d (leader)", k+1, lead.id)
		c.kill(lead)
		time.Sleep(time.Duration(300+rng.Intn(300)) * time.Millisecond)
		c.start(lead)
		c.leader()
	}
	// Then all three at once: nothing is left running to remember anything.
	// Whatever was acknowledged has to come back from the three disks.
	time.Sleep(time.Duration(300+rng.Intn(300)) * time.Millisecond)
	t.Logf("kill %d: all three nodes", leaderKills+1)
	for _, n := range c.nodes {
		c.kill(n)
	}
	for _, n := range c.nodes {
		c.start(n)
	}
	c.leader()
	time.Sleep(300 * time.Millisecond)
	close(stopProducing)
	producing.Wait()

	// Drain: every node reports nothing ready and nothing in flight, and
	// they all report the same thing, several times running.
	type counts struct{ main, dead jobq.Stats }
	read := func(n *crashNode) (counts, error) {
		m, err := n.direct.TopicStats(topic)
		if err != nil {
			return counts{}, err
		}
		d, err := n.direct.TopicStats(dlq)
		return counts{m, d}, err
	}
	var final counts
	quiet := 0
	deadline := time.Now().Add(60 * time.Second)
	for quiet < 15 {
		if time.Now().After(deadline) {
			t.Fatalf("queues never drained; last counts %+v", final)
		}
		time.Sleep(20 * time.Millisecond)
		first, err := read(c.nodes[0])
		ok := err == nil && first.main.Ready == 0 && first.main.InFlight == 0 &&
			first.dead.Ready == 0 && first.dead.InFlight == 0
		for _, n := range c.nodes[1:] {
			other, err := read(n)
			ok = ok && err == nil && other == first
		}
		final = first
		if ok {
			quiet++
		} else {
			quiet = 0
		}
	}
	close(stopConsuming)
	consuming.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(enqueued) < 50 {
		t.Fatalf("only %d enqueues were acknowledged: the run proves nothing", len(enqueued))
	}
	for _, id := range ghosts {
		t.Errorf("GHOST: job %s (%s) was acked and then delivered again", id, enqueued[id])
	}
	lost, unconfirmed := 0, 0
	for id, payload := range enqueued {
		switch {
		case completed[id]:
		case maybeAcked[id] && delivered[id] > 0:
			// Delivered after it was acknowledged, so it was not lost;
			// then settled by an ack whose reply never arrived. The
			// node-side counts below confirm nothing is still owed.
			unconfirmed++
		default:
			lost++
			t.Errorf("LOST: acknowledged enqueue %s (%s) was never delivered and completed", id, payload)
		}
	}

	main, dead := final.main, final.dead
	if main.Enqueued != main.Acked+main.DeadLetters {
		t.Errorf("%s: Enqueued %d != Acked %d + DeadLetters %d", topic, main.Enqueued, main.Acked, main.DeadLetters)
	}
	if dead.Enqueued != main.DeadLetters || dead.Enqueued != dead.Acked+dead.Dropped {
		t.Errorf("%s: Enqueued %d, Acked %d, Dropped %d; %s dead-lettered %d",
			dlq, dead.Enqueued, dead.Acked, dead.Dropped, topic, main.DeadLetters)
	}
	// Every acknowledged enqueue exists on the nodes; they may hold more
	// (an enqueue that landed but whose reply was lost), never fewer.
	if main.Enqueued < len(enqueued) {
		t.Errorf("nodes hold %d enqueues but %d were acknowledged", main.Enqueued, len(enqueued))
	}
	// And no more than one per call that failed. The client retries an
	// enqueue under one request ID, so a retry that follows a lost reply
	// must not add a second job; only a call that gave up without ever
	// hearing back can leave a job the client does not know about.
	if extra := int64(main.Enqueued - len(enqueued)); extra > ambiguous.Load() {
		t.Errorf("nodes hold %d enqueues nobody was told about, but only %d calls failed: a retry enqueued twice",
			extra, ambiguous.Load())
	}
	// Every job the nodes consider finished was acked or dropped for
	// running out of attempts; none simply vanished. And the nodes
	// recorded at least every ack the client saw acknowledged.
	if acked := main.Acked + dead.Acked; acked < len(completed) {
		t.Errorf("nodes recorded %d acks but %d were acknowledged", acked, len(completed))
	}
	if dead.Dropped != 0 {
		t.Errorf("%d jobs were dropped after exhausting their attempts twice over", dead.Dropped)
	}

	redelivered := 0
	for _, n := range delivered {
		if n > 1 {
			redelivered++
		}
	}
	st, err := c.status(c.leader())
	if err != nil {
		t.Fatal(err)
	}
	if st.Term < leaderKills+1 {
		t.Errorf("final term %d: the cluster did not go through %d changes of leader", st.Term, leaderKills+1)
	}
	if redelivered == 0 {
		t.Errorf("no job was delivered twice: no kill caught a job in flight")
	}
	// Every restart after the first snapshot starts from one, and the logs
	// say so. Count them, and the snapshots taken, from what the nodes
	// themselves printed.
	taken, loaded := 0, 0
	for _, n := range c.nodes {
		out := n.out.String()
		taken += strings.Count(out, " snapshotted at index ")
		loaded += strings.Count(out, " loaded a snapshot at index ")
	}
	if taken < 3 || loaded < 3 {
		t.Errorf("nodes took %d snapshots and loaded %d: the run did not exercise restarting from one", taken, loaded)
	}
	t.Logf("snapshots: %d taken, %d loaded", taken, loaded)
	if st.SnapshotIndex == 0 || st.FirstIndex <= 1 {
		t.Errorf("the leader never compacted its log (snapshot at %d, log from %d): the run did not cross a snapshot",
			st.SnapshotIndex, st.FirstIndex)
	}
	verdict := "contract held"
	if t.Failed() {
		verdict = "CONTRACT BROKEN"
	}
	t.Logf(verdict+": %d acknowledged enqueues, %d lost, %d ghosts; %d completed, %d settled by an "+
		"unconfirmed ack; %d jobs redelivered, %d dead-lettered; nodes hold %d enqueues (%d the client "+
		"never saw acknowledged, of %d failed calls); final term %d, log index %d, snapshot at %d, log kept from %d",
		len(enqueued), lost, len(ghosts), len(completed), unconfirmed, redelivered, main.DeadLetters,
		main.Enqueued, main.Enqueued-len(enqueued), ambiguous.Load(), st.Term, st.LastIndex, st.SnapshotIndex, st.FirstIndex)
}
