package jobqhttp

import (
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aleks-drozy/jobq"
	"github.com/aleks-drozy/jobq/cluster"
	"github.com/aleks-drozy/jobq/raft"
)

// httpCluster is three replicated nodes, each with a client-facing server
// and a peer-facing server on loopback, the way jobqd runs them.
type httpCluster struct {
	t       *testing.T
	ids     []raft.NodeID
	nodes   map[raft.NodeID]*cluster.Node
	clients map[raft.NodeID]*httptest.Server
	peers   map[raft.NodeID]*httptest.Server
	trans   map[raft.NodeID]*cluster.HTTPTransport
	down    map[raft.NodeID]bool
}

// lateHandler is a server whose handler is filled in after it is listening,
// because every node needs every other node's URL before it can start.
type lateHandler struct{ h atomic.Pointer[http.Handler] }

func (l *lateHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h := l.h.Load(); h != nil {
		(*h).ServeHTTP(w, r)
		return
	}
	http.Error(w, "starting", http.StatusServiceUnavailable)
}

func newHTTPCluster(t *testing.T) *httpCluster {
	t.Helper()
	c := &httpCluster{
		t:       t,
		ids:     []raft.NodeID{1, 2, 3},
		nodes:   map[raft.NodeID]*cluster.Node{},
		clients: map[raft.NodeID]*httptest.Server{},
		peers:   map[raft.NodeID]*httptest.Server{},
		trans:   map[raft.NodeID]*cluster.HTTPTransport{},
		down:    map[raft.NodeID]bool{},
	}
	clientH := map[raft.NodeID]*lateHandler{}
	peerH := map[raft.NodeID]*lateHandler{}
	peerURLs := map[raft.NodeID]string{}
	clientURLs := map[raft.NodeID]string{}
	for _, id := range c.ids {
		clientH[id], peerH[id] = &lateHandler{}, &lateHandler{}
		c.clients[id] = httptest.NewServer(clientH[id])
		c.peers[id] = httptest.NewServer(peerH[id])
		clientURLs[id] = c.clients[id].URL
		peerURLs[id] = c.peers[id].URL
	}
	for _, id := range c.ids {
		c.trans[id] = cluster.NewHTTPTransport(id, peerURLs)
		n, err := cluster.Start(cluster.Config{
			ID: id, Peers: c.ids, Storage: raft.NewMemoryStorage(), Transport: c.trans[id],
			Tick: 10 * time.Millisecond, ElectionTicks: 10, HeartbeatTicks: 2,
			ClientAddr: func(id raft.NodeID) string { return clientURLs[id] },
		})
		if err != nil {
			t.Fatal(err)
		}
		c.nodes[id] = n
		ph, ch := cluster.PeerHandler(n), NewHandler(n)
		peerH[id].h.Store(&ph)
		clientH[id].h.Store(&ch)
	}
	t.Cleanup(func() {
		for _, id := range c.ids {
			c.kill(id)
		}
	})
	return c
}

// kill takes a node away entirely: both listeners and the node itself.
func (c *httpCluster) kill(id raft.NodeID) {
	if c.down[id] {
		return
	}
	c.down[id] = true
	c.clients[id].CloseClientConnections()
	c.clients[id].Close()
	c.peers[id].Close()
	_ = c.nodes[id].Close()
	c.trans[id].Close()
}

func (c *httpCluster) leader() raft.NodeID {
	c.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		var lead raft.NodeID
		agree := true
		for _, id := range c.ids {
			if !c.down[id] && c.nodes[id].Status().State == raft.Leader {
				lead = id
			}
		}
		for _, id := range c.ids {
			if !c.down[id] && c.nodes[id].Status().Leader != lead {
				agree = false
			}
		}
		if lead != raft.None && agree {
			return lead
		}
		if time.Now().After(deadline) {
			c.t.Fatal("no leader")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (c *httpCluster) allAddrs() string {
	var addrs []string
	for _, id := range c.ids {
		addrs = append(addrs, c.clients[id].URL)
	}
	return strings.Join(addrs, ",")
}

func (c *httpCluster) follower() raft.NodeID {
	lead := c.leader()
	for _, id := range c.ids {
		if id != lead && !c.down[id] {
			return id
		}
	}
	c.t.Fatal("no follower")
	return 0
}

// A follower answers a write with 421, the not_leader code and the
// leader's address. Nothing is applied.
func TestFollowerAnswersWithLeaderHint(t *testing.T) {
	c := newHTTPCluster(t)
	lead := c.leader()
	f := c.follower()

	resp, err := http.Post(c.clients[f].URL+"/v1/topics/work/jobs", "text/plain", strings.NewReader("x"))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMisdirectedRequest {
		t.Fatalf("status %d, want 421", resp.StatusCode)
	}
	var env errorEnvelope
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		t.Fatal(err)
	}
	if env.Error.Code != "not_leader" || env.Error.Leader != c.clients[lead].URL {
		t.Fatalf("envelope %+v, want not_leader naming %s", env.Error, c.clients[lead].URL)
	}
	if st := c.nodes[lead].Stats("work"); st.Enqueued != 0 {
		t.Fatalf("a refused enqueue was applied: %+v", st)
	}

	// With retries off, the client surfaces exactly that refusal.
	one := NewClient(c.clients[f].URL, WithRetryBudget(0))
	_, err = one.Enqueue("work", []byte("x"))
	var nl *jobq.NotLeaderError
	if !errors.As(err, &nl) || nl.Leader != c.clients[lead].URL || !errors.Is(err, jobq.ErrNotLeader) {
		t.Fatalf("client error %v, want a NotLeaderError naming the leader", err)
	}
}

// Pointed first at a follower, the client follows the hint and the caller
// never learns it was not talking to the leader.
func TestClientFollowsHintFromAFollower(t *testing.T) {
	c := newHTTPCluster(t)
	cl := NewClient(c.clients[c.follower()].URL + "," + c.allAddrs())

	id, err := cl.Enqueue("work", []byte("hello"))
	if err != nil {
		t.Fatalf("enqueue through a follower's address: %v", err)
	}
	job, lease, err := cl.Dequeue("work", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if job.ID != id || string(job.Payload) != "hello" {
		t.Fatalf("got %+v", job)
	}
	if _, err := cl.Extend(lease, time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := cl.Ack(lease); err != nil {
		t.Fatal(err)
	}
	if err := cl.Ack(lease); !errors.Is(err, jobq.ErrUnknownLease) {
		t.Fatalf("second ack: %v, want ErrUnknownLease", err)
	}
	if _, _, err := cl.Dequeue("work", time.Minute); !errors.Is(err, jobq.ErrNoJob) {
		t.Fatalf("dequeue from an empty topic: %v, want ErrNoJob", err)
	}
}

// The client carries its caller across the loss of the leader: work
// acknowledged before is still there after, and new work is accepted.
func TestClientSurvivesLosingTheLeader(t *testing.T) {
	c := newHTTPCluster(t)
	cl := NewClient(c.allAddrs())
	c.leader()

	before := map[string]bool{}
	for i := 0; i < 10; i++ {
		id, err := cl.Enqueue("work", []byte("before"))
		if err != nil {
			t.Fatal(err)
		}
		before[id] = true
	}
	job, lease, err := cl.Dequeue("work", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := cl.Ack(lease); err != nil {
		t.Fatal(err)
	}
	delete(before, job.ID)
	acked := job.ID

	c.kill(c.leader())

	after, err := cl.Enqueue("work", []byte("after"))
	if err != nil {
		t.Fatalf("enqueue after the leader was lost: %v", err)
	}
	before[after] = true
	for len(before) > 0 {
		job, lease, err := cl.Dequeue("work", time.Minute)
		if err != nil {
			t.Fatalf("%d acknowledged jobs missing after failover: %v", len(before), err)
		}
		if job.ID == acked {
			t.Fatal("the acked job came back after failover")
		}
		if !before[job.ID] {
			t.Fatalf("unexpected job %s", job.ID)
		}
		delete(before, job.ID)
		if err := cl.Ack(lease); err != nil {
			t.Fatal(err)
		}
	}
}

func TestClusterStatusEndpoint(t *testing.T) {
	c := newHTTPCluster(t)
	lead := c.leader()
	leaders := 0
	for _, id := range c.ids {
		st, err := NewClient(c.clients[id].URL).ClusterStatus()
		if err != nil {
			t.Fatal(err)
		}
		if st.ID != uint64(id) || st.Leader != uint64(lead) || st.LeaderAddr != c.clients[lead].URL {
			t.Errorf("node %d reports %+v, want leader %d at %s", id, st, lead, c.clients[lead].URL)
		}
		if st.State == "leader" {
			leaders++
		}
	}
	if leaders != 1 {
		t.Errorf("%d nodes report themselves leader, want 1", leaders)
	}

	// A plain queue is not a cluster and says so.
	q := jobq.New()
	defer q.Close()
	srv := httptest.NewServer(NewHandler(q))
	defer srv.Close()
	_, err := NewClient(srv.URL).ClusterStatus()
	var api *apiError
	if !errors.As(err, &api) || api.Code != "not_clustered" || api.Status != http.StatusNotFound {
		t.Fatalf("single-node /v1/cluster: %v, want 404 not_clustered", err)
	}
}

// Against one plain server the client must behave as it always has: one
// request, and a failure to connect reported at once rather than retried
// for ten seconds.
func TestSingleServerClientDoesNotRetry(t *testing.T) {
	srv := httptest.NewServer(NewHandler(jobq.New()))
	url := srv.URL
	srv.Close()

	start := time.Now()
	_, err := NewClient(url).Enqueue("work", []byte("x"))
	if err == nil {
		t.Fatal("enqueue to a closed server succeeded")
	}
	if took := time.Since(start); took > 3*time.Second {
		t.Fatalf("single-server client spent %v retrying", took)
	}
}

// With no leader anywhere the client backs off between tries and gives up
// once the next wait would overrun its budget, having tried every address.
func TestClientGivesUpWhenNoNodeAnswers(t *testing.T) {
	var hits atomic.Int64
	dead := func() string {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hits.Add(1)
			writeEngineError(w, &jobq.NotLeaderError{})
		}))
		t.Cleanup(srv.Close)
		return srv.URL
	}
	cl := NewClient(dead()+","+dead(), WithRetryBudget(300*time.Millisecond))
	start := time.Now()
	_, err := cl.Enqueue("work", []byte("x"))
	if !errors.Is(err, jobq.ErrNotLeader) {
		t.Fatalf("err = %v, want ErrNotLeader", err)
	}
	if took := time.Since(start); took < 100*time.Millisecond || took > 5*time.Second {
		t.Errorf("gave up after %v, want within the 300ms budget but not at once", took)
	}
	if n := hits.Load(); n < 3 || n > 8 {
		t.Errorf("%d requests made across two leaderless nodes in 300ms, want a handful: "+
			"more than one try, but backing off", n)
	}
}

// A pool of workers sharing one Client must share its connections too.
// With Go's default of two idle connections per host, sixteen workers
// would open a fresh connection for most requests; this pins that they
// open about one each, however many requests they make.
func TestClientReusesConnections(t *testing.T) {
	q := jobq.New()
	defer q.Close()
	var opened atomic.Int64
	srv := httptest.NewUnstartedServer(NewHandler(q))
	srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			opened.Add(1)
		}
	}
	srv.Start()
	defer srv.Close()

	const workers, each = 16, 60
	cl := NewClient(srv.URL)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < each; i++ {
				if _, err := cl.Enqueue("work", []byte("x")); err != nil {
					t.Errorf("enqueue: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	if got := opened.Load(); got > 2*workers {
		t.Fatalf("%d requests from %d workers opened %d connections; they are not being reused",
			workers*each, workers, got)
	}
}

// A hint is followed only to an address the caller configured. A server
// that names somewhere else, by mistake or on purpose, must not be able to
// send the caller's payloads there.
func TestClientIgnoresHintToUnconfiguredAddress(t *testing.T) {
	var strayHits atomic.Int64
	stray := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		strayHits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer stray.Close()
	liar := func() *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			writeEngineError(w, &jobq.NotLeaderError{Leader: stray.URL})
		}))
	}
	a, b := liar(), liar()
	defer a.Close()
	defer b.Close()

	// One address: the refusal comes straight back, naming the leader.
	_, err := NewClient(a.URL).Enqueue("work", []byte("secret"))
	var nl *jobq.NotLeaderError
	if !errors.As(err, &nl) || nl.Leader != stray.URL {
		t.Fatalf("single address: err = %v, want a NotLeaderError naming %s", err, stray.URL)
	}
	// Several: the client rotates among its own until the budget runs out.
	_, err = NewClient(a.URL+","+b.URL, WithRetryBudget(200*time.Millisecond)).Enqueue("work", []byte("secret"))
	if !errors.Is(err, jobq.ErrNotLeader) {
		t.Fatalf("two addresses: err = %v, want ErrNotLeader", err)
	}
	if n := strayHits.Load(); n != 0 {
		t.Fatalf("%d requests reached an address the client was never given", n)
	}
}
