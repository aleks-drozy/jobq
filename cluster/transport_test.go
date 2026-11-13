package cluster

import (
	"bytes"
	"errors"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aleks-drozy/jobq/raft"
)

func TestCommandRoundTrip(t *testing.T) {
	at := time.Unix(1_800_000_000, 123456789).UTC()
	cases := []command{
		{At: at, Op: opEnqueue, Topic: "work", Payload: []byte("payload"), Delay: 3 * time.Second, MaxAttempts: 7},
		{At: at, Op: opEnqueue, Topic: "empty payload"},
		{At: at, Op: opDequeue, Topic: "work", Duration: 30 * time.Second},
		{At: at, Op: opAck, Topic: "work", LeaseID: "00000000000000010000000100000001"},
		{At: at, Op: opNack, Topic: "work.dlq", LeaseID: "x"},
		{At: at, Op: opExtend, Topic: "work", LeaseID: "lease", Duration: time.Minute},
	}
	for _, want := range cases {
		got, err := decodeCommand(want.encode())
		if err != nil {
			t.Errorf("%+v: %v", want, err)
			continue
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("round trip changed the command:\n got %+v\nwant %+v", got, want)
		}
	}
}

func TestMessageRoundTrip(t *testing.T) {
	cases := []raft.Message{
		{Type: raft.MsgVote, From: 1, To: 2, Term: 9, LogIndex: 40, LogTerm: 8},
		{Type: raft.MsgVoteResp, From: 2, To: 1, Term: 9, Reject: true},
		{Type: raft.MsgAppend, From: 1, To: 3, Term: 9, LogIndex: 40, LogTerm: 8, Commit: 39, Entries: []raft.Entry{
			{Index: 41, Term: 9, Kind: raft.EntryNoop},
			{Index: 42, Term: 9, Data: []byte("a command")},
		}},
		{Type: raft.MsgAppendResp, From: 3, To: 1, Term: 9, LogIndex: 40, Reject: true, Hint: 12},
		{Type: raft.MsgSnapshot, From: 1, To: 3, Term: 9, Snapshot: &raft.Snapshot{Index: 40, Term: 8, Data: []byte("the whole queue")}},
		// A message after the snapshot shows its length was honoured.
		{Type: raft.MsgAppend, From: 1, To: 3, Term: 9, LogIndex: 40, LogTerm: 8},
	}
	var batch []byte
	for _, m := range cases {
		batch = appendFrame(batch, m)
	}
	got, err := decodeFrames(batch)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, cases) {
		t.Fatalf("round trip changed the batch:\n got %+v\nwant %+v", got, cases)
	}
}

// The peer endpoint and the log both decode bytes this process did not
// write. Whatever arrives, decoding must return an error or a value, never
// panic and never allocate what a length field merely claims.
func TestDecodersSurviveGarbage(t *testing.T) {
	good := appendFrame(nil, raft.Message{Type: raft.MsgAppend, From: 1, To: 2, Term: 3,
		Entries: []raft.Entry{{Index: 1, Term: 3, Data: bytes.Repeat([]byte("x"), 40)}}})
	cmd := command{At: time.Unix(1, 0), Op: opEnqueue, Topic: "t", Payload: []byte("payload")}.encode()

	for cut := 0; cut < len(good); cut++ {
		if _, err := decodeFrames(good[:cut]); cut > 0 && err == nil {
			t.Errorf("a batch cut to %d of %d bytes decoded without error", cut, len(good))
		}
	}
	for cut := 0; cut < len(cmd); cut++ {
		if _, err := decodeCommand(cmd[:cut]); err == nil {
			t.Errorf("a command cut to %d of %d bytes decoded without error", cut, len(cmd))
		}
	}
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 20000; i++ {
		b := make([]byte, rng.Intn(96))
		rng.Read(b)
		_, _ = decodeFrames(b)
		_, _ = decodeCommand(b)
		// And valid input with a few bytes flipped, which gets much
		// further into the decoder than noise does.
		m := append([]byte(nil), good...)
		for k := 0; k < 1+rng.Intn(3); k++ {
			m[rng.Intn(len(m))] = byte(rng.Intn(256))
		}
		_, _ = decodeFrames(m)
	}
	// An entry count far larger than the bytes that follow.
	huge := []byte{byte(raft.MsgAppend)}
	huge = append(huge, make([]byte, 7*8+1)...)
	huge = append(huge, 0xff, 0xff, 0xff, 0xff, 0x0f)
	if _, err := decodeMessage(huge); !errors.Is(err, errBadMessage) {
		t.Errorf("a message claiming 4 billion entries: %v, want errBadMessage", err)
	}
}

// Three nodes talking over real HTTP on loopback: they elect a leader,
// replicate, and survive the leader's listener going away.
func TestClusterOverHTTP(t *testing.T) {
	ids := []raft.NodeID{1, 2, 3}
	handlers := map[raft.NodeID]*atomic.Pointer[http.Handler]{}
	servers := map[raft.NodeID]*httptest.Server{}
	urls := map[raft.NodeID]string{}
	for _, id := range ids {
		h := &atomic.Pointer[http.Handler]{}
		handlers[id] = h
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if cur := h.Load(); cur != nil {
				(*cur).ServeHTTP(w, r)
				return
			}
			http.Error(w, "not started", http.StatusServiceUnavailable)
		}))
		servers[id] = srv
		urls[id] = srv.URL
		t.Cleanup(srv.Close)
	}

	nodes := map[raft.NodeID]*Node{}
	for _, id := range ids {
		tr := NewHTTPTransport(id, urls)
		n, err := Start(Config{
			ID: id, Peers: ids, Storage: raft.NewMemoryStorage(), Transport: tr,
			Tick: 10 * time.Millisecond, ElectionTicks: 10, HeartbeatTicks: 2,
		})
		if err != nil {
			t.Fatal(err)
		}
		nodes[id] = n
		ph := PeerHandler(n)
		handlers[id].Store(&ph)
		t.Cleanup(func() {
			_ = n.Close()
			tr.Close()
		})
	}

	leaderOf := func(among ...raft.NodeID) *Node {
		var lead *Node
		eventually(t, "a leader over HTTP", func() bool {
			lead = nil
			for _, id := range among {
				if nodes[id].Status().State == raft.Leader {
					lead = nodes[id]
				}
			}
			if lead == nil {
				return false
			}
			for _, id := range among {
				if nodes[id].Status().Leader != lead.cfg.ID {
					return false
				}
			}
			return true
		})
		return lead
	}

	lead := leaderOf(ids...)
	id, err := lead.Enqueue("work", []byte("over the wire"))
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, "every node to hold the job", func() bool {
		for _, n := range nodes {
			if n.Stats("work").Ready != 1 {
				return false
			}
		}
		return true
	})

	// Take the leader off the network. The other two must carry on.
	servers[lead.cfg.ID].Close()
	_ = lead.Close()
	var rest []raft.NodeID
	for _, nid := range ids {
		if nid != lead.cfg.ID {
			rest = append(rest, nid)
		}
	}
	next := leaderOf(rest...)
	job, lease, err := next.Dequeue("work", time.Minute)
	if err != nil {
		t.Fatalf("new leader could not deliver the replicated job: %v", err)
	}
	if job.ID != id || string(job.Payload) != "over the wire" {
		t.Fatalf("new leader delivered %+v", job)
	}
	if err := next.Ack(lease); err != nil {
		t.Fatal(err)
	}
}

// A request for someone else, or one that is not a batch at all, must not
// reach the node.
func TestPeerHandlerRejectsWhatIsNotForIt(t *testing.T) {
	n, err := Start(Config{ID: 1, Peers: []raft.NodeID{1}, Storage: raft.NewMemoryStorage()})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	srv := httptest.NewServer(PeerHandler(n))
	defer srv.Close()

	post := func(body []byte) int {
		resp, err := http.Post(srv.URL+PeerPath, "application/octet-stream", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	if got := post([]byte("not a batch")); got != http.StatusBadRequest {
		t.Errorf("garbage body: status %d, want 400", got)
	}
	// A well-formed vote request from a node outside the cluster: accepted
	// by the handler, ignored by Raft. The node must not change term.
	stranger := appendFrame(nil, raft.Message{Type: raft.MsgVote, From: 99, To: 1, Term: 1000})
	if got := post(stranger); got != http.StatusNoContent {
		t.Errorf("stranger's vote request: status %d, want 204", got)
	}
	eventually(t, "the node to lead", func() bool { return n.Status().State == raft.Leader })
	if _, err := n.Enqueue("work", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if term := n.Status().Term; term >= 1000 {
		t.Errorf("a message from outside the cluster moved the term to %d", term)
	}
	resp, err := http.Get(srv.URL + PeerPath)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("GET on the peer path: status %d, want 405", resp.StatusCode)
	}
}
