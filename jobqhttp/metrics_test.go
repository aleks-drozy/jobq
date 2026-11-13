package jobqhttp

import (
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aleks-drozy/jobq"
)

var metricLine = regexp.MustCompile(`^([a-z_][a-z0-9_]*)(\{.*\})? (\S+)$`)

// scrape fetches /metrics and returns every sample keyed by its name and
// labels exactly as written, failing on any line that is not valid
// exposition format.
func scrape(t *testing.T, base string) map[string]float64 {
	t.Helper()
	resp, err := http.Get(base + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/plain; version=0.0.4") {
		t.Fatalf("GET /metrics: %d %q", resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	body, _ := io.ReadAll(resp.Body)
	out := map[string]float64{}
	typed := map[string]bool{}
	for _, line := range strings.Split(strings.TrimRight(string(body), "\n"), "\n") {
		if strings.HasPrefix(line, "# TYPE ") {
			typed[strings.Fields(line)[2]] = true
			continue
		}
		if strings.HasPrefix(line, "# HELP ") {
			continue
		}
		m := metricLine.FindStringSubmatch(line)
		if m == nil {
			t.Fatalf("not a valid sample line: %q", line)
		}
		v, err := strconv.ParseFloat(m[3], 64)
		if err != nil {
			t.Fatalf("%q: value is not a number", line)
		}
		family := m[1]
		for _, suffix := range []string{"_bucket", "_sum", "_count"} {
			if base := strings.TrimSuffix(family, suffix); base != family && typed[base] {
				family = base
			}
		}
		if !typed[family] {
			t.Fatalf("sample %q has no TYPE line before it", line)
		}
		if _, dup := out[m[1]+m[2]]; dup {
			t.Fatalf("sample %q appears twice", m[1]+m[2])
		}
		out[m[1]+m[2]] = v
	}
	return out
}

func TestMetricsOnASingleNode(t *testing.T) {
	q := jobq.New()
	defer q.Close()
	srv := httptest.NewServer(NewHandler(q))
	defer srv.Close()
	// A topic name that needs escaping must not break the format.
	odd := "we\"ird\\topic"
	for _, topic := range []string{"work", "work", odd} {
		if _, err := q.Enqueue(topic, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	_, lease, _ := q.Dequeue("work", time.Minute)
	_ = q.Ack(lease)
	_, _, _ = q.Dequeue("work", time.Minute)

	m := scrape(t, srv.URL)
	for name, want := range map[string]float64{
		`jobq_topic_enqueued_total{topic="work"}`:  2,
		`jobq_topic_acked_total{topic="work"}`:     1,
		`jobq_topic_in_flight{topic="work"}`:       1,
		`jobq_topic_ready{topic="work"}`:           0,
		`jobq_topic_ready{topic="we\"ird\\topic"}`: 1,
	} {
		if got, ok := m[name]; !ok || got != want {
			t.Errorf("%s = %v (present %v), want %v", name, got, ok, want)
		}
	}
	for name := range m {
		if strings.HasPrefix(name, "jobq_raft_") || strings.HasPrefix(name, "jobq_snapshot") {
			t.Errorf("a single node reported the cluster metric %s", name)
		}
	}
}

func TestMetricsOnACluster(t *testing.T) {
	c := newHTTPCluster(t)
	lead := c.leader()
	cl := NewClient(c.allAddrs())
	for i := 0; i < 20; i++ {
		if _, err := cl.Enqueue("work", []byte("x")); err != nil {
			t.Fatal(err)
		}
	}

	m := scrape(t, c.clients[lead].URL)
	if m["jobq_raft_is_leader"] != 1 || m["jobq_raft_has_leader"] != 1 || m["jobq_raft_leader_id"] != float64(lead) {
		t.Fatalf("leader does not report itself as leader: %v", m)
	}
	if m["jobq_raft_term"] < 1 || m["jobq_raft_leader_changes_total"] < 1 || m["jobq_raft_last_index"] < 21 {
		t.Fatalf("term %v, leader changes %v, last index %v", m["jobq_raft_term"], m["jobq_raft_leader_changes_total"], m["jobq_raft_last_index"])
	}
	if m[`jobq_topic_enqueued_total{topic="work"}`] != 20 {
		t.Fatalf("enqueued = %v, want 20", m[`jobq_topic_enqueued_total{topic="work"}`])
	}
	// One lag series per follower, and none for the leader itself.
	lags := 0
	for name := range m {
		if strings.HasPrefix(name, "jobq_raft_follower_lag_entries{") {
			lags++
			if strings.Contains(name, `peer="`+strconv.Itoa(int(lead))+`"`) {
				t.Errorf("the leader reports lag for itself: %s", name)
			}
		}
	}
	if lags != 2 {
		t.Fatalf("%d follower lag series, want 2", lags)
	}
	// The histogram saw every enqueue, and its buckets only ever grow.
	const hist = "jobq_raft_commit_duration_seconds"
	if m[hist+"_count"] != 20 || m[hist+`_bucket{le="+Inf"}`] != 20 || m[hist+"_sum"] <= 0 {
		t.Fatalf("commit histogram: count %v, +Inf %v, sum %v", m[hist+"_count"], m[hist+`_bucket{le="+Inf"}`], m[hist+"_sum"])
	}
	prev := 0.0
	for _, le := range []string{"0.0005", "0.001", "0.002", "0.005", "0.01", "0.025", "0.05", "0.1", "0.25", "0.5", "1", "5"} {
		v, ok := m[hist+`_bucket{le="`+le+`"}`]
		if !ok || v < prev {
			t.Fatalf("bucket le=%s is %v (present %v) after %v", le, v, ok, prev)
		}
		prev = v
	}

	// A follower knows who leads, does not claim to, and reports no lag.
	follower := c.follower()
	fm := scrape(t, c.clients[follower].URL)
	if fm["jobq_raft_is_leader"] != 0 || fm["jobq_raft_has_leader"] != 1 || fm["jobq_raft_leader_id"] != float64(lead) {
		t.Fatalf("follower: is_leader %v has_leader %v leader_id %v", fm["jobq_raft_is_leader"], fm["jobq_raft_has_leader"], fm["jobq_raft_leader_id"])
	}
	for name := range fm {
		if strings.HasPrefix(name, "jobq_raft_follower_lag_entries{") {
			t.Errorf("a follower reports replication lag: %s", name)
		}
	}
}

// The case request IDs exist for. The leader applies an enqueue and the
// reply never reaches the client, which cannot tell that from the request
// never arriving, so it tries again elsewhere. One job must result.
func TestClientRetryOfALostReplyDoesNotEnqueueTwice(t *testing.T) {
	c := newHTTPCluster(t)
	lead := c.leader()
	real := NewHandler(c.nodes[lead])
	// Counted from the server's goroutine and read from the test's.
	var lost atomic.Int32
	flaky := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Let the node do the work, then cut the connection before a
		// byte of the answer goes back.
		rec := httptest.NewRecorder()
		real.ServeHTTP(rec, r)
		if rec.Code != http.StatusCreated {
			t.Errorf("the enqueue behind the lost reply got %d", rec.Code)
		}
		lost.Add(1)
		conn, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			conn.Close()
		}
	}))
	defer flaky.Close()

	cl := NewClient(flaky.URL + "," + c.clients[lead].URL)
	id, err := cl.Enqueue("work", []byte("pay-once"))
	if err != nil {
		t.Fatalf("enqueue across a lost reply: %v", err)
	}
	if n := lost.Load(); n != 1 {
		t.Fatalf("%d replies were lost, want exactly 1", n)
	}
	st := c.nodes[lead].Stats("work")
	if st.Enqueued != 1 {
		t.Fatalf("%d jobs were enqueued, want 1", st.Enqueued)
	}
	job, _, err := cl.Dequeue("work", time.Minute)
	if err != nil || job.ID != id {
		t.Fatalf("the ID the client was given (%s) is not the job that exists (%+v, %v)", id, job, err)
	}
	if got := c.nodes[lead].Metrics().DedupedEnqueues; got != 1 {
		t.Fatalf("the node counted %d deduplicated enqueues, want 1", got)
	}
}

func TestRequestIDHeader(t *testing.T) {
	c := newHTTPCluster(t)
	lead := c.leader()
	post := func(key string) (int, string) {
		req, _ := http.NewRequest(http.MethodPost, c.clients[lead].URL+"/v1/topics/work/jobs", strings.NewReader("raw-body"))
		if key != "" {
			req.Header.Set(RequestIDHeader, key)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(body)
	}
	_, a := post("k1")
	_, b := post("k1")
	_, d := post("")
	if a != b || a == d {
		t.Fatalf("same key gave %s and %s; no key gave %s", a, b, d)
	}
	if code, body := post(strings.Repeat("k", jobq.MaxRequestIDLen+1)); code != http.StatusBadRequest || !strings.Contains(body, "request_id_too_long") {
		t.Fatalf("oversized key: %d %s", code, body)
	}
	// A caller's own ID passes through the client unchanged.
	cl := NewClient(c.allAddrs())
	x, err := cl.Enqueue("work", []byte("p"), jobq.WithRequestID("mine"))
	if err != nil {
		t.Fatal(err)
	}
	if y, _ := cl.Enqueue("work", []byte("p"), jobq.WithRequestID("mine")); y != x {
		t.Fatalf("the caller's request ID was not honoured: %s then %s", x, y)
	}
	if _, err := cl.Enqueue("work", nil, jobq.WithRequestID(strings.Repeat("k", jobq.MaxRequestIDLen+1))); err != jobq.ErrRequestIDTooLong {
		t.Fatalf("client with an oversized ID: %v", err)
	}
}
