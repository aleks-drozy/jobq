package jobqhttp

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aleks-drozy/jobq"
)

func newTestServer(t *testing.T, opts ...HandlerOption) (*jobq.Queue, *httptest.Server) {
	t.Helper()
	q := jobq.New()
	t.Cleanup(func() { _ = q.Close() })
	srv := httptest.NewServer(NewHandler(q, opts...))
	t.Cleanup(srv.Close)
	return q, srv
}

func doRaw(t *testing.T, srv *httptest.Server, method, path, contentType string, body []byte) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, srv.URL+path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func decodeEnvelope(t *testing.T, resp *http.Response) errorEnvelope {
	t.Helper()
	defer resp.Body.Close()
	var env errorEnvelope
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		t.Fatalf("decode error envelope: %v", err)
	}
	return env
}

func TestEnqueueJSONContentType(t *testing.T) {
	_, srv := newTestServer(t)
	body, _ := json.Marshal(enqueueJSONRequest{Payload: []byte("hello"), MaxAttempts: 3})
	resp := doRaw(t, srv, http.MethodPost, "/v1/topics/emails/jobs", "application/json", body)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want 201", resp.StatusCode)
	}
	var out enqueueResponseDTO
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.ID == "" {
		t.Error("empty job id")
	}
}

func TestEnqueueRawBodyContentType(t *testing.T) {
	_, srv := newTestServer(t)
	resp := doRaw(t, srv, http.MethodPost, "/v1/topics/emails/jobs?delay_ms=100&max_attempts=2", "text/plain", []byte("raw payload"))
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want 201", resp.StatusCode)
	}
}

func TestEnqueueRawBodyIsPayloadVerbatim(t *testing.T) {
	q, srv := newTestServer(t)
	resp := doRaw(t, srv, http.MethodPost, "/v1/topics/raw/jobs", "", []byte("curl-demoable"))
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want 201", resp.StatusCode)
	}
	job, _, err := q.Dequeue("raw", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if string(job.Payload) != "curl-demoable" {
		t.Errorf("payload = %q, want %q", job.Payload, "curl-demoable")
	}
}

func TestEnqueueReservedTopicIs400(t *testing.T) {
	_, srv := newTestServer(t)
	resp := doRaw(t, srv, http.MethodPost, "/v1/topics/x.dlq/jobs", "", []byte("x"))
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	env := decodeEnvelope(t, resp)
	if env.Error.Code != "reserved_topic" {
		t.Errorf("code = %q, want reserved_topic", env.Error.Code)
	}
}

func TestEnqueueOversizeBodyIs413(t *testing.T) {
	_, srv := newTestServer(t, WithMaxPayload(4))
	resp := doRaw(t, srv, http.MethodPost, "/v1/topics/big/jobs", "", []byte("way too big"))
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", resp.StatusCode)
	}
	env := decodeEnvelope(t, resp)
	if env.Error.Code != "payload_too_large" {
		t.Errorf("code = %q, want payload_too_large", env.Error.Code)
	}
}

func TestEnqueueUnknownJSONFieldIs400(t *testing.T) {
	_, srv := newTestServer(t)
	resp := doRaw(t, srv, http.MethodPost, "/v1/topics/emails/jobs", "application/json", []byte(`{"payload":"aGk=","bogus":true}`))
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	env := decodeEnvelope(t, resp)
	if env.Error.Code != "bad_request" {
		t.Errorf("code = %q, want bad_request", env.Error.Code)
	}
}

func TestWrongMethodIs405(t *testing.T) {
	// PUT is registered on neither the enqueue (POST) nor the peek (GET)
	// handler for this path, so the mux itself must reject it.
	_, srv := newTestServer(t)
	req, err := http.NewRequest(http.MethodPut, srv.URL+"/v1/topics/emails/jobs", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", resp.StatusCode)
	}
	if resp.Header.Get("Allow") == "" {
		t.Error("expected Allow header on 405")
	}
}

func TestDequeueEmptyTopicIs204(t *testing.T) {
	_, srv := newTestServer(t)
	resp := doRaw(t, srv, http.MethodPost, "/v1/topics/never-used/dequeue", "application/json", []byte(`{"visibility_ms":1000}`))
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", resp.StatusCode)
	}
}

func TestDequeueReturnsEnqueuedJob(t *testing.T) {
	q, srv := newTestServer(t)
	if _, err := q.Enqueue("work", []byte("payload")); err != nil {
		t.Fatal(err)
	}
	resp := doRaw(t, srv, http.MethodPost, "/v1/topics/work/dequeue", "application/json", []byte(`{"visibility_ms":30000}`))
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var out dequeueResponseDTO
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.Job == nil || string(out.Job.Payload) != "payload" {
		t.Fatalf("job = %+v", out.Job)
	}
	if out.Lease == nil || out.Lease.ID == "" {
		t.Fatalf("lease = %+v", out.Lease)
	}
}

func TestAckUnknownLeaseIs404(t *testing.T) {
	_, srv := newTestServer(t)
	resp := doRaw(t, srv, http.MethodPost, "/v1/topics/work/leases/nope/ack", "", nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
	env := decodeEnvelope(t, resp)
	if env.Error.Code != "unknown_lease" {
		t.Errorf("code = %q, want unknown_lease", env.Error.Code)
	}
}

func TestAckSettlesLease(t *testing.T) {
	q, srv := newTestServer(t)
	if _, err := q.Enqueue("work", []byte("x")); err != nil {
		t.Fatal(err)
	}
	_, lease, err := q.Dequeue("work", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	resp := doRaw(t, srv, http.MethodPost, "/v1/topics/work/leases/"+lease.ID+"/ack", "", nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", resp.StatusCode)
	}
	st := q.Stats("work")
	if st.Acked != 1 {
		t.Errorf("Acked = %d, want 1", st.Acked)
	}
}

func TestNackRetriesJob(t *testing.T) {
	q, srv := newTestServer(t)
	if _, err := q.Enqueue("work", []byte("x"), jobq.WithMaxAttempts(3)); err != nil {
		t.Fatal(err)
	}
	_, lease, err := q.Dequeue("work", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	resp := doRaw(t, srv, http.MethodPost, "/v1/topics/work/leases/"+lease.ID+"/nack", "", nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", resp.StatusCode)
	}
	st := q.Stats("work")
	if st.Ready != 1 {
		t.Errorf("Ready = %d, want 1 (retried)", st.Ready)
	}
}

func TestExtendLease(t *testing.T) {
	q, srv := newTestServer(t)
	if _, err := q.Enqueue("work", []byte("x")); err != nil {
		t.Fatal(err)
	}
	_, lease, err := q.Dequeue("work", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(extendRequestDTO{ByMs: 60000})
	resp := doRaw(t, srv, http.MethodPost, "/v1/topics/work/leases/"+lease.ID+"/extend", "application/json", body)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var out extendResponseDTO
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if !out.Deadline.After(lease.Deadline) {
		t.Errorf("extended deadline %v not after original %v", out.Deadline, lease.Deadline)
	}
}

func TestExtendNonPositiveIs400(t *testing.T) {
	q, srv := newTestServer(t)
	if _, err := q.Enqueue("work", []byte("x")); err != nil {
		t.Fatal(err)
	}
	_, lease, err := q.Dequeue("work", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(extendRequestDTO{ByMs: 0})
	resp := doRaw(t, srv, http.MethodPost, "/v1/topics/work/leases/"+lease.ID+"/extend", "application/json", body)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	env := decodeEnvelope(t, resp)
	if env.Error.Code != "non_positive_duration" {
		t.Errorf("code = %q, want non_positive_duration", env.Error.Code)
	}
}

func TestTopicsListedSorted(t *testing.T) {
	q, srv := newTestServer(t)
	for _, name := range []string{"zeta", "alpha", "mid"} {
		if _, err := q.Enqueue(name, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	resp := doRaw(t, srv, http.MethodGet, "/v1/topics", "", nil)
	defer resp.Body.Close()
	var out topicsResponseDTO
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	want := []string{"alpha", "mid", "zeta"}
	if len(out.Topics) != len(want) {
		t.Fatalf("topics = %v, want %v", out.Topics, want)
	}
	for i := range want {
		if out.Topics[i] != want[i] {
			t.Errorf("topics[%d] = %q, want %q", i, out.Topics[i], want[i])
		}
	}
}

func TestUnknownTopicStatsAreZero(t *testing.T) {
	_, srv := newTestServer(t)
	resp := doRaw(t, srv, http.MethodGet, "/v1/topics/never-used/stats", "", nil)
	defer resp.Body.Close()
	var st jobq.Stats
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		t.Fatal(err)
	}
	if st != (jobq.Stats{}) {
		t.Errorf("stats = %+v, want zero", st)
	}
}

func TestHealthReportsInMemoryAsNotDurable(t *testing.T) {
	_, srv := newTestServer(t)
	resp := doRaw(t, srv, http.MethodGet, "/v1/health", "", nil)
	defer resp.Body.Close()
	var out healthResponseDTO
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.Status != "ok" {
		t.Errorf("status = %q, want ok", out.Status)
	}
	if out.Durable {
		t.Error("in-memory queue reported durable")
	}
}

func TestHealthReportsDurableWhenRecoveryAttached(t *testing.T) {
	q := jobq.New()
	defer q.Close()
	srv := httptest.NewServer(NewHandler(q, WithRecovery(jobq.RecoveryReport{Segments: 2}, "/data", "always")))
	defer srv.Close()

	resp := doRaw(t, srv, http.MethodGet, "/v1/health", "", nil)
	defer resp.Body.Close()
	var out healthResponseDTO
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if !out.Durable {
		t.Error("durable queue reported not durable")
	}

	resp2 := doRaw(t, srv, http.MethodGet, "/v1/recovery", "", nil)
	defer resp2.Body.Close()
	var rec recoveryResponseDTO
	if err := json.NewDecoder(resp2.Body).Decode(&rec); err != nil {
		t.Fatal(err)
	}
	if rec.Report.Segments != 2 || rec.Dir != "/data" || rec.Sync != "always" {
		t.Errorf("recovery = %+v, want segments=2 dir=/data sync=always", rec)
	}
}
