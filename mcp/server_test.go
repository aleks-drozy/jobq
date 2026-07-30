package mcp

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/aleks-drozy/jobq"
	"github.com/aleks-drozy/jobq/jobqhttp"
)

// This is the headline test, the direct descendant of
// jobqhttp/roundtrip_test.go: one behaviour suite, run twice — once against
// a Backend embedding a local *jobq.Queue, once against a Backend that is a
// *jobqhttp.Client talking to a live httptest server wrapping an identical
// queue. Any divergence in tool output or error mapping fails the build.

type toolErrorDTO struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func newTestSession(t *testing.T, be Backend, opts ServerOptions) *sdk.ClientSession {
	t.Helper()
	server := NewServer(be, opts)
	client := sdk.NewClient(&sdk.Implementation{Name: "test-client", Version: "v0.0.0"}, nil)
	t1, t2 := sdk.NewInMemoryTransports()
	ctx := context.Background()
	if _, err := server.Connect(ctx, t1, nil); err != nil {
		t.Fatalf("server.Connect: %v", err)
	}
	cs, err := client.Connect(ctx, t2, nil)
	if err != nil {
		t.Fatalf("client.Connect: %v", err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	return cs
}

// callTool invokes name with args and decodes the result. If the call
// returned a tool error, ok is false and errDTO carries the code/message
// this package's errors.go put into the error's JSON text.
func callTool[Out any](t *testing.T, cs *sdk.ClientSession, name string, args any) (out Out, ok bool, errDTO toolErrorDTO) {
	t.Helper()
	res, err := cs.CallTool(context.Background(), &sdk.CallToolParams{Name: name, Arguments: args})
	if err != nil {
		t.Fatalf("CallTool(%s): protocol error: %v", name, err)
	}
	if res.IsError {
		if len(res.Content) > 0 {
			if tc, ok2 := res.Content[0].(*sdk.TextContent); ok2 {
				if uerr := json.Unmarshal([]byte(tc.Text), &errDTO); uerr != nil {
					t.Fatalf("CallTool(%s): error content not JSON: %v (%q)", name, uerr, tc.Text)
				}
			}
		}
		return out, false, errDTO
	}
	raw, err := json.Marshal(res.StructuredContent)
	if err != nil {
		t.Fatalf("CallTool(%s): re-marshaling structured content: %v", name, err)
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("CallTool(%s): decoding structured content %s: %v", name, raw, err)
	}
	return out, true, toolErrorDTO{}
}

func newLocalBackendForTest(t *testing.T) Backend {
	t.Helper()
	q := jobq.New()
	t.Cleanup(func() { _ = q.Close() })
	return NewInMemoryBackend(q)
}

func newHTTPBackendForTest(t *testing.T) Backend {
	t.Helper()
	q := jobq.New()
	srv := httptest.NewServer(jobqhttp.NewHandler(q))
	t.Cleanup(func() { srv.Close(); _ = q.Close() })
	return jobqhttp.NewClient(srv.URL)
}

func TestToolSuite(t *testing.T) {
	variants := []struct {
		name string
		make func(t *testing.T) Backend
	}{
		{"local", newLocalBackendForTest},
		{"http", newHTTPBackendForTest},
	}
	for _, v := range variants {
		t.Run(v.name, func(t *testing.T) {
			cs := newTestSession(t, v.make(t), ServerOptions{})
			runToolSuite(t, cs)
		})
	}
}

func runToolSuite(t *testing.T, cs *sdk.ClientSession) {
	t.Run("enqueue dequeue ack", func(t *testing.T) {
		enq, ok, _ := callTool[enqueueOut](t, cs, "jobq_enqueue", map[string]any{
			"topic": "suite-basic", "payload": "hello", "max_attempts": 3,
		})
		if !ok {
			t.Fatal("enqueue returned an error")
		}
		if enq.MaxAttempts != 3 {
			t.Fatalf("MaxAttempts = %d, want 3", enq.MaxAttempts)
		}

		deq, ok, errDTO := callTool[dequeueOut](t, cs, "jobq_dequeue", map[string]any{"topic": "suite-basic"})
		if !ok {
			t.Fatalf("dequeue error: %+v", errDTO)
		}
		if deq.Empty || deq.Job == nil {
			t.Fatal("dequeue reported empty on a topic that was just enqueued to")
		}
		if deq.Job.Payload == nil || *deq.Job.Payload != "hello" {
			t.Fatalf("payload = %+v, want \"hello\"", deq.Job.encodedPayload)
		}
		if deq.Job.Attempt != 1 {
			t.Fatalf("Attempt = %d, want 1", deq.Job.Attempt)
		}

		ack, ok, errDTO := callTool[ackOut](t, cs, "jobq_ack", map[string]any{
			"topic": "suite-basic", "lease_id": deq.Lease.ID,
		})
		if !ok || !ack.Acked {
			t.Fatalf("ack failed: ok=%v err=%+v", ok, errDTO)
		}
	})

	t.Run("dequeue empty topic is empty not error", func(t *testing.T) {
		deq, ok, errDTO := callTool[dequeueOut](t, cs, "jobq_dequeue", map[string]any{"topic": "suite-never-used"})
		if !ok {
			t.Fatalf("expected empty result, got error: %+v", errDTO)
		}
		if !deq.Empty {
			t.Fatal("Empty = false, want true")
		}
	})

	t.Run("peek does not consume an attempt", func(t *testing.T) {
		if _, ok, _ := callTool[enqueueOut](t, cs, "jobq_enqueue", map[string]any{
			"topic": "suite-peek", "payload": "x",
		}); !ok {
			t.Fatal("enqueue failed")
		}
		for i := 0; i < 3; i++ {
			pk, ok, errDTO := callTool[peekOut](t, cs, "jobq_peek", map[string]any{"topic": "suite-peek"})
			if !ok {
				t.Fatalf("peek %d error: %+v", i, errDTO)
			}
			if len(pk.Ready) != 1 {
				t.Fatalf("peek %d: ready = %d, want 1", i, len(pk.Ready))
			}
			if pk.Ready[0].Attempts != 0 {
				t.Fatalf("peek %d: Attempts = %d, want 0", i, pk.Ready[0].Attempts)
			}
		}
		stats, ok, _ := callTool[topicStatsOut](t, cs, "jobq_topic_stats", map[string]any{"topic": "suite-peek"})
		if !ok || stats.Topics["suite-peek"].Ready != 1 {
			t.Fatalf("Stats after peeks = %+v, want Ready=1", stats)
		}
	})

	t.Run("nack retries then dead-letters, dlq peek preserves evidence", func(t *testing.T) {
		if _, ok, _ := callTool[enqueueOut](t, cs, "jobq_enqueue", map[string]any{
			"topic": "suite-dlq-src", "payload": "poison", "max_attempts": 1,
		}); !ok {
			t.Fatal("enqueue failed")
		}
		deq, ok, _ := callTool[dequeueOut](t, cs, "jobq_dequeue", map[string]any{"topic": "suite-dlq-src"})
		if !ok || deq.Job == nil {
			t.Fatal("dequeue failed")
		}
		nk, ok, errDTO := callTool[nackOut](t, cs, "jobq_nack", map[string]any{
			"topic": "suite-dlq-src", "lease_id": deq.Lease.ID,
		})
		if !ok || !nk.Nacked {
			t.Fatalf("nack failed: %+v", errDTO)
		}

		pk, ok, errDTO := callTool[peekOut](t, cs, "jobq_peek", map[string]any{"topic": "suite-dlq-src.dlq"})
		if !ok {
			t.Fatalf("dlq peek error: %+v", errDTO)
		}
		if len(pk.Ready) != 1 {
			t.Fatalf("dlq ready = %d, want 1", len(pk.Ready))
		}
		if pk.Ready[0].DeadLetteredAttempts != 1 {
			t.Fatalf("DeadLetteredAttempts = %d, want 1", pk.Ready[0].DeadLetteredAttempts)
		}

		topics, ok, _ := callTool[listTopicsOut](t, cs, "jobq_list_topics", map[string]any{})
		if !ok {
			t.Fatal("list_topics failed")
		}
		if !containsStr(topics.DlqTopics, "suite-dlq-src.dlq") {
			t.Fatalf("dlq_topics = %v, want to contain suite-dlq-src.dlq", topics.DlqTopics)
		}
	})

	t.Run("extend lengthens the deadline", func(t *testing.T) {
		if _, ok, _ := callTool[enqueueOut](t, cs, "jobq_enqueue", map[string]any{
			"topic": "suite-extend", "payload": "x",
		}); !ok {
			t.Fatal("enqueue failed")
		}
		deq, ok, _ := callTool[dequeueOut](t, cs, "jobq_dequeue", map[string]any{"topic": "suite-extend"})
		if !ok || deq.Lease == nil {
			t.Fatal("dequeue failed")
		}
		ext, ok, errDTO := callTool[extendOut](t, cs, "jobq_extend", map[string]any{
			"topic": "suite-extend", "lease_id": deq.Lease.ID, "by_ms": 120000,
		})
		if !ok {
			t.Fatalf("extend failed: %+v", errDTO)
		}
		if !ext.Deadline.After(deq.Lease.Deadline) {
			t.Fatalf("extended deadline %v not after original %v", ext.Deadline, deq.Lease.Deadline)
		}
	})

	t.Run("dlq_requeue moves a job back with a new ID", func(t *testing.T) {
		if _, ok, _ := callTool[enqueueOut](t, cs, "jobq_enqueue", map[string]any{
			"topic": "suite-requeue", "payload": "crash-victim", "max_attempts": 1,
		}); !ok {
			t.Fatal("enqueue failed")
		}
		deq, ok, _ := callTool[dequeueOut](t, cs, "jobq_dequeue", map[string]any{"topic": "suite-requeue"})
		if !ok || deq.Job == nil {
			t.Fatal("dequeue failed")
		}
		origID := deq.Job.ID
		if _, ok, _ := callTool[nackOut](t, cs, "jobq_nack", map[string]any{
			"topic": "suite-requeue", "lease_id": deq.Lease.ID,
		}); !ok {
			t.Fatal("nack failed")
		}

		rq, ok, errDTO := callTool[dlqRequeueOut](t, cs, "jobq_dlq_requeue", map[string]any{"topic": "suite-requeue"})
		if !ok {
			t.Fatalf("dlq_requeue failed: %+v", errDTO)
		}
		if len(rq.Requeued) != 1 {
			t.Fatalf("requeued = %d, want 1", len(rq.Requeued))
		}
		if rq.Requeued[0].DLQJobID != origID {
			t.Fatalf("dlq_job_id = %q, want %q", rq.Requeued[0].DLQJobID, origID)
		}
		if rq.Requeued[0].NewJobID == origID {
			t.Fatal("new_job_id must differ from the DLQ job id (no move primitive)")
		}

		deq2, ok, _ := callTool[dequeueOut](t, cs, "jobq_dequeue", map[string]any{"topic": "suite-requeue"})
		if !ok || deq2.Job == nil {
			t.Fatal("requeued job did not land back on the source topic")
		}
		if deq2.Job.ID != rq.Requeued[0].NewJobID {
			t.Fatalf("dequeued id %q != requeued new_job_id %q", deq2.Job.ID, rq.Requeued[0].NewJobID)
		}
	})

	t.Run("health and recovery_report respond", func(t *testing.T) {
		h, ok, errDTO := callTool[healthOut](t, cs, "jobq_health", map[string]any{})
		if !ok || h.Status != "ok" {
			t.Fatalf("health failed: ok=%v err=%+v got=%+v", ok, errDTO, h)
		}
		rr, ok, errDTO := callTool[recoveryOut](t, cs, "jobq_recovery_report", map[string]any{})
		if !ok {
			t.Fatalf("recovery_report failed: %+v", errDTO)
		}
		if len(rr.Interpretation) == 0 {
			t.Fatal("interpretation must not be empty")
		}
	})

	t.Run("error mapping: unknown lease", func(t *testing.T) {
		_, ok, errDTO := callTool[ackOut](t, cs, "jobq_ack", map[string]any{
			"topic": "suite-basic", "lease_id": "forged",
		})
		if ok {
			t.Fatal("ack on a forged lease succeeded, want unknown_lease")
		}
		if errDTO.Code != "unknown_lease" {
			t.Fatalf("code = %q, want unknown_lease", errDTO.Code)
		}
	})

	t.Run("error mapping: reserved topic", func(t *testing.T) {
		_, ok, errDTO := callTool[enqueueOut](t, cs, "jobq_enqueue", map[string]any{
			"topic": "suite-reserved.dlq", "payload": "x",
		})
		if ok {
			t.Fatal("enqueue to a .dlq topic succeeded, want reserved_topic")
		}
		if errDTO.Code != "reserved_topic" {
			t.Fatalf("code = %q, want reserved_topic", errDTO.Code)
		}
	})

	t.Run("error mapping: non-positive extend", func(t *testing.T) {
		_, ok, errDTO := callTool[extendOut](t, cs, "jobq_extend", map[string]any{
			"topic": "suite-basic", "lease_id": "whatever", "by_ms": 0,
		})
		if ok {
			t.Fatal("extend with by_ms=0 succeeded, want non_positive_duration")
		}
		if errDTO.Code != "non_positive_duration" {
			t.Fatalf("code = %q, want non_positive_duration", errDTO.Code)
		}
	})
}

func containsStr(ss []string, want string) bool {
	for _, s := range ss {
		if s == want {
			return true
		}
	}
	return false
}

// --- schema conformance --------------------------------------------------

func TestToolListCounts(t *testing.T) {
	t.Run("default: 11 tools", func(t *testing.T) {
		cs := newTestSession(t, newLocalBackendForTest(t), ServerOptions{})
		res, err := cs.ListTools(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(res.Tools) != 11 {
			names := make([]string, len(res.Tools))
			for i, tl := range res.Tools {
				names[i] = tl.Name
			}
			t.Fatalf("got %d tools, want 11: %v", len(res.Tools), names)
		}
	})

	// The five read-only tools are jobq_peek, jobq_list_topics,
	// jobq_topic_stats, jobq_health, and jobq_recovery_report.
	t.Run("-read-only: 5 tools, all readOnlyHint", func(t *testing.T) {
		cs := newTestSession(t, newLocalBackendForTest(t), ServerOptions{ReadOnly: true})
		res, err := cs.ListTools(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(res.Tools) != 5 {
			t.Fatalf("got %d tools, want 5", len(res.Tools))
		}
		for _, tl := range res.Tools {
			if tl.Annotations == nil || !tl.Annotations.ReadOnlyHint {
				t.Errorf("tool %q missing readOnlyHint under -read-only", tl.Name)
			}
		}
	})
}

func TestToolInputSchemasAreObjects(t *testing.T) {
	cs := newTestSession(t, newLocalBackendForTest(t), ServerOptions{})
	res, err := cs.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, tl := range res.Tools {
		m, ok := tl.InputSchema.(map[string]any)
		if !ok {
			t.Errorf("tool %q: InputSchema is %T, want an object", tl.Name, tl.InputSchema)
			continue
		}
		if m["type"] != "object" {
			t.Errorf("tool %q: schema type = %v, want object", tl.Name, m["type"])
		}
	}
}

// --- attempt-burn regression ----------------------------------------------

// This is the regression guard for the entire reason Peek exists: 20
// jobq_peek calls on a dead-lettered job must never move it toward
// Stats.Dropped.
func TestAttemptBurnRegression(t *testing.T) {
	for _, v := range []struct {
		name string
		make func(t *testing.T) Backend
	}{
		{"local", newLocalBackendForTest},
		{"http", newHTTPBackendForTest},
	} {
		t.Run(v.name, func(t *testing.T) {
			cs := newTestSession(t, v.make(t), ServerOptions{})
			if _, ok, _ := callTool[enqueueOut](t, cs, "jobq_enqueue", map[string]any{
				"topic": "burn-src", "payload": "poison", "max_attempts": 1,
			}); !ok {
				t.Fatal("enqueue failed")
			}
			deq, ok, _ := callTool[dequeueOut](t, cs, "jobq_dequeue", map[string]any{"topic": "burn-src"})
			if !ok || deq.Job == nil {
				t.Fatal("dequeue failed")
			}
			if _, ok, _ := callTool[nackOut](t, cs, "jobq_nack", map[string]any{
				"topic": "burn-src", "lease_id": deq.Lease.ID,
			}); !ok {
				t.Fatal("nack failed")
			}

			for i := 0; i < 20; i++ {
				pk, ok, errDTO := callTool[peekOut](t, cs, "jobq_peek", map[string]any{"topic": "burn-src.dlq"})
				if !ok {
					t.Fatalf("peek %d failed: %+v", i, errDTO)
				}
				if len(pk.Ready) != 1 || pk.Ready[0].Attempts != 0 {
					t.Fatalf("peek %d: ready = %+v, want one job at Attempts=0", i, pk.Ready)
				}
			}
			stats, ok, _ := callTool[topicStatsOut](t, cs, "jobq_topic_stats", map[string]any{"topic": "burn-src.dlq"})
			if !ok {
				t.Fatal("topic_stats failed")
			}
			s := stats.Topics["burn-src.dlq"]
			if s.Dropped != 0 {
				t.Fatalf("Dropped = %d, want 0 — 20 peeks must never destroy DLQ evidence", s.Dropped)
			}
			if s.Ready != 1 {
				t.Fatalf("Ready = %d, want 1", s.Ready)
			}
		})
	}
}

// --- dlq_requeue ordering --------------------------------------------------

// faultyAck wraps a Backend and fails the Nth Ack call, to prove
// jobq_dlq_requeue's enqueue-before-ack ordering: even when the ack after a
// successful enqueue fails (simulating a crash in that window), the
// enqueued copy is not rolled back — the fault risks a duplicate, never a
// loss.
type faultyAck struct {
	Backend
	failAckAfter int // fail the call at this 1-based count
	acks         int
}

func (f *faultyAck) Ack(lease jobq.Lease) error {
	f.acks++
	if f.acks == f.failAckAfter {
		return errFault
	}
	return f.Backend.Ack(lease)
}

var errFault = &toolError{Code: "fault_injected", Message: "simulated crash between enqueue and ack"}

func TestDLQRequeueOrdering(t *testing.T) {
	q := jobq.New()
	defer q.Close()
	base := NewInMemoryBackend(q)
	fb := &faultyAck{Backend: base, failAckAfter: 1}
	cs := newTestSession(t, fb, ServerOptions{})

	if _, ok, _ := callTool[enqueueOut](t, cs, "jobq_enqueue", map[string]any{
		"topic": "order-src", "payload": "victim", "max_attempts": 1,
	}); !ok {
		t.Fatal("enqueue failed")
	}
	deq, ok, _ := callTool[dequeueOut](t, cs, "jobq_dequeue", map[string]any{"topic": "order-src"})
	if !ok || deq.Job == nil {
		t.Fatal("dequeue failed")
	}
	if _, ok, _ := callTool[nackOut](t, cs, "jobq_nack", map[string]any{
		"topic": "order-src", "lease_id": deq.Lease.ID,
	}); !ok {
		t.Fatal("nack failed")
	}

	_, ok, errDTO := callTool[dlqRequeueOut](t, cs, "jobq_dlq_requeue", map[string]any{"topic": "order-src"})
	if ok {
		t.Fatal("dlq_requeue succeeded despite the injected ack fault")
	}
	if errDTO.Code != "fault_injected" {
		t.Fatalf("code = %q, want fault_injected", errDTO.Code)
	}

	// The enqueue that happened before the fault must not have been rolled
	// back: the new job is on the source topic even though the tool call
	// reported an error. That is the duplicate-not-loss property.
	stats := q.Stats("order-src")
	if stats.Ready != 1 {
		t.Fatalf("source topic Ready = %d, want 1 — the pre-fault enqueue must survive the ack failure", stats.Ready)
	}
}

// --- misc -------------------------------------------------------------

func TestUnknownTopicPeekIsEmptyNotError(t *testing.T) {
	cs := newTestSession(t, newLocalBackendForTest(t), ServerOptions{})
	pk, ok, errDTO := callTool[peekOut](t, cs, "jobq_peek", map[string]any{"topic": "never-touched"})
	if !ok {
		t.Fatalf("peek on unknown topic errored: %+v", errDTO)
	}
	if len(pk.Ready) != 0 {
		t.Fatalf("ready = %d, want 0", len(pk.Ready))
	}
}

func TestReadOnlyModeHasNoWriteTools(t *testing.T) {
	cs := newTestSession(t, newLocalBackendForTest(t), ServerOptions{ReadOnly: true})
	res, err := cs.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, tl := range res.Tools {
		switch tl.Name {
		case "jobq_enqueue", "jobq_dequeue", "jobq_ack", "jobq_nack", "jobq_extend", "jobq_dlq_requeue":
			t.Errorf("write tool %q registered under -read-only", tl.Name)
		}
	}
}
