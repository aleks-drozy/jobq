package jobq

import (
	"testing"
	"time"
)

// Peek is the read-only inspection path added in P3.5: it must never
// mutate queue state (no Attempts++, no lease, no WAL record), because
// Dequeue is the only other read path and Dequeue mutates on every call.

func TestPeekDoesNotConsumeAttempts(t *testing.T) {
	q, _ := newTestQueue(t)
	if _, err := q.Enqueue("peek-attempts", []byte("x"), WithMaxAttempts(2)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		ready, _, err := q.Peek("peek-attempts", 10, false)
		if err != nil {
			t.Fatalf("peek %d: %v", i, err)
		}
		if len(ready) != 1 {
			t.Fatalf("peek %d: got %d ready jobs, want 1", i, len(ready))
		}
		if ready[0].Attempts != 0 {
			t.Fatalf("peek %d: Attempts = %d, want 0 (peek must not consume)", i, ready[0].Attempts)
		}
	}
	stats := q.Stats("peek-attempts")
	if stats.Ready != 1 {
		t.Fatalf("Stats.Ready = %d, want 1 after 20 peeks", stats.Ready)
	}
}

// TestPeekOnDLQDoesNotDestroyEvidence is the regression guard for the whole
// reason Peek exists: dequeue-then-nack as a "peek" on a DLQ job burns an
// attempt on every look, and a DLQ has no onward DLQ, so five such "peeks"
// on a DefaultMaxAttempts job would drop it into Stats.Dropped forever.
func TestPeekOnDLQDoesNotDestroyEvidence(t *testing.T) {
	q, _ := newTestQueue(t)
	if _, err := q.Enqueue("peek-dlq-src", []byte("poison"), WithMaxAttempts(1)); err != nil {
		t.Fatal(err)
	}
	_, lease, err := q.Dequeue("peek-dlq-src", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Nack(lease); err != nil {
		t.Fatal(err)
	}
	dlq := DeadLetterTopic("peek-dlq-src")

	for i := 0; i < 20; i++ {
		ready, _, err := q.Peek(dlq, 10, false)
		if err != nil {
			t.Fatalf("peek %d: %v", i, err)
		}
		if len(ready) != 1 {
			t.Fatalf("peek %d: got %d dlq jobs, want 1", i, len(ready))
		}
	}
	stats := q.Stats(dlq)
	if stats.Dropped != 0 {
		t.Fatalf("Stats(%q).Dropped = %d, want 0 — peeking must never destroy DLQ evidence", dlq, stats.Dropped)
	}
	if stats.Ready != 1 {
		t.Fatalf("Stats(%q).Ready = %d, want 1", dlq, stats.Ready)
	}
}

func TestPeekThenDequeueSeesSameJobAtAttemptOne(t *testing.T) {
	q, _ := newTestQueue(t)
	if _, err := q.Enqueue("peek-then-dequeue", []byte("payload")); err != nil {
		t.Fatal(err)
	}
	ready, _, err := q.Peek("peek-then-dequeue", 10, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(ready) != 1 {
		t.Fatalf("got %d peeked jobs, want 1", len(ready))
	}
	peeked := ready[0]

	job, _, err := q.Dequeue("peek-then-dequeue", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if job.ID != peeked.ID {
		t.Fatalf("dequeued job ID %q != peeked job ID %q", job.ID, peeked.ID)
	}
	if job.Attempt != 1 {
		t.Fatalf("Attempt after first real dequeue = %d, want 1", job.Attempt)
	}
}

func TestPeekIsFIFOOrderedAndKeepsNotBefore(t *testing.T) {
	q, _ := newTestQueue(t)
	if _, err := q.Enqueue("peek-fifo", []byte("first")); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Enqueue("peek-fifo", []byte("second"), WithDelay(time.Hour)); err != nil {
		t.Fatal(err)
	}

	ready, _, err := q.Peek("peek-fifo", 10, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(ready) != 2 {
		t.Fatalf("got %d ready jobs, want 2 (peek does not filter by NotBefore)", len(ready))
	}
	if string(ready[0].Payload) != "first" || string(ready[1].Payload) != "second" {
		t.Fatalf("FIFO order violated: got %q then %q", ready[0].Payload, ready[1].Payload)
	}
	if ready[1].NotBefore.IsZero() {
		t.Fatalf("delayed job's NotBefore was not preserved in the peeked copy")
	}
}

func TestPeekOfDeadLetteredJobPreservesEvidence(t *testing.T) {
	q, _ := newTestQueue(t)
	if _, err := q.Enqueue("peek-evidence-src", []byte("poison"), WithMaxAttempts(1)); err != nil {
		t.Fatal(err)
	}
	_, lease, err := q.Dequeue("peek-evidence-src", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Nack(lease); err != nil {
		t.Fatal(err)
	}

	ready, _, err := q.Peek(DeadLetterTopic("peek-evidence-src"), 10, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(ready) != 1 {
		t.Fatalf("got %d dlq jobs, want 1", len(ready))
	}
	if ready[0].DeadLetteredAttempts != 1 {
		t.Fatalf("DeadLetteredAttempts = %d, want 1", ready[0].DeadLetteredAttempts)
	}
	if ready[0].DeadLetteredAt.IsZero() {
		t.Fatalf("DeadLetteredAt was not preserved")
	}
}

func TestPeekedPayloadMutationDoesNotCorruptQueue(t *testing.T) {
	q, _ := newTestQueue(t)
	if _, err := q.Enqueue("peek-mutation", []byte("original")); err != nil {
		t.Fatal(err)
	}
	ready, _, err := q.Peek("peek-mutation", 10, false)
	if err != nil {
		t.Fatal(err)
	}
	ready[0].Payload[0] = 'X'

	job, _, err := q.Dequeue("peek-mutation", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if string(job.Payload) != "original" {
		t.Fatalf("payload = %q, want %q — mutating a peeked copy must not corrupt queue state", job.Payload, "original")
	}
}

func TestPeekUnknownTopicIsEmptyNotError(t *testing.T) {
	q, _ := newTestQueue(t)
	ready, inflight, err := q.Peek("peek-never-used", 10, true)
	if err != nil {
		t.Fatalf("Peek(unknown) err = %v, want nil", err)
	}
	if len(ready) != 0 || len(inflight) != 0 {
		t.Fatalf("Peek(unknown) = %v, %v, want empty", ready, inflight)
	}
}

func TestPeekIncludesInflightWhenAsked(t *testing.T) {
	q, _ := newTestQueue(t)
	if _, err := q.Enqueue("peek-inflight", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := q.Dequeue("peek-inflight", time.Minute); err != nil {
		t.Fatal(err)
	}

	ready, inflight, err := q.Peek("peek-inflight", 10, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(ready) != 0 {
		t.Fatalf("ready = %d, want 0 (the only job is leased)", len(ready))
	}
	if len(inflight) != 1 {
		t.Fatalf("inflight = %d, want 1", len(inflight))
	}
	if inflight[0].Attempts != 1 {
		t.Fatalf("inflight job Attempts = %d, want 1 (peek did not add a second)", inflight[0].Attempts)
	}

	// includeInflight=false must not report it.
	ready2, inflight2, err := q.Peek("peek-inflight", 10, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(ready2) != 0 || inflight2 != nil {
		t.Fatalf("includeInflight=false leaked inflight jobs: ready=%v inflight=%v", ready2, inflight2)
	}
}

func TestPeekRespectsLimit(t *testing.T) {
	q, _ := newTestQueue(t)
	for i := 0; i < 5; i++ {
		if _, err := q.Enqueue("peek-limit", []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	ready, _, err := q.Peek("peek-limit", 3, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(ready) != 3 {
		t.Fatalf("got %d jobs, want 3 (limit)", len(ready))
	}
	stats := q.Stats("peek-limit")
	if stats.Ready != 5 {
		t.Fatalf("Stats.Ready = %d, want 5 (peek must not have consumed anything)", stats.Ready)
	}
}
