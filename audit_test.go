package jobq

// Regression tests for the 2026-07-22 adversarial audit of P1. Each test
// names the defect it pins. The audit's refutation stage was lost to a rate
// limit, so every claim here was re-verified by hand before being fixed.

import (
	"bytes"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestEnqueueToDeadLetterTopicIsRejected(t *testing.T) {
	// Audit: a user topic literally named "svc.dlq" collided with the
	// auto-created DLQ of "svc", and its own exhausted jobs were silently
	// destroyed (its deadLetter handler is nil). Direct enqueue to *.dlq is
	// now reserved; consumers still Dequeue/Ack/Nack DLQ jobs freely.
	q, _ := newTestQueue(t)
	if _, err := q.Enqueue("svc.dlq", []byte("x")); !errors.Is(err, ErrReservedTopic) {
		t.Fatalf("Enqueue(svc.dlq) = %v, want ErrReservedTopic", err)
	}
}

func TestExhaustedDLQJobIsDroppedAndCounted(t *testing.T) {
	// A poison job nacked to exhaustion INSIDE the DLQ cannot dead-letter
	// again (".dlq.dlq" chains are refused). It is dropped — but visibly,
	// via Stats.Dropped, never silently.
	q, _ := newTestQueue(t)
	if _, err := q.Enqueue("svc", []byte("poison"), WithMaxAttempts(1)); err != nil {
		t.Fatal(err)
	}
	_, lease, err := q.Dequeue("svc", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Nack(lease); err != nil { // 1/1 attempts -> DLQ
		t.Fatal(err)
	}

	dlq := DeadLetterTopic("svc")
	for i := 0; i < DefaultMaxAttempts; i++ {
		_, lease, err := q.Dequeue(dlq, time.Minute)
		if err != nil {
			t.Fatalf("dlq dequeue %d: %v", i, err)
		}
		if err := q.Nack(lease); err != nil {
			t.Fatalf("dlq nack %d: %v", i, err)
		}
	}
	if _, _, err := q.Dequeue(dlq, time.Minute); !errors.Is(err, ErrNoJob) {
		t.Fatal("exhausted DLQ job still deliverable")
	}
	st := q.Stats(dlq)
	if st.Dropped != 1 {
		t.Errorf("Stats.Dropped = %d, want 1 (destruction must be visible)", st.Dropped)
	}
	if st.DeadLetters != 0 {
		t.Errorf("Stats.DeadLetters on a DLQ = %d, want 0 (nothing moved onward)", st.DeadLetters)
	}
}

func TestDeadLettersCountsArrivalsNotIntentions(t *testing.T) {
	// Audit: retire() incremented DeadLetters before the DLQ hand-off and
	// never checked it happened. The counter must reflect jobs that actually
	// arrived in the DLQ.
	q, _ := newTestQueue(t)
	if _, err := q.Enqueue("svc", []byte("p"), WithMaxAttempts(1)); err != nil {
		t.Fatal(err)
	}
	_, lease, _ := q.Dequeue("svc", time.Minute)
	if err := q.Nack(lease); err != nil {
		t.Fatal(err)
	}
	if got := q.Stats("svc").DeadLetters; got != 1 {
		t.Fatalf("DeadLetters = %d, want 1", got)
	}
	if got := q.Stats(DeadLetterTopic("svc")).Enqueued; got != 1 {
		t.Fatalf("DLQ Enqueued = %d, want 1 (counter and arrival must agree)", got)
	}
}

func TestDequeueRejectsNonPositiveVisibility(t *testing.T) {
	// Audit: visibility <= 0 issued a lease that was already expired,
	// guaranteeing simultaneous double delivery.
	q, _ := newTestQueue(t)
	if _, err := q.Enqueue("svc", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := q.Dequeue("svc", 0); !errors.Is(err, ErrNonPositiveDuration) {
		t.Errorf("Dequeue(vis=0) = %v, want ErrNonPositiveDuration", err)
	}
	if _, _, err := q.Dequeue("svc", -time.Second); !errors.Is(err, ErrNonPositiveDuration) {
		t.Errorf("Dequeue(vis<0) = %v, want ErrNonPositiveDuration", err)
	}
}

func TestExtendRejectsNonPositiveAndNeverShortens(t *testing.T) {
	// Audit: Extend(lease, 0) instantly revoked a live lease (and could
	// dead-letter a job mid-processing); a small d silently moved the
	// deadline BACKWARD. Extend now only lengthens, and reports the
	// effective deadline so Lease.Deadline staleness is the caller's choice,
	// not a trap.
	q, clk := newTestQueue(t)
	if _, err := q.Enqueue("svc", []byte("x")); err != nil {
		t.Fatal(err)
	}
	_, lease, err := q.Dequeue("svc", time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := q.Extend(lease, 0); !errors.Is(err, ErrNonPositiveDuration) {
		t.Errorf("Extend(0) = %v, want ErrNonPositiveDuration", err)
	}
	// Shorter than remaining: deadline must NOT move backward.
	got, err := q.Extend(lease, time.Second)
	if err != nil {
		t.Fatalf("Extend(1s): %v", err)
	}
	if got.Before(lease.Deadline) {
		t.Errorf("Extend shortened the lease: %v -> %v", lease.Deadline, got)
	}
	// Longer: it lengthens from now.
	clk.Advance(30 * time.Second)
	got2, err := q.Extend(lease, 2*time.Minute)
	if err != nil {
		t.Fatalf("Extend(2m): %v", err)
	}
	if want := clk.Now().Add(2 * time.Minute); !got2.Equal(want) {
		t.Errorf("Extend deadline = %v, want %v", got2, want)
	}
}

func TestPayloadIsInsulatedFromCallers(t *testing.T) {
	// Audit: the payload slice was aliased on enqueue and shared one backing
	// array across redeliveries — a consumer mutating its delivery could
	// corrupt what the next consumer receives.
	q, clk := newTestQueue(t)
	src := []byte("original")
	if _, err := q.Enqueue("svc", src); err != nil {
		t.Fatal(err)
	}
	copy(src, "MUTATED!") // producer reuses its buffer after enqueue

	job1, _, err := q.Dequeue("svc", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(job1.Payload, []byte("original")) {
		t.Fatalf("delivery sees producer mutation: %q", job1.Payload)
	}

	copy(job1.Payload, "SCRIBBLE") // consumer scribbles, then loses the lease
	clk.Advance(11 * time.Second)
	job2, _, err := q.Dequeue("svc", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(job2.Payload, []byte("original")) {
		t.Fatalf("redelivery sees consumer scribble: %q", job2.Payload)
	}
}

func TestConcurrentCloseIsSafeAndIdempotent(t *testing.T) {
	// Audit: topic.close was a racy check-then-act on a channel; two
	// concurrent closers could panic the process.
	clk := newTestClock()
	q := New(WithClock(clk.Now))
	if _, err := q.Enqueue("svc", []byte("x")); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := q.Close(); err != nil {
				t.Errorf("Close: %v", err)
			}
		}()
	}
	wg.Wait() // must not panic
}
