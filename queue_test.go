package jobq

import (
	"errors"
	"testing"
	"time"
)

func newTestQueue(t *testing.T) (*Queue, *testClock) {
	t.Helper()
	clk := newTestClock()
	q := New(WithClock(clk.Now))
	t.Cleanup(func() { _ = q.Close() })
	return q, clk
}

func TestEnqueueRejectsEmptyTopic(t *testing.T) {
	q, _ := newTestQueue(t)
	if _, err := q.Enqueue("", []byte("x")); !errors.Is(err, ErrEmptyTopic) {
		t.Fatalf("Enqueue(\"\") = %v, want ErrEmptyTopic", err)
	}
}

func TestTopicsAreIsolated(t *testing.T) {
	q, _ := newTestQueue(t)
	if _, err := q.Enqueue("alpha", []byte("a")); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Enqueue("beta", []byte("b")); err != nil {
		t.Fatal(err)
	}

	job, _, err := q.Dequeue("alpha", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if string(job.Payload) != "a" || job.Topic != "alpha" {
		t.Fatalf("got %q from %q, want %q from alpha", job.Payload, job.Topic, "a")
	}
	// beta must still hold its own job.
	if job, _, err := q.Dequeue("beta", time.Minute); err != nil || string(job.Payload) != "b" {
		t.Fatalf("beta dequeue = %v, %v", job, err)
	}
}

func TestDequeueFromUnknownTopicIsErrNoJobNotPanic(t *testing.T) {
	q, _ := newTestQueue(t)
	if _, _, err := q.Dequeue("never-used", time.Minute); !errors.Is(err, ErrNoJob) {
		t.Fatalf("Dequeue(unknown) = %v, want ErrNoJob", err)
	}
}

func TestExhaustedJobLandsInDeadLetterTopic(t *testing.T) {
	q, _ := newTestQueue(t)
	if _, err := q.Enqueue("mail", []byte("poison"), WithMaxAttempts(2)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		_, lease, err := q.Dequeue("mail", time.Minute)
		if err != nil {
			t.Fatalf("dequeue %d: %v", i, err)
		}
		if err := q.Nack(lease); err != nil {
			t.Fatalf("nack %d: %v", i, err)
		}
	}

	// The DLQ is itself a topic, so it is consumed with the same API.
	dead, lease, err := q.Dequeue(DeadLetterTopic("mail"), time.Minute)
	if err != nil {
		t.Fatalf("dequeue from DLQ: %v", err)
	}
	if string(dead.Payload) != "poison" {
		t.Errorf("DLQ payload = %q, want %q", dead.Payload, "poison")
	}
	if dead.DeadLetteredAt.IsZero() {
		t.Error("DLQ job has no DeadLetteredAt")
	}
	// Evidence of the original failure survives being consumed from the DLQ,
	// which is why it lives in its own field rather than in Attempts.
	if dead.DeadLetteredAttempts != 2 {
		t.Errorf("DeadLetteredAttempts = %d, want 2", dead.DeadLetteredAttempts)
	}
	if dead.Attempt != 1 {
		t.Errorf("DLQ delivery Attempt = %d, want 1 (fresh budget in the DLQ)", dead.Attempt)
	}
	if err := q.Ack(lease); err != nil {
		t.Errorf("ack DLQ job: %v", err)
	}
}

func TestStatsReportPerTopicCounts(t *testing.T) {
	q, _ := newTestQueue(t)
	for i := 0; i < 3; i++ {
		if _, err := q.Enqueue("work", []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	_, lease, err := q.Dequeue("work", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Ack(lease); err != nil {
		t.Fatal(err)
	}

	st := q.Stats("work")
	if st.Enqueued != 3 || st.Acked != 1 || st.Ready != 2 || st.InFlight != 0 {
		t.Errorf("Stats = %+v, want Enqueued 3 / Acked 1 / Ready 2 / InFlight 0", st)
	}
}

func TestOperationsAfterCloseReturnErrClosed(t *testing.T) {
	clk := newTestClock()
	q := New(WithClock(clk.Now))
	if _, err := q.Enqueue("t", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := q.Enqueue("t", []byte("y")); !errors.Is(err, ErrClosed) {
		t.Errorf("Enqueue after Close = %v, want ErrClosed", err)
	}
	if _, _, err := q.Dequeue("t", time.Minute); !errors.Is(err, ErrClosed) {
		t.Errorf("Dequeue after Close = %v, want ErrClosed", err)
	}
	if err := q.Close(); err != nil {
		t.Errorf("second Close = %v, want nil (idempotent)", err)
	}
}

func TestLeaseFromWrongTopicIsRejected(t *testing.T) {
	q, _ := newTestQueue(t)
	if _, err := q.Enqueue("alpha", []byte("a")); err != nil {
		t.Fatal(err)
	}
	_, lease, err := q.Dequeue("alpha", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	forged := lease
	forged.Topic = "beta"
	if err := q.Ack(forged); !errors.Is(err, ErrUnknownLease) {
		t.Errorf("Ack(forged topic) = %v, want ErrUnknownLease", err)
	}
}
