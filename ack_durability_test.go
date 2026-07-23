package jobq

import (
	"testing"
	"time"
)

// Rule 2 of the durability contract - "an acked job never comes back" - is
// only true if the ack record is durable before Ack returns success. The
// crash harness caught the violation as a flaky GHOST: under SyncAlways the
// enqueue record was fsync-gated but the ack record was not, so a cold kill
// between Ack returning and the next group fsync replayed the (durable)
// enqueue without the (lost) ack, and the acked job was delivered again.
//
// This is the deterministic version of that finding: the ack record must be
// appended gated, and ack() must not report success until the WAL's waiter
// for that record has yielded.
func TestAckDoesNotReturnBeforeItsRecordIsDurable(t *testing.T) {
	clk := newTestClock()

	type logged struct {
		rec   walRecord
		gated bool
	}
	var (
		ackWait = make(waiter, 1) // held by the test: the pending fsync
		seen    []logged
	)
	tp := newTopic(topicConfig{
		name: "jobs",
		now:  clk.Now,
		deadLetter: func(j *Job) bool { return false },
		logRec: func(rec walRecord, gated bool) waiter {
			seen = append(seen, logged{rec, gated})
			if rec.Type == recAck {
				return ackWait // yields only when the test says the fsync landed
			}
			done := make(waiter, 1)
			done <- nil
			return done
		},
	})
	defer tp.close()

	if _, err := tp.enqueue([]byte("x"), EnqueueOptions{MaxAttempts: 3}); err != nil {
		t.Fatal(err)
	}
	_, lease, err := tp.dequeue(time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	ackDone := make(chan error, 1)
	go func() { ackDone <- tp.ack(lease.ID) }()

	// The fsync has not happened (ackWait is still held): Ack must not have
	// reported success yet.
	select {
	case err := <-ackDone:
		t.Fatalf("ack returned (err=%v) before its WAL record was durable", err)
	case <-time.After(50 * time.Millisecond):
	}

	ackWait <- nil // fsync lands
	select {
	case err := <-ackDone:
		if err != nil {
			t.Fatalf("ack failed after durable write: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ack never returned after its record became durable")
	}

	for _, l := range seen {
		if l.rec.Type == recAck && !l.gated {
			t.Fatal("recAck was appended ungated - under SyncAlways an ungated " +
				"ack rides the next group commit, and a crash in that window " +
				"resurrects an acked job")
		}
	}
}
