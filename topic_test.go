package jobq

import (
	"errors"
	"sync"
	"testing"
	"time"
)

// testClock is a manually advanced clock. Every time-dependent behaviour in
// jobq reads the clock through a function field, so tests never sleep and
// lease expiry is deterministic.
type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func newTestClock() *testClock {
	return &testClock{now: time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)}
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// newTestTopic returns a topic driven by a manual clock, plus the clock.
// Dead-lettered jobs are captured in the returned slice pointer.
func newTestTopic(t *testing.T) (*topic, *testClock, *[]*Job) {
	t.Helper()
	clk := newTestClock()
	var mu sync.Mutex
	dead := &[]*Job{}
	tp := newTopic("jobs", clk.Now, func(j *Job) {
		mu.Lock()
		*dead = append(*dead, j)
		mu.Unlock()
	})
	t.Cleanup(tp.close)
	return tp, clk, dead
}

func mustEnqueue(t *testing.T, tp *topic, payload string, opts EnqueueOptions) string {
	t.Helper()
	id, err := tp.enqueue([]byte(payload), opts.normalize())
	if err != nil {
		t.Fatalf("enqueue(%q): %v", payload, err)
	}
	return id
}

func mustDequeue(t *testing.T, tp *topic, vis time.Duration) (*Job, Lease) {
	t.Helper()
	job, lease, err := tp.dequeue(vis)
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	return job, lease
}

func TestDequeueReturnsJobsInFIFOOrder(t *testing.T) {
	tp, _, _ := newTestTopic(t)
	mustEnqueue(t, tp, "first", EnqueueOptions{})
	mustEnqueue(t, tp, "second", EnqueueOptions{})

	got1, _ := mustDequeue(t, tp, time.Minute)
	got2, _ := mustDequeue(t, tp, time.Minute)

	if string(got1.Payload) != "first" || string(got2.Payload) != "second" {
		t.Fatalf("FIFO violated: got %q then %q", got1.Payload, got2.Payload)
	}
	if got1.Attempt != 1 {
		t.Errorf("first delivery Attempt = %d, want 1", got1.Attempt)
	}
}

func TestDequeueOnEmptyTopicReturnsErrNoJob(t *testing.T) {
	tp, _, _ := newTestTopic(t)
	if _, _, err := tp.dequeue(time.Minute); !errors.Is(err, ErrNoJob) {
		t.Fatalf("dequeue on empty topic = %v, want ErrNoJob", err)
	}
}

func TestDelayedJobIsInvisibleUntilNotBefore(t *testing.T) {
	tp, clk, _ := newTestTopic(t)
	mustEnqueue(t, tp, "later", EnqueueOptions{Delay: 30 * time.Second})

	if _, _, err := tp.dequeue(time.Minute); !errors.Is(err, ErrNoJob) {
		t.Fatalf("delayed job was visible early: %v", err)
	}
	clk.Advance(31 * time.Second)
	job, _ := mustDequeue(t, tp, time.Minute)
	if string(job.Payload) != "later" {
		t.Fatalf("payload = %q, want %q", job.Payload, "later")
	}
}

func TestAckRemovesJobPermanently(t *testing.T) {
	tp, clk, _ := newTestTopic(t)
	mustEnqueue(t, tp, "work", EnqueueOptions{})
	_, lease := mustDequeue(t, tp, time.Minute)

	if err := tp.ack(lease.ID); err != nil {
		t.Fatalf("ack: %v", err)
	}
	clk.Advance(2 * time.Minute) // past the visibility timeout
	if _, _, err := tp.dequeue(time.Minute); !errors.Is(err, ErrNoJob) {
		t.Fatal("acked job was redelivered after its lease expired")
	}
	if st := tp.stats(); st.Acked != 1 || st.InFlight != 0 || st.Ready != 0 {
		t.Errorf("stats after ack = %+v", st)
	}
}

func TestAckWithUnknownLeaseFails(t *testing.T) {
	tp, _, _ := newTestTopic(t)
	if err := tp.ack("nope"); !errors.Is(err, ErrUnknownLease) {
		t.Fatalf("ack(unknown) = %v, want ErrUnknownLease", err)
	}
}

func TestNackRequeuesImmediatelyAndCountsAttempt(t *testing.T) {
	tp, _, _ := newTestTopic(t)
	mustEnqueue(t, tp, "retry-me", EnqueueOptions{})
	_, lease := mustDequeue(t, tp, time.Minute)

	if err := tp.nack(lease.ID); err != nil {
		t.Fatalf("nack: %v", err)
	}
	job, _ := mustDequeue(t, tp, time.Minute)
	if job.Attempt != 2 {
		t.Errorf("Attempt after nack = %d, want 2", job.Attempt)
	}
}

func TestExpiredLeaseRedeliversJob(t *testing.T) {
	tp, clk, _ := newTestTopic(t)
	mustEnqueue(t, tp, "slow", EnqueueOptions{})
	_, lease := mustDequeue(t, tp, 10*time.Second)

	clk.Advance(11 * time.Second)
	job, _ := mustDequeue(t, tp, 10*time.Second)
	if job.Attempt != 2 {
		t.Errorf("Attempt after expiry = %d, want 2", job.Attempt)
	}
	// The stale lease must no longer settle the job.
	if err := tp.ack(lease.ID); !errors.Is(err, ErrUnknownLease) {
		t.Errorf("ack(expired lease) = %v, want ErrUnknownLease", err)
	}
}

func TestExtendPushesDeadlineOut(t *testing.T) {
	tp, clk, _ := newTestTopic(t)
	mustEnqueue(t, tp, "long", EnqueueOptions{})
	_, lease := mustDequeue(t, tp, 10*time.Second)

	if err := tp.extend(lease.ID, time.Minute); err != nil {
		t.Fatalf("extend: %v", err)
	}
	clk.Advance(30 * time.Second) // past the ORIGINAL deadline only
	if _, _, err := tp.dequeue(time.Second); !errors.Is(err, ErrNoJob) {
		t.Fatal("extended lease expired at its original deadline")
	}
	if err := tp.ack(lease.ID); err != nil {
		t.Errorf("ack after extend: %v", err)
	}
}

func TestJobIsDeadLetteredAfterMaxAttempts(t *testing.T) {
	tp, _, dead := newTestTopic(t)
	mustEnqueue(t, tp, "poison", EnqueueOptions{MaxAttempts: 2})

	for i := 0; i < 2; i++ {
		_, lease := mustDequeue(t, tp, time.Minute)
		if err := tp.nack(lease.ID); err != nil {
			t.Fatalf("nack %d: %v", i, err)
		}
	}
	if _, _, err := tp.dequeue(time.Minute); !errors.Is(err, ErrNoJob) {
		t.Fatal("job past MaxAttempts is still deliverable")
	}
	if len(*dead) != 1 {
		t.Fatalf("dead-lettered %d jobs, want 1", len(*dead))
	}
	if got := (*dead)[0]; string(got.Payload) != "poison" || got.DeadLetteredAt.IsZero() {
		t.Errorf("dead job = %+v, want payload %q and a DeadLetteredAt", got, "poison")
	}
	if st := tp.stats(); st.DeadLetters != 1 {
		t.Errorf("stats.DeadLetters = %d, want 1", st.DeadLetters)
	}
}

func TestConcurrentConsumersNeverShareAJob(t *testing.T) {
	tp, _, _ := newTestTopic(t)
	const jobs = 200
	for i := 0; i < jobs; i++ {
		mustEnqueue(t, tp, "x", EnqueueOptions{})
	}

	var mu sync.Mutex
	seen := map[string]int{}
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				job, lease, err := tp.dequeue(time.Minute)
				if errors.Is(err, ErrNoJob) {
					return
				}
				if err != nil {
					return
				}
				mu.Lock()
				seen[job.ID]++
				mu.Unlock()
				_ = tp.ack(lease.ID)
			}
		}()
	}
	wg.Wait()

	if len(seen) != jobs {
		t.Fatalf("delivered %d distinct jobs, want %d", len(seen), jobs)
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("job %s delivered %d times concurrently, want exactly 1", id, n)
		}
	}
}
