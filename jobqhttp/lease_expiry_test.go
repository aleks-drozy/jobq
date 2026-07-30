package jobqhttp

import (
	"errors"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/aleks-drozy/jobq"
)

// testClock is a manually advanced clock, mirroring the one in the jobq
// package's own tests: lease expiry must be deterministic here too, not
// driven by sleeping.
type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func newTestClock() *testClock {
	return &testClock{now: time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC)}
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

// TestLeaseExpiryOverHTTP drives redelivery with a manual clock rather than
// a real short visibility + sleep: a job dequeued and never settled becomes
// redeliverable the instant the clock crosses its deadline, with Attempt
// incremented — the same contract as the in-process API, observed through
// the HTTP surface.
func TestLeaseExpiryOverHTTP(t *testing.T) {
	clk := newTestClock()
	q := jobq.New(jobq.WithClock(clk.Now))
	defer q.Close()
	srv := httptest.NewServer(NewHandler(q))
	defer srv.Close()
	c := NewClient(srv.URL)

	if _, err := c.Enqueue("orders", []byte("x"), jobq.WithMaxAttempts(5)); err != nil {
		t.Fatal(err)
	}

	job1, _, err := c.Dequeue("orders", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if job1.Attempt != 1 {
		t.Fatalf("first delivery Attempt = %d, want 1", job1.Attempt)
	}

	// Never settle job1. Advance the clock past its lease deadline.
	clk.Advance(11 * time.Second)

	job2, lease2, err := c.Dequeue("orders", 10*time.Second)
	if err != nil {
		t.Fatalf("redelivery dequeue: %v", err)
	}
	if job2.ID != job1.ID {
		t.Fatalf("redelivered job ID = %s, want %s", job2.ID, job1.ID)
	}
	if job2.Attempt != 2 {
		t.Errorf("redelivery Attempt = %d, want 2", job2.Attempt)
	}

	if err := c.Ack(lease2); err != nil {
		t.Fatal(err)
	}
	// The now-settled lease must not settle a second time: this is the
	// "unique settlement" property from the README, observed over HTTP.
	if !errors.Is(c.Ack(lease2), jobq.ErrUnknownLease) {
		t.Errorf("re-acking an already-settled lease should be ErrUnknownLease")
	}
}
