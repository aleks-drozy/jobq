package jobqhttp

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aleks-drozy/jobq"
)

// TestPeekParity mirrors roundtrip_test.go's discipline for the one method
// that isn't part of jobq.Client: it runs the same assertions against a
// local *jobq.Queue and against a *Client talking to a live httptest
// server wrapping an identical queue, so the HTTP peek route can't quietly
// diverge from the engine primitive it wraps. jobq.Queue.Peek and
// Client.Peek have deliberately different signatures (the local call cannot
// fail and reports no Truncated flag; see queue.go's Peek doc), so this
// test drives each through its own path rather than a shared interface.
func TestPeekParity(t *testing.T) {
	t.Run("local", func(t *testing.T) {
		q := jobq.New()
		defer q.Close()

		if _, err := q.Enqueue("peek-parity", []byte("first")); err != nil {
			t.Fatal(err)
		}
		if _, err := q.Enqueue("peek-parity", []byte("second")); err != nil {
			t.Fatal(err)
		}

		ready, inflight, err := q.Peek("peek-parity", 10, false)
		if err != nil {
			t.Fatal(err)
		}
		if len(ready) != 2 {
			t.Fatalf("ready = %d, want 2", len(ready))
		}
		if string(ready[0].Payload) != "first" || string(ready[1].Payload) != "second" {
			t.Fatalf("FIFO order violated: %q, %q", ready[0].Payload, ready[1].Payload)
		}
		if len(inflight) != 0 {
			t.Fatalf("inflight = %d, want 0 (includeInflight=false)", len(inflight))
		}
		if ready[0].Attempts != 0 {
			t.Fatalf("Attempts = %d, want 0 — peek must not consume", ready[0].Attempts)
		}

		ready, _, err = q.Peek("peek-parity", 1, false)
		if err != nil {
			t.Fatal(err)
		}
		if len(ready) != 1 {
			t.Fatalf("limited ready = %d, want 1", len(ready))
		}

		ready, inflight, err = q.Peek("peek-parity-never-used", 10, true)
		if err != nil {
			t.Fatalf("unknown topic err = %v, want nil", err)
		}
		if len(ready) != 0 || len(inflight) != 0 {
			t.Fatalf("unknown topic = %v, %v, want empty", ready, inflight)
		}
	})

	t.Run("http", func(t *testing.T) {
		q := jobq.New()
		defer q.Close()
		srv := httptest.NewServer(NewHandler(q))
		defer srv.Close()
		c := NewClient(srv.URL)

		if _, err := c.Enqueue("peek-parity", []byte("first")); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Enqueue("peek-parity", []byte("second")); err != nil {
			t.Fatal(err)
		}

		ready, inflight, truncated, err := c.Peek("peek-parity", 10, false)
		if err != nil {
			t.Fatal(err)
		}
		if len(ready) != 2 {
			t.Fatalf("ready = %d, want 2", len(ready))
		}
		if string(ready[0].Payload) != "first" || string(ready[1].Payload) != "second" {
			t.Fatalf("FIFO order violated: %q, %q", ready[0].Payload, ready[1].Payload)
		}
		if len(inflight) != 0 {
			t.Fatalf("inflight = %d, want 0 (includeInflight=false)", len(inflight))
		}
		if truncated {
			t.Fatal("truncated = true, want false")
		}
		if ready[0].Attempts != 0 {
			t.Fatalf("Attempts = %d, want 0 — peek must not consume", ready[0].Attempts)
		}

		ready, _, truncated, err = c.Peek("peek-parity", 1, false)
		if err != nil {
			t.Fatal(err)
		}
		if len(ready) != 1 {
			t.Fatalf("limited ready = %d, want 1", len(ready))
		}
		if !truncated {
			t.Fatal("truncated = false, want true (2 jobs, limit 1)")
		}

		ready, inflight, _, err = c.Peek("peek-parity-never-used", 10, true)
		if err != nil {
			t.Fatalf("unknown topic err = %v, want nil", err)
		}
		if len(ready) != 0 || len(inflight) != 0 {
			t.Fatalf("unknown topic = %v, %v, want empty", ready, inflight)
		}
	})
}

func TestPeekIncludesInflightOverHTTP(t *testing.T) {
	q := jobq.New()
	defer q.Close()
	srv := httptest.NewServer(NewHandler(q))
	defer srv.Close()
	c := NewClient(srv.URL)

	if _, err := c.Enqueue("peek-inflight-http", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.Dequeue("peek-inflight-http", time.Minute); err != nil {
		t.Fatal(err)
	}

	ready, inflight, _, err := c.Peek("peek-inflight-http", 10, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(ready) != 0 {
		t.Fatalf("ready = %d, want 0", len(ready))
	}
	if len(inflight) != 1 {
		t.Fatalf("inflight = %d, want 1", len(inflight))
	}
	if inflight[0].Attempts != 1 {
		t.Fatalf("Attempts = %d, want 1", inflight[0].Attempts)
	}
}
