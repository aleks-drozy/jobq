package jobqhttp

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aleks-drozy/jobq"
)

// TestEnqueueOverHTTPSurvivesRestart ties P3's surface to P2's durability
// guarantee: a job enqueued through the HTTP handler, on a queue opened
// with SyncAlways over a real directory, is still dequeuable after the
// process (well — the *jobq.Queue) closes and reopens.
func TestEnqueueOverHTTPSurvivesRestart(t *testing.T) {
	dir := t.TempDir()

	q1, _, err := jobq.Open(dir, jobq.WithSync(jobq.SyncAlways))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	srv := httptest.NewServer(NewHandler(q1))
	c := NewClient(srv.URL)

	id, err := c.Enqueue("orders", []byte("restart-me"), jobq.WithMaxAttempts(5))
	if err != nil {
		t.Fatalf("enqueue over HTTP: %v", err)
	}
	if id == "" {
		t.Fatal("empty job id")
	}

	srv.Close()
	if err := q1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	q2, report, err := jobq.Open(dir, jobq.WithSync(jobq.SyncAlways))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer q2.Close()
	if !report.CleanShutdown {
		t.Errorf("report.CleanShutdown = false, want true (Close wrote CLEANSHUT)")
	}
	if report.JobsRestored != 1 {
		t.Errorf("report.JobsRestored = %d, want 1", report.JobsRestored)
	}

	srv2 := httptest.NewServer(NewHandler(q2))
	defer srv2.Close()
	c2 := NewClient(srv2.URL)

	job, lease, err := c2.Dequeue("orders", time.Minute)
	if err != nil {
		t.Fatalf("dequeue after restart: %v", err)
	}
	if string(job.Payload) != "restart-me" {
		t.Errorf("payload = %q, want restart-me", job.Payload)
	}
	if err := c2.Ack(lease); err != nil {
		t.Fatalf("ack after restart: %v", err)
	}
}
