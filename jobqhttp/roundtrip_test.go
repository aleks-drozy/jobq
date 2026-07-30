package jobqhttp

import (
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aleks-drozy/jobq"
)

// This is the headline test: one behaviour suite, run twice against the
// jobq.Client interface — once with *jobq.Queue directly, once with a
// *jobqhttp.Client pointed at a live httptest server wrapping an identical
// in-memory queue. Any divergence in sentinel errors, attempt counting, or
// DLQ routing fails the build; that is what makes "the HTTP surface is the
// same queue" a checked claim rather than a README sentence.

type roundtripCase struct {
	name string
	run  func(t *testing.T, c jobq.Client)
}

var roundtripCases = []roundtripCase{
	{"enqueue dequeue ack", func(t *testing.T, c jobq.Client) {
		if _, err := c.Enqueue("rt-basic", []byte("payload")); err != nil {
			t.Fatal(err)
		}
		job, lease, err := c.Dequeue("rt-basic", time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if string(job.Payload) != "payload" {
			t.Errorf("payload = %q, want payload", job.Payload)
		}
		if job.Attempt != 1 {
			t.Errorf("Attempt = %d, want 1", job.Attempt)
		}
		if err := c.Ack(lease); err != nil {
			t.Fatal(err)
		}
	}},
	{"dequeue empty topic is ErrNoJob", func(t *testing.T, c jobq.Client) {
		_, _, err := c.Dequeue("rt-never-used", time.Minute)
		if !errors.Is(err, jobq.ErrNoJob) {
			t.Fatalf("err = %v, want ErrNoJob", err)
		}
	}},
	{"enqueue empty topic is ErrEmptyTopic", func(t *testing.T, c jobq.Client) {
		if _, err := c.Enqueue("", []byte("x")); !errors.Is(err, jobq.ErrEmptyTopic) {
			t.Fatalf("err = %v, want ErrEmptyTopic", err)
		}
	}},
	{"enqueue reserved topic is ErrReservedTopic", func(t *testing.T, c jobq.Client) {
		if _, err := c.Enqueue("rt-reserved.dlq", []byte("x")); !errors.Is(err, jobq.ErrReservedTopic) {
			t.Fatalf("err = %v, want ErrReservedTopic", err)
		}
	}},
	{"ack unknown lease is ErrUnknownLease", func(t *testing.T, c jobq.Client) {
		if err := c.Ack(jobq.Lease{ID: "forged", Topic: "rt-basic"}); !errors.Is(err, jobq.ErrUnknownLease) {
			t.Fatalf("err = %v, want ErrUnknownLease", err)
		}
	}},
	{"double ack is ErrUnknownLease", func(t *testing.T, c jobq.Client) {
		if _, err := c.Enqueue("rt-double-ack", []byte("x")); err != nil {
			t.Fatal(err)
		}
		_, lease, err := c.Dequeue("rt-double-ack", time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if err := c.Ack(lease); err != nil {
			t.Fatal(err)
		}
		if err := c.Ack(lease); !errors.Is(err, jobq.ErrUnknownLease) {
			t.Fatalf("second ack = %v, want ErrUnknownLease", err)
		}
	}},
	{"extend non-positive is ErrNonPositiveDuration", func(t *testing.T, c jobq.Client) {
		if _, err := c.Enqueue("rt-extend", []byte("x")); err != nil {
			t.Fatal(err)
		}
		_, lease, err := c.Dequeue("rt-extend", time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.Extend(lease, 0); !errors.Is(err, jobq.ErrNonPositiveDuration) {
			t.Fatalf("err = %v, want ErrNonPositiveDuration", err)
		}
	}},
	{"nack retries then dead-letters at attempt budget", func(t *testing.T, c jobq.Client) {
		if _, err := c.Enqueue("rt-dlq-src", []byte("poison"), jobq.WithMaxAttempts(2)); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 2; i++ {
			_, lease, err := c.Dequeue("rt-dlq-src", time.Minute)
			if err != nil {
				t.Fatalf("dequeue %d: %v", i, err)
			}
			if err := c.Nack(lease); err != nil {
				t.Fatalf("nack %d: %v", i, err)
			}
		}
		dead, lease, err := c.Dequeue(jobq.DeadLetterTopic("rt-dlq-src"), time.Minute)
		if err != nil {
			t.Fatalf("dequeue DLQ: %v", err)
		}
		if string(dead.Payload) != "poison" {
			t.Errorf("DLQ payload = %q, want poison", dead.Payload)
		}
		if dead.DeadLetteredAttempts != 2 {
			t.Errorf("DeadLetteredAttempts = %d, want 2", dead.DeadLetteredAttempts)
		}
		if dead.Attempt != 1 {
			t.Errorf("DLQ delivery Attempt = %d, want 1 (fresh budget)", dead.Attempt)
		}
		if err := c.Ack(lease); err != nil {
			t.Fatal(err)
		}
	}},
	{"extend lengthens the deadline", func(t *testing.T, c jobq.Client) {
		if _, err := c.Enqueue("rt-extend-ok", []byte("x")); err != nil {
			t.Fatal(err)
		}
		_, lease, err := c.Dequeue("rt-extend-ok", time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		newDeadline, err := c.Extend(lease, 2*time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if !newDeadline.After(lease.Deadline) {
			t.Errorf("extended deadline %v not after original %v", newDeadline, lease.Deadline)
		}
	}},
}

func TestRoundtripLocalQueue(t *testing.T) {
	q := jobq.New()
	defer q.Close()
	for _, tc := range roundtripCases {
		t.Run(tc.name, func(t *testing.T) { tc.run(t, q) })
	}
}

func TestRoundtripHTTPClient(t *testing.T) {
	q := jobq.New()
	defer q.Close()
	srv := httptest.NewServer(NewHandler(q))
	defer srv.Close()
	c := NewClient(srv.URL)
	for _, tc := range roundtripCases {
		t.Run(tc.name, func(t *testing.T) { tc.run(t, c) })
	}
}
