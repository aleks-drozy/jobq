package jobqhttp

import (
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aleks-drozy/jobq"
)

func newTestClientPair(t *testing.T, opts ...HandlerOption) (*jobq.Queue, *Client) {
	t.Helper()
	q := jobq.New()
	t.Cleanup(func() { _ = q.Close() })
	srv := httptest.NewServer(NewHandler(q, opts...))
	t.Cleanup(srv.Close)
	return q, NewClient(srv.URL)
}

func TestClientEnqueueDequeueAck(t *testing.T) {
	_, c := newTestClientPair(t)
	id, err := c.Enqueue("emails", []byte("hi"), jobq.WithMaxAttempts(3))
	if err != nil {
		t.Fatal(err)
	}
	if id == "" {
		t.Fatal("empty id")
	}
	job, lease, err := c.Dequeue("emails", 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if string(job.Payload) != "hi" {
		t.Errorf("payload = %q, want hi", job.Payload)
	}
	if err := c.Ack(lease); err != nil {
		t.Fatal(err)
	}
}

func TestClientDequeueNoJobIsErrNoJob(t *testing.T) {
	_, c := newTestClientPair(t)
	_, _, err := c.Dequeue("never-used", time.Second)
	if !errors.Is(err, jobq.ErrNoJob) {
		t.Fatalf("err = %v, want ErrNoJob", err)
	}
}

func TestClientAckUnknownLeaseIsErrUnknownLease(t *testing.T) {
	_, c := newTestClientPair(t)
	err := c.Ack(jobq.Lease{ID: "nope", Topic: "work"})
	if !errors.Is(err, jobq.ErrUnknownLease) {
		t.Fatalf("err = %v, want ErrUnknownLease", err)
	}
}

func TestClientExtendNonPositiveIsErrNonPositiveDuration(t *testing.T) {
	_, c := newTestClientPair(t)
	if _, err := c.Enqueue("work", []byte("x")); err != nil {
		t.Fatal(err)
	}
	_, lease, err := c.Dequeue("work", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Extend(lease, 0); !errors.Is(err, jobq.ErrNonPositiveDuration) {
		t.Fatalf("err = %v, want ErrNonPositiveDuration", err)
	}
}

func TestClientNackAndExhaustion(t *testing.T) {
	_, c := newTestClientPair(t)
	if _, err := c.Enqueue("mail", []byte("poison"), jobq.WithMaxAttempts(2)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		_, lease, err := c.Dequeue("mail", time.Minute)
		if err != nil {
			t.Fatalf("dequeue %d: %v", i, err)
		}
		if err := c.Nack(lease); err != nil {
			t.Fatalf("nack %d: %v", i, err)
		}
	}
	dead, lease, err := c.Dequeue(jobq.DeadLetterTopic("mail"), time.Minute)
	if err != nil {
		t.Fatalf("dequeue DLQ: %v", err)
	}
	if string(dead.Payload) != "poison" {
		t.Errorf("DLQ payload = %q, want poison", dead.Payload)
	}
	if err := c.Ack(lease); err != nil {
		t.Fatal(err)
	}
}

func TestClientTopicsAndStats(t *testing.T) {
	_, c := newTestClientPair(t)
	if _, err := c.Enqueue("alpha", []byte("a")); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Enqueue("beta", []byte("b")); err != nil {
		t.Fatal(err)
	}
	topics, err := c.Topics()
	if err != nil {
		t.Fatal(err)
	}
	if len(topics) != 2 {
		t.Fatalf("topics = %v, want 2 entries", topics)
	}
	st, err := c.TopicStats("alpha")
	if err != nil {
		t.Fatal(err)
	}
	if st.Enqueued != 1 || st.Ready != 1 {
		t.Errorf("stats = %+v, want Enqueued=1 Ready=1", st)
	}
	all, err := c.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 {
		t.Fatalf("Stats() = %v, want 2 topics", all)
	}
}

func TestClientDequeueWaitBlocksUntilEnqueue(t *testing.T) {
	q, c := newTestClientPair(t)
	done := make(chan struct{})
	var job *jobq.Job
	var derr error
	go func() {
		job, _, derr = c.DequeueWait("delayed", 30*time.Second, 2*time.Second)
		close(done)
	}()

	time.Sleep(100 * time.Millisecond) // let the long-poll start waiting
	if _, err := q.Enqueue("delayed", []byte("arrived")); err != nil {
		t.Fatal(err)
	}

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("DequeueWait did not return after enqueue")
	}
	if derr != nil {
		t.Fatalf("DequeueWait error: %v", derr)
	}
	if job == nil || string(job.Payload) != "arrived" {
		t.Fatalf("job = %+v", job)
	}
}

func TestClientDequeueWaitTimesOutToErrNoJob(t *testing.T) {
	_, c := newTestClientPair(t)
	start := time.Now()
	_, _, err := c.DequeueWait("never-arrives", time.Minute, 200*time.Millisecond)
	elapsed := time.Since(start)
	if !errors.Is(err, jobq.ErrNoJob) {
		t.Fatalf("err = %v, want ErrNoJob", err)
	}
	if elapsed < 150*time.Millisecond {
		t.Errorf("returned too early: %v", elapsed)
	}
}
