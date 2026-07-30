package jobqhttp

import (
	"errors"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aleks-drozy/jobq"
)

// TestConservationUnderConcurrentHTTPWorkers reuses the P1 conservation
// property (every job that entered the queue ends up in exactly one place)
// but drives it through concurrent HTTP workers instead of in-process
// calls, under -race. Each job has MaxAttempts(1): an even-indexed job is
// acked directly, an odd-indexed one is nacked once, which immediately
// dead-letters it (no retry loop needed to reach the DLQ), so no lease is
// ever contended between two goroutines and duplicate delivery does not
// enter into this test — see queue_test.go / invariant_test.go for that
// property, already covered against the in-process API.
func TestConservationUnderConcurrentHTTPWorkers(t *testing.T) {
	const (
		producers       = 8
		jobsPerProducer = 50
		consumers       = 8
		dlqConsumers    = 4
		topic           = "work"
	)
	total := producers * jobsPerProducer

	q := jobq.New()
	defer q.Close()
	srv := httptest.NewServer(NewHandler(q))
	defer srv.Close()

	var wg sync.WaitGroup

	// Producers: each enqueues its slice of [0, total) as the payload, so
	// consumers can classify even/odd without needing the queue-assigned ID.
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			c := NewClient(srv.URL)
			for i := 0; i < jobsPerProducer; i++ {
				idx := p*jobsPerProducer + i
				if _, err := c.Enqueue(topic, []byte(strconv.Itoa(idx)), jobq.WithMaxAttempts(1)); err != nil {
					t.Errorf("enqueue %d: %v", idx, err)
				}
			}
		}(p)
	}
	wg.Wait()

	var mu sync.Mutex
	acked := map[string]bool{}
	recordAck := func(payload string) {
		mu.Lock()
		defer mu.Unlock()
		if acked[payload] {
			t.Errorf("payload %s acked twice", payload)
		}
		acked[payload] = true
	}

	stop := make(chan struct{})
	var workers sync.WaitGroup

	// Main-topic consumers: even -> ack, odd -> nack (dead-letters at
	// MaxAttempts(1)).
	for i := 0; i < consumers; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			c := NewClient(srv.URL)
			for {
				select {
				case <-stop:
					return
				default:
				}
				job, lease, err := c.Dequeue(topic, 30*time.Second)
				if errors.Is(err, jobq.ErrNoJob) {
					time.Sleep(2 * time.Millisecond)
					continue
				}
				if err != nil {
					t.Errorf("dequeue: %v", err)
					return
				}
				n, convErr := strconv.Atoi(string(job.Payload))
				if convErr != nil {
					t.Errorf("bad payload %q: %v", job.Payload, convErr)
					continue
				}
				if n%2 == 0 {
					if err := c.Ack(lease); err != nil {
						t.Errorf("ack: %v", err)
						continue
					}
					recordAck(string(job.Payload))
				} else {
					if err := c.Nack(lease); err != nil {
						t.Errorf("nack: %v", err)
					}
				}
			}
		}()
	}

	// DLQ consumers: drain and ack everything that lands in work.dlq.
	dlqTopic := jobq.DeadLetterTopic(topic)
	for i := 0; i < dlqConsumers; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			c := NewClient(srv.URL)
			for {
				select {
				case <-stop:
					return
				default:
				}
				job, lease, err := c.Dequeue(dlqTopic, 30*time.Second)
				if errors.Is(err, jobq.ErrNoJob) {
					time.Sleep(2 * time.Millisecond)
					continue
				}
				if err != nil {
					t.Errorf("dlq dequeue: %v", err)
					return
				}
				if err := c.Ack(lease); err != nil {
					t.Errorf("dlq ack: %v", err)
					continue
				}
				recordAck(string(job.Payload))
			}
		}()
	}

	deadline := time.Now().Add(20 * time.Second)
	for {
		mu.Lock()
		n := len(acked)
		mu.Unlock()
		if n >= total {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out: acked %d/%d", n, total)
		}
		time.Sleep(10 * time.Millisecond)
	}
	close(stop)
	workers.Wait()

	if len(acked) != total {
		t.Fatalf("acked %d jobs, want %d", len(acked), total)
	}

	st := q.Stats(topic)
	dlqSt := q.Stats(dlqTopic)
	if st.Enqueued != total {
		t.Errorf("Enqueued = %d, want %d", st.Enqueued, total)
	}
	if st.Ready != 0 || st.InFlight != 0 || dlqSt.Ready != 0 || dlqSt.InFlight != 0 {
		t.Errorf("leftover jobs: main Ready=%d InFlight=%d, dlq Ready=%d InFlight=%d",
			st.Ready, st.InFlight, dlqSt.Ready, dlqSt.InFlight)
	}
	// Conservation: every enqueued job is accounted for as either an
	// immediate ack on the main topic or an ack after dead-lettering.
	if st.Acked+st.DeadLetters != total {
		t.Errorf("Acked(%d) + DeadLetters(%d) = %d, want %d", st.Acked, st.DeadLetters, st.Acked+st.DeadLetters, total)
	}
	if dlqSt.Acked != st.DeadLetters {
		t.Errorf("dlq Acked = %d, want %d (= main DeadLetters)", dlqSt.Acked, st.DeadLetters)
	}
}
