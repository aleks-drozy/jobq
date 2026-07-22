package jobq

import (
	"errors"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"
)

// The conservation invariant: every job that entered the queue is, at any
// quiescent point, in exactly one of four places — ready, in flight, acked,
// or dead-lettered. A leak (a job that is nowhere) and a double-count (a job
// in two places) are the two failure modes this test exists to catch, and
// both are silent in ordinary use.
//
// The randomized driver interleaves enqueue, ack, nack and lease expiry
// through a manually advanced clock, so a failure reproduces exactly from
// its seed rather than depending on scheduler luck.
func TestConservationInvariantUnderRandomOperations(t *testing.T) {
	const (
		steps    = 4000
		topics   = 3
		maxJobs  = 400
		visLease = 5 * time.Second
	)

	for _, seed := range []int64{1, 7, 20260722, 99991} {
		t.Run("seed="+strconv.FormatInt(seed, 10), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			clk := newTestClock()
			q := New(WithClock(clk.Now))
			defer func() { _ = q.Close() }()

			names := []string{"alpha", "beta", "gamma"}[:topics]
			enqueued := map[string]int{}
			held := map[string][]Lease{} // outstanding leases per topic
			ackedIDs := map[string]bool{}

			for step := 0; step < steps; step++ {
				name := names[rng.Intn(len(names))]
				switch rng.Intn(100) {
				case 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
					15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29:
					// ~30% enqueue, bounded so the queue cannot grow forever
					if enqueued[name] >= maxJobs {
						continue
					}
					opts := []EnqueueOption{WithMaxAttempts(1 + rng.Intn(3))}
					if rng.Intn(5) == 0 {
						opts = append(opts, WithDelay(time.Duration(rng.Intn(10))*time.Second))
					}
					if _, err := q.Enqueue(name, []byte("payload"), opts...); err != nil {
						t.Fatalf("enqueue: %v", err)
					}
					enqueued[name]++

				case 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
					41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54:
					// ~25% dequeue
					job, lease, err := q.Dequeue(name, visLease)
					if errors.Is(err, ErrNoJob) {
						continue
					}
					if err != nil {
						t.Fatalf("dequeue: %v", err)
					}
					if ackedIDs[job.ID] {
						t.Fatalf("job %s delivered after it was acked", job.ID)
					}
					held[name] = append(held[name], lease)

				case 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65,
					66, 67, 68, 69, 70, 71, 72, 73, 74:
					// ~20% ack
					leases := held[name]
					if len(leases) == 0 {
						continue
					}
					i := rng.Intn(len(leases))
					lease := leases[i]
					held[name] = append(leases[:i], leases[i+1:]...)
					err := q.Ack(lease)
					if errors.Is(err, ErrUnknownLease) {
						continue // the lease expired first; legitimate
					}
					if err != nil {
						t.Fatalf("ack: %v", err)
					}
					if ackedIDs[lease.JobID] {
						t.Fatalf("job %s acked twice", lease.JobID)
					}
					ackedIDs[lease.JobID] = true

				case 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89:
					// ~15% nack
					leases := held[name]
					if len(leases) == 0 {
						continue
					}
					i := rng.Intn(len(leases))
					lease := leases[i]
					held[name] = append(leases[:i], leases[i+1:]...)
					if err := q.Nack(lease); err != nil && !errors.Is(err, ErrUnknownLease) {
						t.Fatalf("nack: %v", err)
					}

				default:
					// ~10% advance the clock, expiring leases and releasing
					// delayed jobs
					clk.Advance(time.Duration(1+rng.Intn(8)) * time.Second)
				}
			}

			// Quiescence: expire everything still outstanding, then account.
			clk.Advance(time.Hour)
			for _, name := range names {
				st := q.Stats(name)
				dlq := q.Stats(DeadLetterTopic(name))
				accounted := st.Ready + st.InFlight + st.Acked + st.DeadLetters
				if accounted != enqueued[name] {
					t.Errorf("topic %s: enqueued %d but accounted %d "+
						"(ready %d + inflight %d + acked %d + dead %d)",
						name, enqueued[name], accounted,
						st.Ready, st.InFlight, st.Acked, st.DeadLetters)
				}
				if st.InFlight != 0 {
					t.Errorf("topic %s: %d leases survived an hour of clock advance",
						name, st.InFlight)
				}
				// Every dead-lettered job must have arrived in the DLQ.
				if dlq.Enqueued != st.DeadLetters {
					t.Errorf("topic %s: %d dead-lettered but DLQ received %d",
						name, st.DeadLetters, dlq.Enqueued)
				}
			}
		})
	}
}

// A job must never be delivered to two consumers at once, even when leases
// are expiring while other consumers are dequeuing. This is the property the
// actor model exists to guarantee; run it under -race.
func TestNoConcurrentDoubleDeliveryUnderExpiry(t *testing.T) {
	clk := newTestClock()
	q := New(WithClock(clk.Now))
	defer func() { _ = q.Close() }()

	const jobs = 150
	for i := 0; i < jobs; i++ {
		if _, err := q.Enqueue("work", []byte("x"), WithMaxAttempts(100)); err != nil {
			t.Fatal(err)
		}
	}

	var (
		mu      sync.Mutex
		holders = map[string]bool{} // jobID -> currently leased by someone
		wg      sync.WaitGroup
	)
	stop := make(chan struct{})

	for w := 0; w < 6; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				job, lease, err := q.Dequeue("work", time.Second)
				if err != nil {
					continue
				}
				mu.Lock()
				if holders[job.ID] {
					mu.Unlock()
					t.Errorf("job %s leased twice simultaneously", job.ID)
					return
				}
				holders[job.ID] = true
				mu.Unlock()

				// Hold briefly, then release by ack or by letting it expire.
				if job.Attempt%2 == 0 {
					mu.Lock()
					delete(holders, job.ID)
					mu.Unlock()
					_ = q.Ack(lease)
				} else {
					mu.Lock()
					delete(holders, job.ID)
					mu.Unlock()
					_ = q.Nack(lease)
				}
			}
		}()
	}

	// Drive expiry concurrently with the consumers.
	for i := 0; i < 200; i++ {
		clk.Advance(300 * time.Millisecond)
		time.Sleep(time.Millisecond)
	}
	close(stop)
	wg.Wait()
}
