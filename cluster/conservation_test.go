package cluster

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aleks-drozy/jobq"
)

// TestConservationAcrossLeaderKills is the single-node conservation
// property (every job that entered the queue ends up in exactly one
// place) run against three nodes while the leader is repeatedly shut down
// and brought back under concurrent producers and consumers.
//
// What it requires:
//
//	no loss      every enqueue a producer saw acknowledged is completed
//	no ghosts    a job whose ack was acknowledged is never delivered again
//	conservation on every node, Enqueued = Acked + DeadLetters for the
//	             topic and Enqueued = Acked + Dropped for its dead-letter
//	             topic, with nothing left ready or in flight
//	agreement    all three nodes report identical queues
//
// What it permits, and counts: a job delivered more than once because its
// lease died with a leader, and an enqueue applied twice because the
// producer could not tell whether its first try landed.
func TestConservationAcrossLeaderKills(t *testing.T) {
	if testing.Short() {
		t.Skip("leader-kill conservation test skipped under -short")
	}
	const (
		producers = 4
		consumers = 4
		kills     = 4
		topic     = "work"
	)
	dlq := jobq.DeadLetterTopic(topic)
	c := newTestCluster(t, 3).startAll()
	c.leader()

	var (
		mu         sync.Mutex
		enqueued   = map[string]bool{} // payloads whose enqueue was acknowledged
		completed  = map[string]int{}  // payload -> acknowledged acks
		maybeAcked = map[string]bool{} // payload -> an ack whose outcome is unknown
		ackedJobs  = map[string]bool{} // job IDs whose ack was acknowledged
		deliveries int
		ghosts     []string
		attempted  atomic.Int64
	)
	killing := make(chan struct{})

	// Producers run for as long as leaders are being killed, so every
	// kill lands on a cluster with work in every state. An enqueue that
	// fails after reaching a leader may or may not have been applied, so
	// the producer tries again: that is where duplicate enqueues come from
	// until request IDs exist.
	var producing sync.WaitGroup
	for p := 0; p < producers; p++ {
		producing.Add(1)
		go func(p int) {
			defer producing.Done()
			for i := 0; ; i++ {
				select {
				case <-killing:
					return
				default:
				}
				attempted.Add(1)
				payload := fmt.Sprintf("p%d-%d", p, i)
				for {
					_, err := c.enqueue(topic, []byte(payload), jobq.WithMaxAttempts(3))
					if err == nil {
						mu.Lock()
						enqueued[payload] = true
						mu.Unlock()
						break
					}
					if !errors.Is(err, jobq.ErrUnavailable) {
						t.Errorf("enqueue %s: %v", payload, err)
						return
					}
				}
				time.Sleep(2 * time.Millisecond)
			}
		}(p)
	}

	// Consumers drain the topic and its dead-letter topic and ack
	// everything they are handed.
	stop := make(chan struct{})
	var consuming sync.WaitGroup
	consume := func(from string) {
		defer consuming.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			job, lease, err := c.dequeue(from, 30*time.Second)
			if err != nil {
				if !errors.Is(err, jobq.ErrNoJob) && !errors.Is(err, jobq.ErrUnavailable) {
					t.Errorf("dequeue %s: %v", from, err)
					return
				}
				time.Sleep(time.Millisecond)
				continue
			}
			payload := string(job.Payload)
			mu.Lock()
			deliveries++
			if ackedJobs[job.ID] {
				ghosts = append(ghosts, job.ID)
			}
			mu.Unlock()

			// Hold the job for a moment, as real work would, so that
			// leases are outstanding when a leader dies.
			time.Sleep(3 * time.Millisecond)
			err = c.ack(lease)
			mu.Lock()
			switch {
			case err == nil:
				ackedJobs[job.ID] = true
				completed[payload]++
			case errors.Is(err, jobq.ErrUnknownLease) || errors.Is(err, jobq.ErrUnavailable):
				// The lease died with a leader, or the ack's fate is
				// unknown. Either the job comes round again or an
				// earlier try of this very ack already landed.
				maybeAcked[payload] = true
			default:
				t.Errorf("ack: %v", err)
			}
			mu.Unlock()
		}
	}
	for i := 0; i < consumers; i++ {
		consuming.Add(2)
		go consume(topic)
		go consume(dlq)
	}

	// The killer: shut the leader down, let the others elect, bring it
	// back, and repeat while the work is in full flow.
	go func() {
		defer close(killing)
		for k := 0; k < kills; k++ {
			time.Sleep(80 * time.Millisecond)
			id := c.leader().cfg.ID
			c.stop(id)
			c.leader()
			time.Sleep(30 * time.Millisecond)
			c.start(id)
		}
	}()

	producing.Wait()
	<-killing
	c.leader()

	// Drained means: nothing ready or in flight anywhere, twice in a row,
	// with every consumer still running.
	quiet := 0
	eventually(t, "the queues to drain", func() bool {
		for _, n := range c.live() {
			for _, name := range []string{topic, dlq} {
				if st := n.Stats(name); st.Ready != 0 || st.InFlight != 0 {
					quiet = 0
					return false
				}
			}
		}
		quiet++
		return quiet > 25
	})
	close(stop)
	consuming.Wait()
	c.converged()

	mu.Lock()
	defer mu.Unlock()
	if len(ghosts) > 0 {
		t.Errorf("GHOST: %d acked jobs were delivered again, e.g. %s", len(ghosts), ghosts[0])
	}
	if int64(len(enqueued)) != attempted.Load() {
		t.Fatalf("only %d of %d enqueues were ever acknowledged", len(enqueued), attempted.Load())
	}
	lost := 0
	for payload := range enqueued {
		if completed[payload] == 0 && !maybeAcked[payload] {
			lost++
			t.Errorf("LOST: acknowledged enqueue %s was never completed", payload)
		}
	}

	nodes := c.live()
	if len(nodes) != 3 {
		t.Fatalf("%d nodes alive at the end, want 3", len(nodes))
	}
	want := snapshot(nodes[0])
	for _, n := range nodes[1:] {
		if got := snapshot(n); got != want {
			t.Fatalf("node %d and node %d disagree:\n%s\n---\n%s", nodes[0].cfg.ID, n.cfg.ID, want, got)
		}
	}
	main, dead := nodes[0].Stats(topic), nodes[0].Stats(dlq)
	if main.Ready != 0 || main.InFlight != 0 || dead.Ready != 0 || dead.InFlight != 0 {
		t.Errorf("jobs left over: %s %+v, %s %+v", topic, main, dlq, dead)
	}
	if main.Enqueued != main.Acked+main.DeadLetters {
		t.Errorf("%s: Enqueued %d != Acked %d + DeadLetters %d", topic, main.Enqueued, main.Acked, main.DeadLetters)
	}
	if dead.Enqueued != main.DeadLetters || dead.Enqueued != dead.Acked+dead.Dropped {
		t.Errorf("%s: Enqueued %d, Acked %d, Dropped %d; %s dead-lettered %d",
			dlq, dead.Enqueued, dead.Acked, dead.Dropped, topic, main.DeadLetters)
	}
	if main.Enqueued < len(enqueued) {
		t.Errorf("cluster holds %d enqueues but %d were acknowledged", main.Enqueued, len(enqueued))
	}
	if acked := main.Acked + dead.Acked; acked < len(ackedJobs) {
		t.Errorf("cluster recorded %d acks but %d were acknowledged", acked, len(ackedJobs))
	}

	// The run must actually have been through what it claims to test.
	final := nodes[0].Status().Term
	if final < uint64(kills) {
		t.Errorf("only reached term %d: the leader was not replaced %d times", final, kills)
	}
	if len(enqueued) < 100 {
		t.Errorf("only %d jobs went through: too few to mean anything", len(enqueued))
	}
	if deliveries == len(ackedJobs) {
		t.Errorf("no job was ever delivered twice: no kill caught a job in flight, so the " +
			"lease-expiry path was never exercised")
	}
	t.Logf("%d acknowledged enqueues, %d applied (%d duplicates); %d deliveries, %d redelivered; "+
		"%d dead-lettered, %d dropped; final term %d, lost %d",
		len(enqueued), main.Enqueued, main.Enqueued-len(enqueued),
		deliveries, deliveries-len(ackedJobs), main.DeadLetters, dead.Dropped, final, lost)
}
