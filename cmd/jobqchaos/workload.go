package main

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/aleks-drozy/jobq"
	"github.com/aleks-drozy/jobq/jobqhttp"
)

// write is one enqueue call, as the producer that made it saw it.
type write struct {
	sent, done time.Time
	ok         bool
}

// accounting is everything the clients were told, and nothing they were
// not. The contract is judged from this and from the nodes' final counts.
type accounting struct {
	enqueued   map[string]bool // job IDs of acknowledged enqueues
	completed  map[string]bool // job IDs whose ack was acknowledged
	maybeAcked map[string]bool // job IDs with an ack whose outcome is unknown
	delivered  map[string]int  // job ID -> deliveries
	ghosts     []string        // job IDs delivered after their ack was acknowledged
	failed     int             // enqueue calls that returned an error
}

func newAccounting() *accounting {
	return &accounting{enqueued: map[string]bool{}, completed: map[string]bool{},
		maybeAcked: map[string]bool{}, delivered: map[string]int{}}
}

// workload is producers and consumers on one topic, sharing one client as
// a real worker pool would.
type workload struct {
	cl    *jobqhttp.Client
	topic string
	pace  time.Duration

	mu     sync.Mutex
	acct   *accounting
	writes []write

	stopProducing, stopConsuming chan struct{}
	producing, consuming         sync.WaitGroup
}

const (
	// visibility is short so that a job whose ack was lost comes round
	// again while the harness is still watching.
	visibility = 10 * time.Second
	// maxAttempts is high so that redelivery caused by the faults never
	// exhausts a job: a job dropped for running out of attempts would be
	// indistinguishable, from here, from one the cluster lost.
	maxAttempts = 25
)

func startWorkload(addrs, topic string, producers, consumers int, pace time.Duration) *workload {
	w := &workload{
		cl:    jobqhttp.NewClient(addrs, jobqhttp.WithRetryBudget(20*time.Second), jobqhttp.WithTimeout(8*time.Second)),
		topic: topic, pace: pace, acct: newAccounting(),
		stopProducing: make(chan struct{}), stopConsuming: make(chan struct{}),
	}
	for p := 0; p < producers; p++ {
		w.producing.Add(1)
		go w.produce(p)
	}
	for i := 0; i < consumers; i++ {
		w.consuming.Add(1)
		go w.consume(topic)
	}
	w.consuming.Add(1)
	go w.consume(jobq.DeadLetterTopic(topic))
	return w
}

func (w *workload) produce(p int) {
	defer w.producing.Done()
	for i := 0; ; i++ {
		select {
		case <-w.stopProducing:
			return
		default:
		}
		payload := fmt.Sprintf("p%d-%d", p, i)
		sent := time.Now()
		id, err := w.cl.Enqueue(w.topic, []byte(payload), jobq.WithMaxAttempts(maxAttempts))
		done := time.Now()
		w.mu.Lock()
		w.writes = append(w.writes, write{sent: sent, done: done, ok: err == nil})
		if err == nil {
			// Only now is the job acknowledged, and so owed.
			w.acct.enqueued[id] = true
		} else {
			// Not acknowledged, so not owed. It may exist anyway.
			w.acct.failed++
		}
		w.mu.Unlock()
		time.Sleep(w.pace)
	}
}

func (w *workload) consume(from string) {
	defer w.consuming.Done()
	for {
		select {
		case <-w.stopConsuming:
			return
		default:
		}
		job, lease, err := w.cl.Dequeue(from, visibility)
		if err != nil {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		w.mu.Lock()
		w.acct.delivered[job.ID]++
		if w.acct.completed[job.ID] {
			w.acct.ghosts = append(w.acct.ghosts, job.ID)
		}
		w.mu.Unlock()

		time.Sleep(2 * time.Millisecond) // the work, during which a fault may land
		err = w.cl.Ack(lease)
		w.mu.Lock()
		if err == nil {
			// Acknowledged: this job must never be seen again.
			w.acct.completed[job.ID] = true
		} else {
			// The lease died with a leader, or the ack landed and its reply
			// was lost. Either the job comes round again or it is already
			// settled; the nodes' final counts decide.
			w.acct.maybeAcked[job.ID] = true
		}
		w.mu.Unlock()
	}
}

func (w *workload) stopProducers() {
	close(w.stopProducing)
	w.producing.Wait()
}

func (w *workload) stopConsumers() {
	close(w.stopConsuming)
	w.consuming.Wait()
}

// snapshot copies the write log as it stands.
func (w *workload) snapshot() []write {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]write(nil), w.writes...)
}

// contract is the verdict on one fault's worth of work.
type contract struct {
	Acknowledged    int `json:"acknowledged_enqueues"`
	Lost            int `json:"lost"`
	Ghosts          int `json:"ghosts"`
	Duplicates      int `json:"duplicates"` // jobs delivered more than once: permitted
	Completed       int `json:"completed"`
	UnconfirmedAcks int `json:"unconfirmed_acks"` // settled by an ack whose reply never arrived
	FailedEnqueues  int `json:"failed_enqueues"`  // enqueue calls that returned an error
	DeadLettered    int `json:"dead_lettered"`
	NodeEnqueued    int `json:"node_enqueued"` // what the nodes hold; may exceed Acknowledged by up to FailedEnqueues
	// Violations is every way the contract or the nodes' own arithmetic
	// was broken. Empty means the contract held.
	Violations []string `json:"violations"`
}

// judge checks what the clients were told against what the nodes ended up
// holding. main and dead are the topic's and its dead-letter topic's final
// counts, read after the queue drained and every node agreed on them.
func judge(a *accounting, main, dead jobq.Stats) contract {
	c := contract{
		Acknowledged: len(a.enqueued), Ghosts: len(a.ghosts), Completed: len(a.completed),
		FailedEnqueues: a.failed, DeadLettered: main.DeadLetters, NodeEnqueued: main.Enqueued,
		Violations: []string{},
	}
	bad := func(format string, args ...any) { c.Violations = append(c.Violations, fmt.Sprintf(format, args...)) }

	for i, id := range a.ghosts {
		if i == 10 {
			bad("GHOST: and %d more", len(a.ghosts)-10)
			break
		}
		bad("GHOST: job %s was acked and then delivered again", id)
	}
	var lost []string
	for id := range a.enqueued {
		switch {
		case a.completed[id]:
		case a.maybeAcked[id] && a.delivered[id] > 0:
			// Delivered after it was acknowledged, so not lost; then
			// settled by an ack whose reply never arrived. The nodes'
			// counts below confirm nothing is still owed.
			c.UnconfirmedAcks++
		default:
			lost = append(lost, id)
		}
	}
	sort.Strings(lost)
	c.Lost = len(lost)
	for i, id := range lost {
		if i == 10 {
			bad("LOST: and %d more", len(lost)-10)
			break
		}
		bad("LOST: acknowledged enqueue %s was never delivered and completed", id)
	}
	for _, n := range a.delivered {
		if n > 1 {
			c.Duplicates++
		}
	}

	if main.Enqueued != main.Acked+main.DeadLetters {
		bad("nodes: Enqueued %d != Acked %d + DeadLetters %d", main.Enqueued, main.Acked, main.DeadLetters)
	}
	if dead.Enqueued != main.DeadLetters || dead.Enqueued != dead.Acked+dead.Dropped {
		bad("nodes: dead-letter topic Enqueued %d, Acked %d, Dropped %d; topic dead-lettered %d",
			dead.Enqueued, dead.Acked, dead.Dropped, main.DeadLetters)
	}
	// Every acknowledged enqueue exists on the nodes; they may hold more
	// (an enqueue that landed but whose reply was lost), never fewer.
	if main.Enqueued < len(a.enqueued) {
		bad("nodes hold %d enqueues but %d were acknowledged", main.Enqueued, len(a.enqueued))
	}
	// And no more than one per call that failed. The client retries an
	// enqueue under one request ID, so a retry after a lost reply must not
	// add a second job.
	if extra := main.Enqueued - len(a.enqueued); extra > a.failed {
		bad("nodes hold %d enqueues nobody was told about, but only %d calls failed: a retry enqueued twice", extra, a.failed)
	}
	if acked := main.Acked + dead.Acked; acked < len(a.completed) {
		bad("nodes recorded %d acks but %d were acknowledged", acked, len(a.completed))
	}
	if dead.Dropped != 0 {
		bad("%d jobs were destroyed after exhausting their attempts twice over", dead.Dropped)
	}
	return c
}

// firstAckAfter is when the first acknowledgement arrived for a write that
// was sent at or after from. A write already on the wire at from does not
// count: it may have been answered by a node the fault had not reached yet,
// and says nothing about recovery.
func firstAckAfter(ws []write, from time.Time) (time.Time, bool) {
	var first time.Time
	for _, w := range ws {
		if w.ok && !w.sent.Before(from) && (first.IsZero() || w.done.Before(first)) {
			first = w.done
		}
	}
	return first, !first.IsZero()
}

// longestStall is the longest stretch between from and to in which no
// write was acknowledged.
func longestStall(ws []write, from, to time.Time) time.Duration {
	var acks []time.Time
	for _, w := range ws {
		if w.ok && !w.done.Before(from) && !w.done.After(to) {
			acks = append(acks, w.done)
		}
	}
	sort.Slice(acks, func(i, j int) bool { return acks[i].Before(acks[j]) })
	longest, prev := time.Duration(0), from
	for _, t := range append(acks, to) {
		if gap := t.Sub(prev); gap > longest {
			longest = gap
		}
		prev = t
	}
	return longest
}

// latencies returns how long each acknowledged write sent in [from, to)
// took, and how many sent in that window failed.
func latencies(ws []write, from, to time.Time) (took []time.Duration, failed int) {
	for _, w := range ws {
		if w.sent.Before(from) || !w.sent.Before(to) {
			continue
		}
		if w.ok {
			took = append(took, w.done.Sub(w.sent))
		} else {
			failed++
		}
	}
	return took, failed
}

// percentile is the nearest-rank p-th percentile of ds, 0 if ds is empty.
// With few samples the high percentiles are simply the maximum.
func percentile(ds []time.Duration, p float64) time.Duration {
	if len(ds) == 0 {
		return 0
	}
	s := append([]time.Duration(nil), ds...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	rank := int(math.Ceil(float64(len(s)) * p / 100))
	return s[min(max(rank, 1), len(s))-1]
}
