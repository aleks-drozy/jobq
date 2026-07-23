package jobq

import (
	"container/heap"
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"
)

// topic owns all state for one named queue and mutates it from a single
// goroutine ("actor per topic"). Callers hand work in over channels and read
// results back over per-request reply channels, so the hot path needs no
// mutexes and lease expiry cannot race with delivery.
//
// The alternative — one shared mutex over global maps — is simpler to write
// but serialises unrelated topics against each other and makes "expire this
// lease exactly once" subtle. Here, expiry is just another message.
type topic struct {
	name string
	now  func() time.Time
	// deadLetter is called from the actor goroutine when a job exhausts its
	// attempts; it reports whether the job actually arrived in the DLQ.
	// nil on dead-letter topics themselves (no ".dlq.dlq" chains).
	deadLetter func(*Job) bool

	reqs      chan request
	done      chan struct{}
	closeOnce sync.Once
}

// request is one operation for the actor loop. Exactly one field is set.
type request struct {
	enqueue *enqueueReq
	adopt   *adoptReq
	dequeue *dequeueReq
	settle  *settleReq
	stats   chan Stats
}

// adoptReq hands an existing job to this topic without resetting its
// history. Used for dead-lettering, where the attempt count and the original
// enqueue time are the evidence of what went wrong.
type adoptReq struct {
	job   *Job
	reply chan struct{}
}

type enqueueReq struct {
	payload []byte
	opts    EnqueueOptions
	reply   chan enqueueResp
}

type enqueueResp struct {
	id  string
	err error
}

type dequeueReq struct {
	visibility time.Duration
	reply      chan dequeueResp
}

type dequeueResp struct {
	job   *Job
	lease Lease
	err   error
}

type settleKind int

const (
	settleAck settleKind = iota
	settleNack
	settleExtend
)

type settleReq struct {
	kind    settleKind
	leaseID string
	extend  time.Duration
	reply   chan settleResp
}

type settleResp struct {
	deadline time.Time // effective lease deadline after an extend
	err      error
}

// inflight is a leased job awaiting settlement.
type inflight struct {
	job      *Job
	leaseID  string
	deadline time.Time
	index    int // position in the deadline heap, maintained by heap.Interface
}

func newTopic(name string, now func() time.Time, deadLetter func(*Job) bool) *topic {
	t := &topic{
		name:       name,
		now:        now,
		deadLetter: deadLetter,
		reqs:       make(chan request),
		done:       make(chan struct{}),
	}
	go t.run()
	return t
}

// run is the actor loop: the only goroutine that touches topic state.
func (t *topic) run() {
	var (
		ready    []*Job // FIFO; may contain not-yet-visible delayed jobs
		leases   = map[string]*inflight{}
		deadline = &deadlineHeap{}
		stats    Stats
	)
	heap.Init(deadline)

	// expire returns leases whose deadline has passed to the ready list (or
	// the DLQ). It runs before every read of queue state so that expiry is
	// observed at exactly the moment it becomes true, without a timer
	// goroutine racing the actor.
	expire := func() {
		now := t.now()
		for deadline.Len() > 0 {
			next := (*deadline)[0]
			if next.deadline.After(now) {
				return
			}
			heap.Pop(deadline)
			delete(leases, next.leaseID)
			stats.InFlight--
			t.retire(next.job, &ready, &stats)
		}
	}

	// popReady removes and returns the first visible job, or nil.
	popReady := func() *Job {
		now := t.now()
		for i, job := range ready {
			if job.NotBefore.After(now) {
				continue // still delayed; leave it in place for FIFO fairness
			}
			ready = append(ready[:i], ready[i+1:]...)
			return job
		}
		return nil
	}

	for {
		select {
		case <-t.done:
			return
		case req := <-t.reqs:
			switch {
			case req.enqueue != nil:
				r := req.enqueue
				now := t.now()
				job := &Job{
					ID:    newID(),
					Topic: t.name,
					// Copied: a producer reusing its buffer after Enqueue
					// must not be able to mutate what gets delivered.
					Payload:     append([]byte(nil), r.payload...),
					MaxAttempts: r.opts.MaxAttempts,
					EnqueuedAt:  now,
				}
				if r.opts.Delay > 0 {
					job.NotBefore = now.Add(r.opts.Delay)
				}
				ready = append(ready, job)
				stats.Enqueued++
				r.reply <- enqueueResp{id: job.ID}

			case req.adopt != nil:
				job := req.adopt.job
				job.Topic = t.name
				// The job gets a fresh attempt budget here so a consumer can
				// drain the dead-letter topic; the failure evidence moves to
				// DeadLetteredAttempts, which nothing resets.
				job.DeadLetteredAttempts = job.Attempts
				job.Attempts = 0
				job.Attempt = 0
				job.MaxAttempts = DefaultMaxAttempts
				ready = append(ready, job)
				stats.Enqueued++
				close(req.adopt.reply)

			case req.dequeue != nil:
				r := req.dequeue
				if r.visibility <= 0 {
					r.reply <- dequeueResp{err: ErrNonPositiveDuration}
					break
				}
				expire()
				job := popReady()
				if job == nil {
					r.reply <- dequeueResp{err: ErrNoJob}
					break
				}
				job.Attempts++
				job.Attempt = job.Attempts
				fl := &inflight{
					job:      job,
					leaseID:  newID(),
					deadline: t.now().Add(r.visibility),
				}
				leases[fl.leaseID] = fl
				heap.Push(deadline, fl)
				stats.InFlight++
				copied := *job
				copied.Payload = append([]byte(nil), job.Payload...)
				r.reply <- dequeueResp{
					job: &copied,
					lease: Lease{
						ID:       fl.leaseID,
						JobID:    job.ID,
						Topic:    t.name,
						Deadline: fl.deadline,
					},
				}

			case req.settle != nil:
				r := req.settle
				expire() // a lease that just expired must not settle
				fl, ok := leases[r.leaseID]
				if !ok {
					r.reply <- settleResp{err: ErrUnknownLease}
					break
				}
				switch r.kind {
				case settleAck:
					delete(leases, r.leaseID)
					heap.Remove(deadline, fl.index)
					stats.InFlight--
					stats.Acked++
					r.reply <- settleResp{}
				case settleNack:
					delete(leases, r.leaseID)
					heap.Remove(deadline, fl.index)
					stats.InFlight--
					t.retire(fl.job, &ready, &stats)
					r.reply <- settleResp{}
				case settleExtend:
					// Extend only lengthens. Shortening a live lease would
					// hand the job to a second consumer while the first
					// still holds it; releasing early is what Nack is for.
					if proposed := t.now().Add(r.extend); proposed.After(fl.deadline) {
						fl.deadline = proposed
						heap.Fix(deadline, fl.index)
					}
					r.reply <- settleResp{deadline: fl.deadline}
				}

			case req.stats != nil:
				expire()
				snapshot := stats
				snapshot.Ready = len(ready)
				req.stats <- snapshot
			}
		}
	}
}

// retire sends a job back to the ready list, or onward when it has no
// attempts left: into the dead-letter queue, or — inside a DLQ, which has no
// onward queue — out of existence, counted in Stats.Dropped so destruction
// is never silent. DeadLetters counts confirmed arrivals, not intentions.
// Called only from the actor goroutine.
func (t *topic) retire(job *Job, ready *[]*Job, stats *Stats) {
	if job.Attempts >= job.MaxAttempts {
		job.DeadLetteredAt = t.now()
		if t.deadLetter != nil && t.deadLetter(job) {
			stats.DeadLetters++
		} else {
			stats.Dropped++
		}
		return
	}
	*ready = append(*ready, job)
}

func (t *topic) enqueue(payload []byte, opts EnqueueOptions) (string, error) {
	reply := make(chan enqueueResp, 1)
	if err := t.send(request{enqueue: &enqueueReq{payload: payload, opts: opts, reply: reply}}); err != nil {
		return "", err
	}
	resp := <-reply
	return resp.id, resp.err
}

// adopt places an existing job into this topic, preserving its history, and
// reports whether the topic accepted it before shutdown.
func (t *topic) adopt(job *Job) bool {
	reply := make(chan struct{})
	if err := t.send(request{adopt: &adoptReq{job: job, reply: reply}}); err != nil {
		return false
	}
	<-reply
	return true
}

func (t *topic) dequeue(visibility time.Duration) (*Job, Lease, error) {
	reply := make(chan dequeueResp, 1)
	if err := t.send(request{dequeue: &dequeueReq{visibility: visibility, reply: reply}}); err != nil {
		return nil, Lease{}, err
	}
	resp := <-reply
	return resp.job, resp.lease, resp.err
}

func (t *topic) ack(leaseID string) error {
	_, err := t.settle(settleAck, leaseID, 0)
	return err
}

func (t *topic) nack(leaseID string) error {
	_, err := t.settle(settleNack, leaseID, 0)
	return err
}

func (t *topic) extend(leaseID string, d time.Duration) (time.Time, error) {
	if d <= 0 {
		return time.Time{}, ErrNonPositiveDuration
	}
	return t.settle(settleExtend, leaseID, d)
}

func (t *topic) settle(kind settleKind, leaseID string, d time.Duration) (time.Time, error) {
	reply := make(chan settleResp, 1)
	if err := t.send(request{settle: &settleReq{kind: kind, leaseID: leaseID, extend: d, reply: reply}}); err != nil {
		return time.Time{}, err
	}
	resp := <-reply
	return resp.deadline, resp.err
}

func (t *topic) stats() Stats {
	reply := make(chan Stats, 1)
	if err := t.send(request{stats: reply}); err != nil {
		return Stats{}
	}
	return <-reply
}

// send delivers a request to the actor, or reports that the topic is closed.
func (t *topic) send(req request) error {
	select {
	case <-t.done:
		return ErrClosed
	case t.reqs <- req:
		return nil
	}
}

func (t *topic) close() {
	t.closeOnce.Do(func() { close(t.done) })
}

// deadlineHeap is a min-heap of in-flight jobs ordered by lease deadline, so
// expiry checks cost O(1) to peek instead of scanning every lease.
type deadlineHeap []*inflight

func (h deadlineHeap) Len() int           { return len(h) }
func (h deadlineHeap) Less(i, j int) bool { return h[i].deadline.Before(h[j].deadline) }
func (h deadlineHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i]; h[i].index = i; h[j].index = j }
func (h *deadlineHeap) Push(x any)        { fl := x.(*inflight); fl.index = len(*h); *h = append(*h, fl) }
func (h *deadlineHeap) Pop() any {
	old := *h
	n := len(old)
	fl := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return fl
}

func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("jobq: crypto/rand failed: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}
