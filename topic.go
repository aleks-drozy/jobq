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
	// logRec appends a WAL record; nil in in-memory mode. The actor never
	// blocks on it: gated waiters are forwarded to the caller's reply
	// channel by a helper goroutine, so fsync latency stalls the caller
	// (as durability demands) but never the actor loop.
	logRec  func(walRecord, bool) waiter
	walID   uint32
	initial []*Job // restored jobs, already justified by the log
	// newID mints job and lease IDs. Random by default; a replicated queue
	// replaces it so every replica mints the same IDs from the same log.
	newID func() string
	// passive stops Stats and Peek from observing lease expiry. A
	// replicated state machine sets it: a read that moved an expired job
	// back to ready on one replica and not another would reorder that
	// replica's ready list and the replicas would diverge.
	passive   bool
	reqs      chan request
	done      chan struct{}
	closeOnce sync.Once
}

// topicConfig bundles newTopic's wiring; only name and now are required.
type topicConfig struct {
	name       string
	now        func() time.Time
	deadLetter func(*Job) bool
	logRec     func(walRecord, bool) waiter
	walID      uint32
	initial    []*Job
	newID      func() string
	passive    bool
}

// request is one operation for the actor loop. Exactly one field is set.
type request struct {
	enqueue   *enqueueReq
	adopt     *adoptReq
	dequeue   *dequeueReq
	settle    *settleReq
	peek      *peekReq
	pending   *pendingReq
	expireAll chan struct{}
	stats     chan Stats
}

// pendingReq asks whether a Dequeue made at time at would find work: a
// visible ready job, or a lease whose deadline has passed. It reads and
// never mutates, whatever the clock says.
type pendingReq struct {
	at    time.Time
	reply chan bool
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

// peekReq is a read-only inspection: it never mutates queue state, takes no
// lease, consumes no attempt, and writes nothing to the log. It exists
// because Dequeue is the only read path and Dequeue mutates (Attempts++),
// which makes "just look at the DLQ" destructive — see queue.go's Peek doc.
type peekReq struct {
	limit           int
	includeInflight bool
	reply           chan peekResp
}

type peekResp struct {
	ready     []*Job
	inflight  []*Job
	truncated bool
}

// inflight is a leased job awaiting settlement.
type inflight struct {
	job      *Job
	leaseID  string
	deadline time.Time
	index    int // position in the deadline heap, maintained by heap.Interface
}

func newTopic(cfg topicConfig) *topic {
	t := &topic{
		name:       cfg.name,
		now:        cfg.now,
		deadLetter: cfg.deadLetter,
		logRec:     cfg.logRec,
		walID:      cfg.walID,
		initial:    cfg.initial,
		newID:      cfg.newID,
		passive:    cfg.passive,
		reqs:       make(chan request),
		done:       make(chan struct{}),
	}
	if t.newID == nil {
		t.newID = newID
	}
	go t.run()
	return t
}

// log appends rec to the WAL if one is attached. Non-gated records return a
// pre-resolved waiter, so this never blocks the actor.
func (t *topic) log(rec walRecord, gated bool) waiter {
	if t.logRec == nil {
		done := make(waiter, 1)
		done <- nil
		return done
	}
	rec.TopicID = t.walID
	return t.logRec(rec, gated)
}

// run is the actor loop: the only goroutine that touches topic state.
func (t *topic) run() {
	var (
		ready    = t.initial // restored FIFO; state already justified by the log
		leases   = map[string]*inflight{}
		deadline = &deadlineHeap{}
		stats    Stats
	)
	t.initial = nil
	stats.Enqueued = len(ready)
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
					ID:    t.newID(),
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
				// Rule 1 of the durability contract: the enqueue must be
				// durable before Enqueue returns (under SyncAlways). The
				// waiter is forwarded off-loop so fsync stalls the caller,
				// never the actor. The job is visible to consumers before
				// the fsync lands; a crash in that window loses only an
				// enqueue that was never acknowledged.
				wait := t.log(walRecord{
					Type: recEnqueue, JobID: job.ID,
					MaxAttempts: job.MaxAttempts, At: now,
					Delay: r.opts.Delay, Payload: job.Payload,
				}, true)
				go func(id string, reply chan enqueueResp) {
					reply <- enqueueResp{id: id, err: <-wait}
				}(job.ID, r.reply)

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
					leaseID:  t.newID(),
					deadline: t.now().Add(r.visibility),
				}
				leases[fl.leaseID] = fl
				heap.Push(deadline, fl)
				stats.InFlight++
				t.log(walRecord{Type: recDeliver, JobID: job.ID, Attempts: job.Attempts}, false)
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
					// Rule 2 of the durability contract: an acked job never
					// comes back. Only true if the ack record is durable
					// before Ack returns success (under SyncAlways) - an
					// ungated ack rides the next group commit, and a crash
					// in that window replays the durable enqueue without
					// the lost ack, resurrecting the job. The crash harness
					// caught exactly that as a flaky GHOST. Same off-loop
					// forwarding as Rule 1: fsync stalls the caller, never
					// the actor.
					wait := t.log(walRecord{Type: recAck, JobID: fl.job.ID}, true)
					go func(reply chan settleResp) {
						reply <- settleResp{err: <-wait}
					}(r.reply)
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

			case req.peek != nil:
				r := req.peek
				if !t.passive {
					expire() // same discipline as stats: settle expiry before reading
				}
				limit := r.limit
				n := len(ready)
				truncated := false
				if limit > 0 && n > limit {
					n = limit
					truncated = true
				}
				readyOut := make([]*Job, n)
				for i := 0; i < n; i++ {
					copied := *ready[i]
					copied.Payload = append([]byte(nil), ready[i].Payload...)
					readyOut[i] = &copied
				}
				var inflightOut []*Job
				if r.includeInflight {
					m := len(*deadline)
					if limit > 0 && m > limit {
						m = limit
						truncated = true
					}
					inflightOut = make([]*Job, m)
					for i := 0; i < m; i++ {
						job := (*deadline)[i].job
						copied := *job
						copied.Payload = append([]byte(nil), job.Payload...)
						inflightOut[i] = &copied
					}
				}
				r.reply <- peekResp{ready: readyOut, inflight: inflightOut, truncated: truncated}

			case req.pending != nil:
				at := req.pending.at
				has := deadline.Len() > 0 && !(*deadline)[0].deadline.After(at)
				for i := 0; !has && i < len(ready); i++ {
					has = !ready[i].NotBefore.After(at)
				}
				req.pending.reply <- has

			case req.expireAll != nil:
				// Every lease ends now, whatever its deadline. Heap order
				// is total (deadline, then lease ID), so every replica
				// returns the jobs to ready in the same order.
				for deadline.Len() > 0 {
					next := heap.Pop(deadline).(*inflight)
					delete(leases, next.leaseID)
					stats.InFlight--
					t.retire(next.job, &ready, &stats)
				}
				close(req.expireAll)

			case req.stats != nil:
				if !t.passive {
					expire()
				}
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
			// The ADOPT record is written by the queue's dead-letter
			// handler, which knows the destination topic's WAL id.
		} else {
			stats.Dropped++
			t.log(walRecord{Type: recDrop, JobID: job.ID}, false)
		}
		return
	}
	t.log(walRecord{Type: recRetry, JobID: job.ID, Attempts: job.Attempts}, false)
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

// peek is read-only: no lease, no attempt consumed, no WAL record. See
// queue.go's Peek doc for why this exists.
func (t *topic) peek(limit int, includeInflight bool) ([]*Job, []*Job, bool, error) {
	reply := make(chan peekResp, 1)
	if err := t.send(request{peek: &peekReq{limit: limit, includeInflight: includeInflight, reply: reply}}); err != nil {
		return nil, nil, false, err
	}
	resp := <-reply
	return resp.ready, resp.inflight, resp.truncated, nil
}

// pending is read-only; see pendingReq.
func (t *topic) pending(at time.Time) bool {
	reply := make(chan bool, 1)
	if err := t.send(request{pending: &pendingReq{at: at, reply: reply}}); err != nil {
		return false
	}
	return <-reply
}

// expireAll ends every outstanding lease, as a restart does.
func (t *topic) expireAll() {
	done := make(chan struct{})
	if err := t.send(request{expireAll: done}); err != nil {
		return
	}
	<-done
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

func (h deadlineHeap) Len() int { return len(h) }

// Less breaks deadline ties on the lease ID so the order is total. Two
// leases issued at the same clock reading then expire in the same order on
// every replica, whatever shape each replica's heap happens to be in.
func (h deadlineHeap) Less(i, j int) bool {
	if !h[i].deadline.Equal(h[j].deadline) {
		return h[i].deadline.Before(h[j].deadline)
	}
	return h[i].leaseID < h[j].leaseID
}
func (h deadlineHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i]; h[i].index = i; h[j].index = j }
func (h *deadlineHeap) Push(x any)   { fl := x.(*inflight); fl.index = len(*h); *h = append(*h, fl) }
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
