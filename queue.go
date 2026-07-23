package jobq

import (
	"strings"
	"sync"
	"time"
)

// DeadLetterSuffix is appended to a topic's name to form its dead-letter
// topic. Dead-letter topics are ordinary topics: they are consumed with the
// same Dequeue/Ack calls, which keeps the API surface small and lets a
// consumer drain poison jobs without special machinery.
const DeadLetterSuffix = ".dlq"

// DeadLetterTopic returns the dead-letter topic name for topic.
func DeadLetterTopic(topic string) string { return topic + DeadLetterSuffix }

// Queue is a collection of named topics. It is safe for concurrent use.
//
// The Queue itself holds only the topic registry behind a mutex; all job
// state lives in per-topic actor goroutines, so contention here is limited
// to topic creation.
type Queue struct {
	now func() time.Time

	mu     sync.RWMutex
	topics map[string]*topic
	closed bool
}

// Option configures a Queue.
type Option func(*Queue)

// WithClock replaces the time source. Tests use it to drive lease expiry
// deterministically instead of sleeping.
func WithClock(now func() time.Time) Option {
	return func(q *Queue) { q.now = now }
}

// New creates an empty Queue.
func New(opts ...Option) *Queue {
	q := &Queue{now: time.Now, topics: map[string]*topic{}}
	for _, opt := range opts {
		opt(q)
	}
	return q
}

// EnqueueOption tunes a single Enqueue call.
type EnqueueOption func(*EnqueueOptions)

// WithDelay makes the job invisible for d after enqueueing.
func WithDelay(d time.Duration) EnqueueOption {
	return func(o *EnqueueOptions) { o.Delay = d }
}

// WithMaxAttempts overrides DefaultMaxAttempts for this job.
func WithMaxAttempts(n int) EnqueueOption {
	return func(o *EnqueueOptions) { o.MaxAttempts = n }
}

// Enqueue adds a job to a topic and returns its ID. The topic is created on
// first use.
func (q *Queue) Enqueue(topicName string, payload []byte, opts ...EnqueueOption) (string, error) {
	if strings.TrimSpace(topicName) == "" {
		return "", ErrEmptyTopic
	}
	// Dead-letter topics are populated only by exhausted jobs. Allowing
	// direct enqueue would let a user topic named "x.dlq" collide with the
	// auto-created DLQ of "x" — and jobs exhausting their attempts on such a
	// topic would have nowhere to go.
	if strings.HasSuffix(topicName, DeadLetterSuffix) {
		return "", ErrReservedTopic
	}
	var o EnqueueOptions
	for _, opt := range opts {
		opt(&o)
	}
	tp, err := q.topicFor(topicName, true)
	if err != nil {
		return "", err
	}
	return tp.enqueue(payload, o.normalize())
}

// Dequeue leases the next ready job from a topic. It returns ErrNoJob when
// nothing is ready — including for a topic that has never been used, so
// consumers can poll a topic before any producer has touched it.
func (q *Queue) Dequeue(topicName string, visibility time.Duration) (*Job, Lease, error) {
	tp, err := q.topicFor(topicName, false)
	if err != nil {
		return nil, Lease{}, err
	}
	if tp == nil {
		return nil, Lease{}, ErrNoJob
	}
	return tp.dequeue(visibility)
}

// Ack marks a leased job complete.
func (q *Queue) Ack(lease Lease) error {
	tp, err := q.leaseTopic(lease)
	if err != nil {
		return err
	}
	return tp.ack(lease.ID)
}

// Nack returns a leased job for retry, consuming one attempt. A job that has
// no attempts left moves to the dead-letter topic.
func (q *Queue) Nack(lease Lease) error {
	tp, err := q.leaseTopic(lease)
	if err != nil {
		return err
	}
	return tp.nack(lease.ID)
}

// Extend lengthens a lease to at least d from now and returns the effective
// deadline. It never shortens a lease (releasing early is Nack's job) and
// rejects non-positive durations, which would amount to instant revocation.
func (q *Queue) Extend(lease Lease, d time.Duration) (time.Time, error) {
	if d <= 0 {
		return time.Time{}, ErrNonPositiveDuration
	}
	tp, err := q.leaseTopic(lease)
	if err != nil {
		return time.Time{}, err
	}
	return tp.extend(lease.ID, d)
}

// Stats returns a consistent snapshot of one topic's counters. An unknown
// topic reports zeroes.
func (q *Queue) Stats(topicName string) Stats {
	tp, err := q.topicFor(topicName, false)
	if err != nil || tp == nil {
		return Stats{}
	}
	return tp.stats()
}

// Topics lists the known topic names, including dead-letter topics.
func (q *Queue) Topics() []string {
	q.mu.RLock()
	defer q.mu.RUnlock()
	names := make([]string, 0, len(q.topics))
	for name := range q.topics {
		names = append(names, name)
	}
	return names
}

// Close shuts down every topic. It is idempotent and safe to call
// concurrently; the topic shutdown loop runs under the registry lock, so a
// second Close returns only after the first has finished. Jobs still in
// memory are dropped: durability across restarts is the WAL's job (P2), not
// Close's. An operation racing Close may complete or return ErrClosed.
func (q *Queue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil
	}
	q.closed = true
	for _, tp := range q.topics {
		tp.close()
	}
	return nil
}

// leaseTopic resolves the topic a lease belongs to. A lease naming a topic
// that does not exist is treated as unknown rather than as an error about
// the topic, so a forged or stale lease looks the same to callers.
func (q *Queue) leaseTopic(lease Lease) (*topic, error) {
	tp, err := q.topicFor(lease.Topic, false)
	if err != nil {
		return nil, err
	}
	if tp == nil {
		return nil, ErrUnknownLease
	}
	return tp, nil
}

// topicFor returns the named topic, creating it when create is true. It
// returns (nil, nil) for an unknown topic when create is false.
func (q *Queue) topicFor(name string, create bool) (*topic, error) {
	q.mu.RLock()
	if q.closed {
		q.mu.RUnlock()
		return nil, ErrClosed
	}
	tp, ok := q.topics[name]
	q.mu.RUnlock()
	if ok {
		return tp, nil
	}
	if !create {
		return nil, nil
	}

	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil, ErrClosed
	}
	if tp, ok := q.topics[name]; ok { // lost the race; reuse the winner
		return tp, nil
	}
	tp = newTopic(name, q.now, q.deadLetterHandler(name))
	q.topics[name] = tp
	return tp, nil
}

// deadLetterHandler routes exhausted jobs into "<name>.dlq". Jobs already in
// a dead-letter topic are not re-dead-lettered: that would build an infinite
// chain of ".dlq.dlq" topics, so they are dropped after their attempts run
// out and the DeadLetters counter is the record that it happened.
func (q *Queue) deadLetterHandler(name string) func(*Job) bool {
	if strings.HasSuffix(name, DeadLetterSuffix) {
		return nil
	}
	dlqName := DeadLetterTopic(name)
	return func(job *Job) bool {
		// Runs on the source topic's actor goroutine. The DLQ is a separate
		// actor, so this hands off without blocking on shared state.
		dlq, err := q.topicFor(dlqName, true)
		if err != nil {
			return false // queue closing; counted as Dropped by the caller
		}
		return dlq.adopt(job)
	}
}
