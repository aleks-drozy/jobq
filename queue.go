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
	now  func() time.Time
	wal  *wal // nil in in-memory mode
	wopt walOptions

	mu          sync.RWMutex
	topics      map[string]*topic
	topicIDs    map[string]uint32
	nextTopicID uint32
	closed      bool
}

// Option configures a Queue.
type Option func(*Queue)

// WithClock replaces the time source. Tests use it to drive lease expiry
// deterministically instead of sleeping.
func WithClock(now func() time.Time) Option {
	return func(q *Queue) { q.now = now }
}

// WithSync selects the fsync policy for a durable queue (see SyncPolicy for
// what each one risks). Ignored by New.
func WithSync(p SyncPolicy) Option {
	return func(q *Queue) { q.wopt.policy = p }
}

// WithSyncEvery sets the flush period for SyncInterval. Ignored otherwise.
func WithSyncEvery(d time.Duration) Option {
	return func(q *Queue) { q.wopt.interval = d }
}

// WithSegmentSize caps WAL segment files, mainly for tests. Ignored by New.
func WithSegmentSize(n int64) Option {
	return func(q *Queue) { q.wopt.segmentBytes = n }
}

// New creates an empty, purely in-memory Queue. Nothing survives Close.
func New(opts ...Option) *Queue {
	q := &Queue{now: time.Now, topics: map[string]*topic{}, topicIDs: map[string]uint32{}}
	for _, opt := range opts {
		opt(q)
	}
	return q
}

// Open creates or recovers a durable Queue rooted at dir. The returned
// report says exactly what recovery found and did; log it.
func Open(dir string, opts ...Option) (*Queue, RecoveryReport, error) {
	q := New(opts...)
	if err := ensureWALDir(dir); err != nil {
		return nil, RecoveryReport{}, err
	}
	lock, err := acquireDirLock(dir)
	if err != nil {
		return nil, RecoveryReport{}, err
	}
	st, rep, err := replayDir(walSubdir(dir))
	if err != nil {
		_ = releaseDirLock(lock)
		return nil, rep, err
	}
	w, err := openWALWithLock(dir, q.wopt, lock)
	if err != nil {
		_ = releaseDirLock(lock)
		return nil, rep, err
	}
	q.wal = w

	ids, maxID := topicIDsFromLog(st.topics)
	q.topicIDs = ids
	q.nextTopicID = maxID

	// Seed restored topics deterministically (sorted names) so replay order
	// never depends on map iteration.
	names := make([]string, 0, len(st.topicJobs))
	for name := range st.topicJobs {
		names = append(names, name)
	}
	sortStrings(names)
	for _, name := range names {
		if _, err := q.makeTopic(name, st.topicJobs[name]); err != nil {
			_ = q.Close()
			return nil, rep, err
		}
	}
	return q, rep, nil
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
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
	if q.wal != nil {
		return q.wal.close()
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
	return q.makeTopicLocked(name, nil)
}

// makeTopic creates a topic (with restored jobs) taking the registry lock.
func (q *Queue) makeTopic(name string, initial []*Job) (*topic, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil, ErrClosed
	}
	if tp, ok := q.topics[name]; ok {
		return tp, nil
	}
	return q.makeTopicLocked(name, initial)
}

// makeTopicLocked assigns the topic's WAL identity (logging a TOPIC record
// on first assignment) and starts its actor. Caller holds q.mu.
func (q *Queue) makeTopicLocked(name string, initial []*Job) (*topic, error) {
	var logRec func(walRecord, bool) waiter
	var id uint32
	if q.wal != nil {
		known, ok := q.topicIDs[name]
		if !ok {
			q.nextTopicID++
			known = q.nextTopicID
			q.topicIDs[name] = known
			w := q.wal.append(walRecord{Type: recTopic, TopicID: known, TopicName: name}, false)
			_ = w // TOPIC records ride the next group commit
		}
		id = known
		logRec = q.wal.append
	}
	tp := newTopic(topicConfig{
		name:       name,
		now:        q.now,
		deadLetter: q.deadLetterHandler(name),
		logRec:     logRec,
		walID:      id,
		initial:    initial,
	})
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
		// Attempts are captured BEFORE adopt: once the job is in the DLQ's
		// ready list a consumer may already be mutating its counters.
		attemptsAtDeath := job.Attempts
		deadAt := job.DeadLetteredAt
		jobID := job.ID
		dlq, err := q.topicFor(dlqName, true)
		if err != nil {
			return false // queue closing; counted as Dropped by the caller
		}
		if !dlq.adopt(job) {
			return false
		}
		if q.wal != nil {
			q.mu.RLock()
			dlqID := q.topicIDs[dlqName]
			q.mu.RUnlock()
			q.wal.append(walRecord{Type: recAdopt, JobID: jobID, TopicID: dlqID,
				Attempts: attemptsAtDeath, At: deadAt}, false)
		}
		return true
	}
}
