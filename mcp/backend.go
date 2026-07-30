package mcp

import (
	"time"

	"github.com/aleks-drozy/jobq"
)

// Backend is what the tool handlers in tools.go talk to. It is jobq.Client
// (the five lifecycle methods, free on both a local *jobq.Queue and a
// jobqhttp.Client — see jobq's client.go) plus the inspection surface
// jobqhttp.Client already has, plus Peek (added in P3.5; see queue.go's
// Peek doc in the root module for why Dequeue alone isn't safe to build a
// DLQ inspection tool on top of).
//
// *jobqhttp.Client satisfies this outright. localBackend adapts a local
// *jobq.Queue, which is missing Health/Recovery (those describe the
// process, not the queue) and returns nil errors where the local queue
// cannot fail. Running the tool test suite against both is what makes the
// MCP surface provably identical embedded vs. remote — the same discipline
// jobqhttp/roundtrip_test.go established for the HTTP layer.
type Backend interface {
	jobq.Client

	Topics() ([]string, error)
	TopicStats(topic string) (jobq.Stats, error)
	Stats() (map[string]jobq.Stats, error)
	Health() (status string, durable bool, uptime time.Duration, err error)
	Recovery() (report jobq.RecoveryReport, startedAt time.Time, dir, sync string, err error)
	Peek(topic string, limit int, includeInflight bool) (ready, inflight []*jobq.Job, truncated bool, err error)
}

// localBackend adapts an embedded *jobq.Queue to Backend. Its own
// operations cannot fail the way a network call can, so it manufactures the
// nil errors jobqhttp.Client's equivalents return honestly.
type localBackend struct {
	q         *jobq.Queue
	startedAt time.Time
	dir       string // "" for an in-memory queue
	sync      string // "" for an in-memory queue
	durable   bool
	recovery  jobq.RecoveryReport // zero value for an in-memory queue
}

// NewLocalBackend wraps q for embedded (-dir) mode. Pass the RecoveryReport
// jobq.Open returned so jobq_recovery_report has something real to show;
// dir and sync label where and how the queue is durable.
func NewLocalBackend(q *jobq.Queue, report jobq.RecoveryReport, dir, sync string) Backend {
	return &localBackend{q: q, startedAt: time.Now(), dir: dir, sync: sync, durable: dir != "", recovery: report}
}

// NewInMemoryBackend wraps a purely in-memory q (jobq.New()), reporting
// durable:false. Mainly for the demo harness and tests.
func NewInMemoryBackend(q *jobq.Queue) Backend {
	return &localBackend{q: q, startedAt: time.Now()}
}

func (b *localBackend) Enqueue(topic string, payload []byte, opts ...jobq.EnqueueOption) (string, error) {
	return b.q.Enqueue(topic, payload, opts...)
}

func (b *localBackend) Dequeue(topic string, visibility time.Duration) (*jobq.Job, jobq.Lease, error) {
	return b.q.Dequeue(topic, visibility)
}

func (b *localBackend) Ack(lease jobq.Lease) error { return b.q.Ack(lease) }

func (b *localBackend) Nack(lease jobq.Lease) error { return b.q.Nack(lease) }

func (b *localBackend) Extend(lease jobq.Lease, d time.Duration) (time.Time, error) {
	return b.q.Extend(lease, d)
}

func (b *localBackend) Topics() ([]string, error) { return b.q.Topics(), nil }

func (b *localBackend) TopicStats(topic string) (jobq.Stats, error) {
	return b.q.Stats(topic), nil
}

func (b *localBackend) Stats() (map[string]jobq.Stats, error) {
	names := b.q.Topics()
	out := make(map[string]jobq.Stats, len(names))
	for _, name := range names {
		out[name] = b.q.Stats(name)
	}
	return out, nil
}

func (b *localBackend) Health() (status string, durable bool, uptime time.Duration, err error) {
	return "ok", b.durable, time.Since(b.startedAt), nil
}

func (b *localBackend) Recovery() (report jobq.RecoveryReport, startedAt time.Time, dir, sync string, err error) {
	return b.recovery, b.startedAt, b.dir, b.sync, nil
}

// Peek mirrors the limit+1 trick jobqhttp's handlePeek uses: jobq.Queue.Peek
// itself reports no Truncated flag (it cannot fail, so its signature stays
// three values — see queue.go), so one extra job is fetched and trimmed
// here to derive it, rather than widening the engine's own signature.
func (b *localBackend) Peek(topic string, limit int, includeInflight bool) (ready, inflight []*jobq.Job, truncated bool, err error) {
	if limit <= 0 {
		limit = 10
	}
	ready, inflight, err = b.q.Peek(topic, limit+1, includeInflight)
	if err != nil {
		return nil, nil, false, err
	}
	if len(ready) > limit {
		truncated = true
		ready = ready[:limit]
	}
	if len(inflight) > limit {
		truncated = true
		inflight = inflight[:limit]
	}
	return ready, inflight, truncated, nil
}

var _ Backend = (*localBackend)(nil)
