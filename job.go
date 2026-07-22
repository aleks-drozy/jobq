// Package jobq is a durable, single-node job queue built on the standard
// library alone. It offers at-least-once delivery with leases (visibility
// timeouts), automatic retries, and dead-letter queues.
//
// Exactly-once delivery is deliberately not offered: a consumer that dies
// after doing its work but before acking will see the job again. Consumers
// that need effective-once semantics should key their side effects on
// Job.ID and Job.Attempt. See docs/superpowers/specs for the reasoning.
package jobq

import (
	"errors"
	"time"
)

// Errors returned by Queue operations. Callers should compare with errors.Is.
var (
	// ErrNoJob means no job was ready at the moment of the request.
	ErrNoJob = errors.New("jobq: no job available")
	// ErrUnknownLease means the lease ID is unknown, already settled, or
	// expired and the job was redelivered to another consumer.
	ErrUnknownLease = errors.New("jobq: unknown or expired lease")
	// ErrClosed means the queue (or the topic) has been closed.
	ErrClosed = errors.New("jobq: closed")
	// ErrEmptyTopic means a topic name was empty.
	ErrEmptyTopic = errors.New("jobq: topic name must not be empty")
)

// DefaultMaxAttempts is used when EnqueueOptions.MaxAttempts is zero.
const DefaultMaxAttempts = 5

// Job is a unit of work handed to a consumer.
type Job struct {
	ID       string
	Topic    string
	Payload  []byte
	Attempt  int // 1 on first delivery, incremented on every redelivery
	Attempts int // attempts consumed so far, including this one
	// MaxAttempts is the ceiling after which the job is dead-lettered.
	MaxAttempts int
	EnqueuedAt  time.Time
	// NotBefore is the earliest time the job may be delivered; zero means
	// immediately.
	NotBefore time.Time
	// DeadLetteredAt is set only on jobs read from a dead-letter topic.
	DeadLetteredAt time.Time
	// DeadLetteredAttempts records how many attempts the job consumed on its
	// original topic before being dead-lettered. Attempts is reset when the
	// job enters the dead-letter topic (so it gets a fresh budget there),
	// which would otherwise destroy the evidence of why it failed.
	DeadLetteredAttempts int
}

// Lease is a consumer's temporary, exclusive claim on a job.
type Lease struct {
	ID       string
	JobID    string
	Topic    string
	Deadline time.Time
}

// EnqueueOptions tune a single Enqueue call.
type EnqueueOptions struct {
	// Delay makes the job invisible for this duration after enqueueing.
	Delay time.Duration
	// MaxAttempts overrides DefaultMaxAttempts when > 0.
	MaxAttempts int
}

func (o EnqueueOptions) normalize() EnqueueOptions {
	if o.MaxAttempts <= 0 {
		o.MaxAttempts = DefaultMaxAttempts
	}
	if o.Delay < 0 {
		o.Delay = 0
	}
	return o
}

// Stats is a point-in-time count of a topic's jobs. The counts are taken
// atomically inside the topic's own goroutine, so they are consistent with
// each other rather than sampled independently.
type Stats struct {
	Ready       int // visible now or waiting on a delay
	InFlight    int // leased, not yet settled
	Acked       int // cumulative
	DeadLetters int // cumulative moves to the DLQ
	Enqueued    int // cumulative
}
