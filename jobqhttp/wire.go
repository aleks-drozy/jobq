package jobqhttp

import (
	"time"

	"github.com/aleks-drozy/jobq"
)

// Wire DTOs. Declared exactly once here and used by both handler.go (server)
// and client.go (client), so a change to the wire format cannot desync the
// two sides — they share the same Go types, not independently maintained
// mirrors of them.

type jobDTO struct {
	ID                   string    `json:"id"`
	Topic                string    `json:"topic"`
	Payload              []byte    `json:"payload"`
	Attempt              int       `json:"attempt"`
	Attempts             int       `json:"attempts"`
	MaxAttempts          int       `json:"max_attempts"`
	EnqueuedAt           time.Time `json:"enqueued_at"`
	NotBefore            time.Time `json:"not_before,omitempty"`
	DeadLetteredAt       time.Time `json:"dead_lettered_at,omitempty"`
	DeadLetteredAttempts int       `json:"dead_lettered_attempts,omitempty"`
}

func jobToDTO(j *jobq.Job) jobDTO {
	return jobDTO{
		ID:                   j.ID,
		Topic:                j.Topic,
		Payload:              j.Payload,
		Attempt:              j.Attempt,
		Attempts:             j.Attempts,
		MaxAttempts:          j.MaxAttempts,
		EnqueuedAt:           j.EnqueuedAt,
		NotBefore:            j.NotBefore,
		DeadLetteredAt:       j.DeadLetteredAt,
		DeadLetteredAttempts: j.DeadLetteredAttempts,
	}
}

func dtoToJob(d jobDTO) *jobq.Job {
	return &jobq.Job{
		ID:                   d.ID,
		Topic:                d.Topic,
		Payload:              d.Payload,
		Attempt:              d.Attempt,
		Attempts:             d.Attempts,
		MaxAttempts:          d.MaxAttempts,
		EnqueuedAt:           d.EnqueuedAt,
		NotBefore:            d.NotBefore,
		DeadLetteredAt:       d.DeadLetteredAt,
		DeadLetteredAttempts: d.DeadLetteredAttempts,
	}
}

type leaseDTO struct {
	ID       string    `json:"id"`
	JobID    string    `json:"job_id"`
	Topic    string    `json:"topic"`
	Deadline time.Time `json:"deadline"`
}

func leaseToDTO(l jobq.Lease) leaseDTO {
	return leaseDTO{ID: l.ID, JobID: l.JobID, Topic: l.Topic, Deadline: l.Deadline}
}

func dtoToLease(d leaseDTO) jobq.Lease {
	return jobq.Lease{ID: d.ID, JobID: d.JobID, Topic: d.Topic, Deadline: d.Deadline}
}

// enqueueJSONRequest is the body for POST .../jobs when Content-Type is
// application/json. Payload is a Go []byte, so encoding/json base64-encodes
// and decodes it for free on both sides.
type enqueueJSONRequest struct {
	Payload     []byte `json:"payload"`
	DelayMs     int64  `json:"delay_ms,omitempty"`
	MaxAttempts int    `json:"max_attempts,omitempty"`
}

type enqueueResponseDTO struct {
	ID string `json:"id"`
}

// dequeueRequestDTO is the body for POST .../dequeue. WaitMs is an
// HTTP-only extension over jobq.Client.Dequeue (which never waits); it
// drives the bounded long-poll in handler_dequeue.go.
type dequeueRequestDTO struct {
	VisibilityMs int64 `json:"visibility_ms"`
	WaitMs       int64 `json:"wait_ms,omitempty"`
}

type dequeueResponseDTO struct {
	Job   *jobDTO   `json:"job"`
	Lease *leaseDTO `json:"lease"`
}

type extendRequestDTO struct {
	ByMs int64 `json:"by_ms"`
}

type extendResponseDTO struct {
	Deadline time.Time `json:"deadline"`
}

type topicsResponseDTO struct {
	Topics []string `json:"topics"`
}

type statsResponseDTO struct {
	Topics map[string]jobq.Stats `json:"topics"`
}

type healthResponseDTO struct {
	Status   string `json:"status"`
	Durable  bool   `json:"durable"`
	UptimeMs int64  `json:"uptime_ms"`
}

// recoveryResponseDTO wraps the RecoveryReport captured once at process
// start. StartedAt is named explicitly (not "now") because this is a
// process-start snapshot, not live queue state — a client polling this
// endpoint later must not mistake it for a fresh recovery.
type recoveryResponseDTO struct {
	Report    jobq.RecoveryReport `json:"report"`
	StartedAt time.Time           `json:"started_at"`
	Dir       string              `json:"dir"`
	Sync      string              `json:"sync"`
}

// peekResponseDTO is the body for GET .../jobs — read-only inspection, no
// lease, no attempt consumed. Truncated is true when more jobs exist than
// limit allowed through.
type peekResponseDTO struct {
	Ready     []jobDTO `json:"ready"`
	Inflight  []jobDTO `json:"inflight"`
	Truncated bool     `json:"truncated"`
}

// errorEnvelope is the one error shape used everywhere in the API.
type errorEnvelope struct {
	Error errorBody `json:"error"`
}

type errorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	// Leader is set only with code "not_leader": the address clients
	// should use for the node believed to lead, when one is known.
	Leader string `json:"leader,omitempty"`
}

// ClusterStatus is the body of GET /v1/cluster: one node's own view.
type ClusterStatus struct {
	ID    uint64 `json:"id"`
	State string `json:"state"` // "leader", "follower" or "candidate"
	Term  uint64 `json:"term"`
	// Leader is the node this one believes leads; 0 if it knows of none.
	Leader     uint64 `json:"leader"`
	LeaderAddr string `json:"leader_addr,omitempty"`
	Commit     uint64 `json:"commit"`
	Applied    uint64 `json:"applied"`
	LastIndex  uint64 `json:"last_index"`
}
