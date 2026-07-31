package mcp

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/aleks-drozy/jobq"
	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

// --- shared shapes -------------------------------------------------------

// jobOut is the job object every tool that surfaces a job returns, in the
// same shape everywhere (see the "job object, everywhere" note in the P3.5
// plan). encodedPayload is embedded so its fields flatten in.
type jobOut struct {
	ID    string `json:"id"`
	Topic string `json:"topic"`
	encodedPayload
	Attempt              int        `json:"attempt"`
	Attempts             int        `json:"attempts"`
	MaxAttempts          int        `json:"max_attempts"`
	EnqueuedAt           time.Time  `json:"enqueued_at"`
	NotBefore            *time.Time `json:"not_before,omitempty"`
	DeadLetteredAt       *time.Time `json:"dead_lettered_at,omitempty"`
	DeadLetteredAttempts int        `json:"dead_lettered_attempts,omitempty"`
}

func jobToOut(j *jobq.Job) jobOut {
	out := jobOut{
		ID:                   j.ID,
		Topic:                j.Topic,
		encodedPayload:       encodePayload(j.Payload),
		Attempt:              j.Attempt,
		Attempts:             j.Attempts,
		MaxAttempts:          j.MaxAttempts,
		EnqueuedAt:           j.EnqueuedAt,
		DeadLetteredAttempts: j.DeadLetteredAttempts,
	}
	if !j.NotBefore.IsZero() {
		nb := j.NotBefore
		out.NotBefore = &nb
	}
	if !j.DeadLetteredAt.IsZero() {
		da := j.DeadLetteredAt
		out.DeadLetteredAt = &da
	}
	return out
}

type leaseOut struct {
	ID       string    `json:"id"`
	JobID    string    `json:"job_id"`
	Topic    string    `json:"topic"`
	Deadline time.Time `json:"deadline"`
}

func leaseToOut(l jobq.Lease) leaseOut {
	return leaseOut{ID: l.ID, JobID: l.JobID, Topic: l.Topic, Deadline: l.Deadline}
}

// statsOut snake_cases jobq.Stats at this boundary rather than adding json
// tags to the engine struct just for one client — see the P3.5 plan's note
// on jobq_topic_stats.
type statsOut struct {
	Ready       int `json:"ready"`
	InFlight    int `json:"in_flight"`
	Acked       int `json:"acked"`
	DeadLetters int `json:"dead_letters"`
	Dropped     int `json:"dropped"`
	Enqueued    int `json:"enqueued"`
}

func statsToOut(s jobq.Stats) statsOut {
	return statsOut{
		Ready: s.Ready, InFlight: s.InFlight, Acked: s.Acked,
		DeadLetters: s.DeadLetters, Dropped: s.Dropped, Enqueued: s.Enqueued,
	}
}

// toolset holds the Backend and per-server tuning every tool handler needs.
// Its methods are the ToolHandlerFor values registered in server.go.
type toolset struct {
	be         Backend
	maxWait    time.Duration
	maxPayload int
	mode       string // "local" or "http", surfaced by jobq_health
	addr       string // set when mode == "http"
}

// waiter is implemented by jobqhttp.Client (DequeueWait): when the backend
// can long-poll server-side, use that instead of polling over the wire from
// here.
type waiter interface {
	DequeueWait(topic string, visibility, wait time.Duration) (*jobq.Job, jobq.Lease, error)
}

const dequeuePollTick = 25 * time.Millisecond

// dequeueWithWait gives every backend the same wait_ms behavior. A backend
// that can long-poll server-side (jobqhttp.Client) does; a local *jobq.Queue
// has no such call, so this polls in-process instead — cheap, since it
// never leaves the machine.
func (ts *toolset) dequeueWithWait(topic string, visibility, wait time.Duration) (*jobq.Job, jobq.Lease, error) {
	if wait > 0 {
		if w, ok := ts.be.(waiter); ok {
			return w.DequeueWait(topic, visibility, wait)
		}
	}
	job, lease, err := ts.be.Dequeue(topic, visibility)
	if err == nil || !errors.Is(err, jobq.ErrNoJob) || wait <= 0 {
		return job, lease, err
	}
	deadline := time.Now().Add(wait)
	ticker := time.NewTicker(dequeuePollTick)
	defer ticker.Stop()
	for now := range ticker.C {
		if !now.Before(deadline) {
			return nil, jobq.Lease{}, jobq.ErrNoJob
		}
		job, lease, err := ts.be.Dequeue(topic, visibility)
		if err == nil || !errors.Is(err, jobq.ErrNoJob) {
			return job, lease, err
		}
	}
	return nil, jobq.Lease{}, jobq.ErrNoJob
}

// --- 1. jobq_enqueue -------------------------------------------------------

type enqueueIn struct {
	Topic         string  `json:"topic" jsonschema:"Topic to add the job to. Created on first use. Names ending in .dlq are reserved for dead-letter queues and rejected."`
	Payload       *string `json:"payload,omitempty" jsonschema:"UTF-8 payload text. Exactly one of payload or payload_base64 must be set."`
	PayloadBase64 *string `json:"payload_base64,omitempty" jsonschema:"Base64-encoded payload bytes, for binary payloads. Exactly one of payload or payload_base64 must be set."`
	DelayMs       *int64  `json:"delay_ms,omitempty" jsonschema:"Milliseconds the job stays invisible after enqueue. Default 0 (immediately visible)."`
	MaxAttempts   *int    `json:"max_attempts,omitempty" jsonschema:"Deliveries allowed before the job is dead-lettered to <topic>.dlq. Default 5."`
}

type enqueueOut struct {
	JobID       string `json:"job_id"`
	Topic       string `json:"topic"`
	MaxAttempts int    `json:"max_attempts"`
}

func (ts *toolset) enqueue(_ context.Context, _ *sdk.CallToolRequest, in enqueueIn) (*sdk.CallToolResult, enqueueOut, error) {
	payload, err := decodePayload(in.Payload, in.PayloadBase64)
	if err != nil {
		return nil, enqueueOut{}, err
	}
	if ts.maxPayload > 0 && len(payload) > ts.maxPayload {
		return nil, enqueueOut{}, &toolError{
			Code:    "payload_too_large",
			Message: fmt.Sprintf("payload is %d bytes, over this server's %d byte limit (-max-payload)", len(payload), ts.maxPayload),
		}
	}

	var opts []jobq.EnqueueOption
	if in.DelayMs != nil && *in.DelayMs > 0 {
		opts = append(opts, jobq.WithDelay(time.Duration(*in.DelayMs)*time.Millisecond))
	}
	maxAttempts := jobq.DefaultMaxAttempts
	if in.MaxAttempts != nil && *in.MaxAttempts > 0 {
		maxAttempts = *in.MaxAttempts
		opts = append(opts, jobq.WithMaxAttempts(maxAttempts))
	}

	id, err := ts.be.Enqueue(in.Topic, payload, opts...)
	if err != nil {
		return nil, enqueueOut{}, mapError(err)
	}
	return nil, enqueueOut{JobID: id, Topic: in.Topic, MaxAttempts: maxAttempts}, nil
}

// --- 2. jobq_dequeue ---------------------------------------------------

type dequeueIn struct {
	Topic        string `json:"topic" jsonschema:"Topic to lease a job from."`
	VisibilityMs *int64 `json:"visibility_ms,omitempty" jsonschema:"Lease length in ms. If not acked or nacked by then the job is redelivered. Default 30000."`
	WaitMs       *int64 `json:"wait_ms,omitempty" jsonschema:"Bounded long-poll in ms if the topic is empty (server-side polling, not push). Capped by this server's -max-wait. Default 0."`
}

type dequeueOut struct {
	Empty bool      `json:"empty"`
	Job   *jobOut   `json:"job,omitempty"`
	Lease *leaseOut `json:"lease,omitempty"`
}

func (ts *toolset) dequeue(_ context.Context, _ *sdk.CallToolRequest, in dequeueIn) (*sdk.CallToolResult, dequeueOut, error) {
	visibility := 30 * time.Second
	if in.VisibilityMs != nil && *in.VisibilityMs > 0 {
		visibility = time.Duration(*in.VisibilityMs) * time.Millisecond
	}
	var wait time.Duration
	if in.WaitMs != nil && *in.WaitMs > 0 {
		wait = time.Duration(*in.WaitMs) * time.Millisecond
	}
	if ts.maxWait > 0 && wait > ts.maxWait {
		wait = ts.maxWait
	}

	job, lease, err := ts.dequeueWithWait(in.Topic, visibility, wait)
	if err != nil {
		if errors.Is(err, jobq.ErrNoJob) {
			return nil, dequeueOut{Empty: true}, nil
		}
		return nil, dequeueOut{}, mapError(err)
	}
	jo := jobToOut(job)
	lo := leaseToOut(lease)
	return nil, dequeueOut{Job: &jo, Lease: &lo}, nil
}

// --- 3/4. jobq_ack / jobq_nack ------------------------------------------

type leaseIn struct {
	Topic   string `json:"topic" jsonschema:"Topic the lease belongs to."`
	LeaseID string `json:"lease_id" jsonschema:"Lease ID returned by jobq_dequeue."`
}

type ackOut struct {
	Acked bool `json:"acked"`
}

func (ts *toolset) ack(_ context.Context, _ *sdk.CallToolRequest, in leaseIn) (*sdk.CallToolResult, ackOut, error) {
	if err := ts.be.Ack(jobq.Lease{ID: in.LeaseID, Topic: in.Topic}); err != nil {
		return nil, ackOut{}, mapError(err)
	}
	return nil, ackOut{Acked: true}, nil
}

type nackOut struct {
	Nacked bool   `json:"nacked"`
	Note   string `json:"note"`
}

const nackNote = "Attempt consumed. If the budget is now exhausted the job has moved to <topic>.dlq — " +
	"or, inside a .dlq, been destroyed and counted in Stats.Dropped."

func (ts *toolset) nack(_ context.Context, _ *sdk.CallToolRequest, in leaseIn) (*sdk.CallToolResult, nackOut, error) {
	if err := ts.be.Nack(jobq.Lease{ID: in.LeaseID, Topic: in.Topic}); err != nil {
		return nil, nackOut{}, mapError(err)
	}
	return nil, nackOut{Nacked: true, Note: nackNote}, nil
}

// --- 5. jobq_extend ------------------------------------------------------

type extendIn struct {
	Topic   string `json:"topic" jsonschema:"Topic the lease belongs to."`
	LeaseID string `json:"lease_id" jsonschema:"Lease ID returned by jobq_dequeue."`
	ByMs    int64  `json:"by_ms" jsonschema:"Milliseconds to extend by. Extend only lengthens; it never shortens a lease."`
}

type extendOut struct {
	Deadline time.Time `json:"deadline"`
}

func (ts *toolset) extend(_ context.Context, _ *sdk.CallToolRequest, in extendIn) (*sdk.CallToolResult, extendOut, error) {
	if in.ByMs <= 0 {
		return nil, extendOut{}, &toolError{Code: "non_positive_duration", Message: "by_ms must be positive"}
	}
	deadline, err := ts.be.Extend(jobq.Lease{ID: in.LeaseID, Topic: in.Topic}, time.Duration(in.ByMs)*time.Millisecond)
	if err != nil {
		return nil, extendOut{}, mapError(err)
	}
	return nil, extendOut{Deadline: deadline}, nil
}

// --- 6. jobq_peek ----------------------------------------------------------

const (
	defaultPeekLimit = 10
	maxPeekLimit     = 100
)

type peekIn struct {
	Topic           string `json:"topic" jsonschema:"Topic to inspect, e.g. \"emails\" or \"emails.dlq\"."`
	Limit           *int   `json:"limit,omitempty" jsonschema:"Max jobs to return, 1-100. Default 10."`
	IncludeInflight *bool  `json:"include_inflight,omitempty" jsonschema:"Also list leased (in-flight) jobs. Default false."`
}

type peekOut struct {
	Topic     string   `json:"topic"`
	Ready     []jobOut `json:"ready"`
	Inflight  []jobOut `json:"inflight,omitempty"`
	Truncated bool     `json:"truncated"`
}

func (ts *toolset) peek(_ context.Context, _ *sdk.CallToolRequest, in peekIn) (*sdk.CallToolResult, peekOut, error) {
	limit := defaultPeekLimit
	if in.Limit != nil && *in.Limit > 0 {
		limit = *in.Limit
	}
	if limit > maxPeekLimit {
		limit = maxPeekLimit
	}
	includeInflight := in.IncludeInflight != nil && *in.IncludeInflight

	ready, inflight, truncated, err := ts.be.Peek(in.Topic, limit, includeInflight)
	if err != nil {
		return nil, peekOut{}, mapError(err)
	}
	out := peekOut{Topic: in.Topic, Ready: make([]jobOut, len(ready)), Truncated: truncated}
	for i, j := range ready {
		out.Ready[i] = jobToOut(j)
	}
	if includeInflight {
		out.Inflight = make([]jobOut, len(inflight))
		for i, j := range inflight {
			out.Inflight[i] = jobToOut(j)
		}
	}
	return nil, out, nil
}

// --- 7. jobq_list_topics -------------------------------------------------

type emptyIn struct{}

type listTopicsOut struct {
	Topics    []string `json:"topics"`
	DlqTopics []string `json:"dlq_topics"`
}

func (ts *toolset) listTopics(_ context.Context, _ *sdk.CallToolRequest, _ emptyIn) (*sdk.CallToolResult, listTopicsOut, error) {
	names, err := ts.be.Topics()
	if err != nil {
		return nil, listTopicsOut{}, mapError(err)
	}
	out := listTopicsOut{Topics: []string{}, DlqTopics: []string{}}
	for _, n := range names {
		if strings.HasSuffix(n, jobq.DeadLetterSuffix) {
			out.DlqTopics = append(out.DlqTopics, n)
		} else {
			out.Topics = append(out.Topics, n)
		}
	}
	sort.Strings(out.Topics)
	sort.Strings(out.DlqTopics)
	return nil, out, nil
}

// --- 8. jobq_topic_stats -------------------------------------------------

type topicStatsIn struct {
	Topic *string `json:"topic,omitempty" jsonschema:"Restrict to one topic. Omit for every topic."`
}

type topicStatsOut struct {
	Topics map[string]statsOut `json:"topics"`
}

func (ts *toolset) topicStats(_ context.Context, _ *sdk.CallToolRequest, in topicStatsIn) (*sdk.CallToolResult, topicStatsOut, error) {
	out := topicStatsOut{Topics: map[string]statsOut{}}
	if in.Topic != nil && *in.Topic != "" {
		s, err := ts.be.TopicStats(*in.Topic)
		if err != nil {
			return nil, topicStatsOut{}, mapError(err)
		}
		out.Topics[*in.Topic] = statsToOut(s)
		return nil, out, nil
	}
	names, err := ts.be.Topics()
	if err != nil {
		return nil, topicStatsOut{}, mapError(err)
	}
	for _, n := range names {
		s, err := ts.be.TopicStats(n)
		if err != nil {
			return nil, topicStatsOut{}, mapError(err)
		}
		out.Topics[n] = statsToOut(s)
	}
	return nil, out, nil
}

// --- 9. jobq_dlq_requeue ---------------------------------------------------

const (
	defaultRequeueLimit = 10
	maxRequeueLimit     = 50
	// requeueScanWindow bounds the read-only jobq_peek snapshot the job_ids
	// path plans from. 100 is already exercised against both backends by
	// jobq_peek's own maxPeekLimit.
	requeueScanWindow = 100
)

// Reason strings for the job_ids path's skipped/not_reached buckets. Kept as
// constants so tests can assert them exactly.
const (
	// reasonPassedOver marks a non-target job that was safely dequeued (it
	// had at least one attempt to spare) and held for the duration of this
	// call, then returned via a deferred Nack once the plan finished.
	reasonPassedOver = "not in job_ids; passed over to reach a named job — this call held its lease and returned it via nack (one attempt spent)"
	// reasonWouldDestroy marks a non-target that was never dequeued at all
	// because doing so would have put it on its final attempt, and any
	// subsequent nack or lease expiry would then destroy it.
	reasonWouldDestroy = "passing over would consume this job's final attempt and destroy it; left untouched"
	// reasonLimitReached is the not_reached reason when limit (which counts
	// only requeues, not pass-overs) was exhausted before every named ID was
	// reached.
	reasonLimitReached = "limit_reached"
	// reasonConcurrent is the not_reached reason when the DLQ changed
	// between the planning peek and execution, invalidating the rest of the
	// plan.
	reasonConcurrent = "dlq changed concurrently"
)

// reasonBlockedBy is the not_reached reason for named IDs sitting behind a
// blocker job that was left untouched (see reasonWouldDestroy).
func reasonBlockedBy(blockerID string) string {
	return fmt.Sprintf("blocked by %s: passing it over would destroy it", blockerID)
}

type dlqRequeueIn struct {
	Topic       string   `json:"topic" jsonschema:"Source topic, e.g. \"emails\" — the .dlq suffix is added. Passing \"emails.dlq\" also works."`
	Limit       *int     `json:"limit,omitempty" jsonschema:"Max jobs requeued, 1-50 (default 10). When job_ids is set, this counts requeues only — jobs passed over to reach a named job_id no longer count against it, and a job whose pass-over would destroy it is never dequeued at all."`
	JobIDs      []string `json:"job_ids,omitempty" jsonschema:"Requeue only these dead-lettered job IDs (from jobq_peek). Selection is planned from a read-only jobq_peek snapshot first, so it never risks the jobs it scans past; every ID ends up in exactly one of requeued/not_found/not_reached in the response. Omit job_ids to take the first N (see limit) in FIFO order instead."`
	MaxAttempts *int     `json:"max_attempts,omitempty" jsonschema:"Attempt budget for the requeued copy. Default 5."`
}

type requeuedItem struct {
	DLQJobID     string `json:"dlq_job_id"`
	NewJobID     string `json:"new_job_id"`
	PayloadBytes int    `json:"payload_bytes"`
}

type skippedItem struct {
	DLQJobID string `json:"dlq_job_id"`
	Reason   string `json:"reason"`
}

// notReachedItem is a named job_ids entry that was present in the scanned
// window but never processed this call — the plan stopped before reaching
// it. See the reason constants above for why.
type notReachedItem struct {
	JobID  string `json:"job_id"`
	Reason string `json:"reason"`
}

type dlqRequeueOut struct {
	Requeued []requeuedItem `json:"requeued"`
	// Rescued holds jobs that landed on their final attempt only because a
	// concurrent DLQ change defeated the planning-time safety check — the
	// only alternative disposition the engine offers at that point is
	// destruction, so these are requeued instead and reported separately
	// from an ordinary requeue.
	Rescued        []requeuedItem   `json:"rescued,omitempty"`
	Skipped        []skippedItem    `json:"skipped"`
	NotFound       []string         `json:"not_found"`
	NotReached     []notReachedItem `json:"not_reached"`
	LimitReached   bool             `json:"limit_reached"`
	RemainingInDLQ int              `json:"remaining_in_dlq"`
}

// fillRemaining reads remaining_in_dlq for out. Best-effort: a stats read
// that fails leaves the zero value rather than failing the whole call, since
// the requeue/rescue/skip work is already done by the time this runs.
func (ts *toolset) fillRemaining(out *dlqRequeueOut, dlqTopic string) {
	if stats, err := ts.be.TopicStats(dlqTopic); err == nil {
		out.RemainingInDLQ = stats.Ready
	}
}

// dlqRequeue is the one compound tool: per job it does
// dequeue-from-DLQ -> enqueue-to-source -> ack-the-DLQ-lease, in that
// order, never ack-first. A fault between enqueue and ack leaves the DLQ
// lease to expire and the job to be redelivered there — a duplicate, which
// at-least-once permits. Ack-first would risk losing it outright. The
// requeued job is a new job with a new ID (there is no move primitive in
// the engine), so DeadLetteredAttempts history does not follow it — which
// is why both IDs come back.
//
// Without job_ids, every dequeued job is a target (dlqRequeueFIFO): the
// first `limit` in FIFO order, never nacked, so that path was already safe
// and is unchanged here.
//
// With job_ids, selection is planned from a read-only jobq_peek snapshot
// (dlqRequeueByIDs) before anything is touched: a non-target whose pass-over
// would put it on its final attempt is never dequeued at all, so it can
// never be destroyed by this call. A safe non-target is dequeued at most
// once, its lease held (not nacked) until every planned step has run, then
// released — one attempt spent, reported, and provably survivable since it
// was pre-checked to have budget left. Every requested ID lands in exactly
// one of requeued/not_found/not_reached, so nothing is silently stranded.
func (ts *toolset) dlqRequeue(_ context.Context, _ *sdk.CallToolRequest, in dlqRequeueIn) (*sdk.CallToolResult, dlqRequeueOut, error) {
	dlqTopic := in.Topic
	if !strings.HasSuffix(dlqTopic, jobq.DeadLetterSuffix) {
		dlqTopic = jobq.DeadLetterTopic(dlqTopic)
	}
	srcTopic := strings.TrimSuffix(dlqTopic, jobq.DeadLetterSuffix)

	limit := defaultRequeueLimit
	if in.Limit != nil && *in.Limit > 0 {
		limit = *in.Limit
	}
	if limit > maxRequeueLimit {
		limit = maxRequeueLimit
	}

	var enqOpts []jobq.EnqueueOption
	if in.MaxAttempts != nil && *in.MaxAttempts > 0 {
		enqOpts = append(enqOpts, jobq.WithMaxAttempts(*in.MaxAttempts))
	}

	if len(in.JobIDs) == 0 {
		return ts.dlqRequeueFIFO(dlqTopic, srcTopic, limit, enqOpts)
	}
	return ts.dlqRequeueByIDs(dlqTopic, srcTopic, limit, in.JobIDs, enqOpts)
}

// dlqRequeueFIFO is the job_ids-omitted path: take the first `limit` dead
// letters in FIFO order. It never nacks, so it never risks destroying
// anything it touches.
func (ts *toolset) dlqRequeueFIFO(dlqTopic, srcTopic string, limit int, enqOpts []jobq.EnqueueOption) (*sdk.CallToolResult, dlqRequeueOut, error) {
	out := dlqRequeueOut{Requeued: []requeuedItem{}, Skipped: []skippedItem{}, NotFound: []string{}, NotReached: []notReachedItem{}}
	for i := 0; i < limit; i++ {
		job, lease, err := ts.be.Dequeue(dlqTopic, 30*time.Second)
		if err != nil {
			if errors.Is(err, jobq.ErrNoJob) {
				break
			}
			return nil, dlqRequeueOut{}, mapError(err)
		}
		newID, err := ts.be.Enqueue(srcTopic, job.Payload, enqOpts...)
		if err != nil {
			return nil, dlqRequeueOut{}, mapError(err)
		}
		if err := ts.be.Ack(lease); err != nil {
			return nil, dlqRequeueOut{}, mapError(err)
		}
		out.Requeued = append(out.Requeued, requeuedItem{DLQJobID: job.ID, NewJobID: newID, PayloadBytes: len(job.Payload)})
	}
	ts.fillRemaining(&out, dlqTopic)
	return nil, out, nil
}

// dlqRequeueByIDs is the job_ids path. See the dlqRequeue doc comment and
// the reason constants above for the safety argument; in short:
//
//  1. Peek a read-only snapshot of the DLQ head (requeueScanWindow jobs).
//  2. Plan, in head order, stopping at the first of: every named ID found
//     in the window handled; the limit (requeues only) reached; or a
//     non-target whose pass-over would consume its final attempt — that
//     job is never dequeued, and everything behind it in the window is
//     reported not_reached.
//  3. Execute the plan: targets get dequeue -> enqueue-to-source -> ack;
//     safe non-targets get dequeued and their lease HELD (not nacked) so
//     they cannot be re-encountered within this call. If the dequeued job's
//     ID doesn't match the plan (a concurrent DLQ change), the stale
//     snapshot is abandoned: a matching target is requeued, a non-target
//     already on its final attempt is rescued (requeue, since destruction
//     is the only other disposition), otherwise it's held+skipped like any
//     safe non-target — and every remaining named ID is reported
//     not_reached with reasonConcurrent.
//  4. Finally (even on error paths), nack every held lease. Each was
//     pre-checked to have attempts to spare, so this provably cannot
//     destroy it.
func (ts *toolset) dlqRequeueByIDs(dlqTopic, srcTopic string, limit int, jobIDs []string, enqOpts []jobq.EnqueueOption) (res *sdk.CallToolResult, out dlqRequeueOut, callErr error) {
	out = dlqRequeueOut{Requeued: []requeuedItem{}, Skipped: []skippedItem{}, NotFound: []string{}, NotReached: []notReachedItem{}}

	wantIDs := make(map[string]bool, len(jobIDs))
	wantOrder := make([]string, 0, len(jobIDs))
	for _, id := range jobIDs {
		if !wantIDs[id] {
			wantIDs[id] = true
			wantOrder = append(wantOrder, id)
		}
	}

	ready, _, _, err := ts.be.Peek(dlqTopic, requeueScanWindow, false)
	if err != nil {
		return nil, dlqRequeueOut{}, mapError(err)
	}

	foundIdx := make(map[string]int, len(wantIDs))
	lastNeededIdx := -1
	for i, j := range ready {
		if wantIDs[j.ID] {
			foundIdx[j.ID] = i
			lastNeededIdx = i
		}
	}
	for _, id := range wantOrder {
		if _, ok := foundIdx[id]; !ok {
			out.NotFound = append(out.NotFound, id)
		}
	}
	if lastNeededIdx == -1 {
		// None of the named IDs are in the scanned window at all — nothing
		// to plan, nothing to touch.
		ts.fillRemaining(&out, dlqTopic)
		return nil, out, nil
	}

	type plannedStep struct {
		job    *jobq.Job
		target bool
	}
	var plan []plannedStep
	handled := make(map[string]bool, len(foundIdx))
	requeuesPlanned := 0
	limitHit := false
	blockerID := ""

planLoop:
	for i := 0; i <= lastNeededIdx; i++ {
		j := ready[i]
		if requeuesPlanned >= limit {
			limitHit = true
			break planLoop
		}
		if wantIDs[j.ID] {
			plan = append(plan, plannedStep{job: j, target: true})
			requeuesPlanned++
			handled[j.ID] = true
			continue
		}
		if j.Attempts+1 >= j.MaxAttempts {
			// Dequeuing this job would put it on its final attempt; any
			// subsequent nack or lease expiry then destroys it. Leave it
			// untouched (never dequeued) and stop the scan here.
			blockerID = j.ID
			out.Skipped = append(out.Skipped, skippedItem{DLQJobID: j.ID, Reason: reasonWouldDestroy})
			break planLoop
		}
		plan = append(plan, plannedStep{job: j, target: false})
	}

	var stopReason string
	switch {
	case blockerID != "":
		stopReason = reasonBlockedBy(blockerID)
	case limitHit:
		stopReason = reasonLimitReached
		out.LimitReached = true
	}
	if stopReason != "" {
		for _, id := range wantOrder {
			if _, ok := foundIdx[id]; ok && !handled[id] {
				out.NotReached = append(out.NotReached, notReachedItem{JobID: id, Reason: stopReason})
			}
		}
	}

	// heldLeases are safe non-targets already dequeued but not yet nacked —
	// held so they can't be re-encountered within this call, then returned
	// once the plan finishes (or faults), by the deferred loop below. Every
	// entry was pre-checked (Attempts+1 < MaxAttempts at plan time, or an
	// equivalent live check on the race path), so nacking it here provably
	// cannot destroy it.
	//
	// remaining_in_dlq is filled in here too, not at the normal-path return
	// site: TopicStats must be read AFTER every held lease is released,
	// never before, or it undercounts jobs this call is about to hand back.
	var heldLeases []jobq.Lease
	defer func() {
		for _, l := range heldLeases {
			_ = ts.be.Nack(l)
		}
		if callErr == nil {
			ts.fillRemaining(&out, dlqTopic)
		}
	}()

	raceStopped := false
execLoop:
	for _, st := range plan {
		job, lease, derr := ts.be.Dequeue(dlqTopic, 30*time.Second)
		if derr != nil {
			if errors.Is(derr, jobq.ErrNoJob) {
				raceStopped = true
				break execLoop
			}
			return nil, dlqRequeueOut{}, mapError(derr)
		}

		if job.ID != st.job.ID {
			// Concurrent DLQ activity invalidated the snapshot this plan was
			// built from. Handle whatever actually came off the head, then
			// stop consulting the stale plan.
			switch {
			case wantIDs[job.ID] && !handled[job.ID]:
				newID, eerr := ts.be.Enqueue(srcTopic, job.Payload, enqOpts...)
				if eerr != nil {
					return nil, dlqRequeueOut{}, mapError(eerr)
				}
				if aerr := ts.be.Ack(lease); aerr != nil {
					return nil, dlqRequeueOut{}, mapError(aerr)
				}
				out.Requeued = append(out.Requeued, requeuedItem{DLQJobID: job.ID, NewJobID: newID, PayloadBytes: len(job.Payload)})
				handled[job.ID] = true
			case job.Attempts >= job.MaxAttempts:
				// The peek-time safety gate was defeated by the race: this
				// job is now on its final attempt. Destruction is the only
				// other disposition the engine offers, so rescue it instead.
				newID, eerr := ts.be.Enqueue(srcTopic, job.Payload, enqOpts...)
				if eerr != nil {
					return nil, dlqRequeueOut{}, mapError(eerr)
				}
				if aerr := ts.be.Ack(lease); aerr != nil {
					return nil, dlqRequeueOut{}, mapError(aerr)
				}
				out.Rescued = append(out.Rescued, requeuedItem{DLQJobID: job.ID, NewJobID: newID, PayloadBytes: len(job.Payload)})
			default:
				heldLeases = append(heldLeases, lease)
				out.Skipped = append(out.Skipped, skippedItem{DLQJobID: job.ID, Reason: reasonPassedOver})
			}
			raceStopped = true
			break execLoop
		}

		if st.target {
			newID, eerr := ts.be.Enqueue(srcTopic, job.Payload, enqOpts...)
			if eerr != nil {
				// Leave the lease to expire rather than nack it here: the
				// job is on the attempt it was dequeued on, and nack vs.
				// expiry both route through the same retire() — no change
				// in outcome, only in timing.
				return nil, dlqRequeueOut{}, mapError(eerr)
			}
			if aerr := ts.be.Ack(lease); aerr != nil {
				return nil, dlqRequeueOut{}, mapError(aerr)
			}
			out.Requeued = append(out.Requeued, requeuedItem{DLQJobID: job.ID, NewJobID: newID, PayloadBytes: len(job.Payload)})
		} else {
			heldLeases = append(heldLeases, lease)
			out.Skipped = append(out.Skipped, skippedItem{DLQJobID: job.ID, Reason: reasonPassedOver})
		}
	}

	if raceStopped {
		for _, id := range wantOrder {
			if _, ok := foundIdx[id]; !ok || handled[id] {
				continue
			}
			already := false
			for _, nr := range out.NotReached {
				if nr.JobID == id {
					already = true
					break
				}
			}
			if !already {
				out.NotReached = append(out.NotReached, notReachedItem{JobID: id, Reason: reasonConcurrent})
			}
		}
	}

	return nil, out, nil
}

// --- 10. jobq_health -------------------------------------------------------

type healthOut struct {
	Status   string `json:"status"`
	Durable  bool   `json:"durable"`
	UptimeMs int64  `json:"uptime_ms"`
	Mode     string `json:"mode"`
	Addr     string `json:"addr,omitempty"`
}

func (ts *toolset) health(_ context.Context, _ *sdk.CallToolRequest, _ emptyIn) (*sdk.CallToolResult, healthOut, error) {
	status, durable, uptime, err := ts.be.Health()
	if err != nil {
		return nil, healthOut{}, mapError(err)
	}
	return nil, healthOut{Status: status, Durable: durable, UptimeMs: uptime.Milliseconds(), Mode: ts.mode, Addr: ts.addr}, nil
}

// --- 11. jobq_recovery_report ----------------------------------------------

type recoveryReportOut struct {
	Segments         int   `json:"segments"`
	RecordsApplied   int   `json:"records_applied"`
	CleanShutdown    bool  `json:"clean_shutdown"`
	TornTail         bool  `json:"torn_tail"`
	BytesTruncated   int64 `json:"bytes_truncated"`
	JobsRestored     int   `json:"jobs_restored"`
	JobsDeadLettered int   `json:"jobs_dead_lettered"`
}

func recoveryReportToOut(r jobq.RecoveryReport) recoveryReportOut {
	return recoveryReportOut{
		Segments: r.Segments, RecordsApplied: r.RecordsApplied,
		CleanShutdown: r.CleanShutdown, TornTail: r.TornTail, BytesTruncated: r.BytesTruncated,
		JobsRestored: r.JobsRestored, JobsDeadLettered: r.JobsDeadLettered,
	}
}

type recoveryOut struct {
	Report         recoveryReportOut `json:"report"`
	StartedAt      time.Time         `json:"started_at"`
	Dir            string            `json:"dir"`
	Sync           string            `json:"sync"`
	Interpretation []string          `json:"interpretation"`
}

// interpretRecovery turns a RecoveryReport into plain sentences over its
// flags — a small deterministic function, not model output, so the same
// report always yields the same diagnosis.
func interpretRecovery(r jobq.RecoveryReport) []string {
	var lines []string
	switch {
	case r.Segments == 0:
		lines = append(lines, "no WAL segments were found — this is a fresh queue directory, not a recovered one.")
	case r.CleanShutdown:
		lines = append(lines, "clean_shutdown=true: the previous process shut down in an orderly way. Replay found nothing to infer.")
	case r.TornTail:
		lines = append(lines, fmt.Sprintf(
			"clean_shutdown=false with torn_tail=true: the process died mid-write. The log was truncated at the last valid record (%d bytes discarded); only writes that were never acknowledged were lost.",
			r.BytesTruncated))
	default:
		lines = append(lines, "clean_shutdown=false: the previous process did not shut down in an orderly way, though no torn write was found at the tail.")
	}
	if r.JobsDeadLettered > 0 {
		lines = append(lines, fmt.Sprintf(
			"jobs_dead_lettered=%d: those jobs were in flight when the process died AND had already consumed their last attempt, so replay dead-lettered them. Their dead_lettered_at is zero — the engine records no timestamp it cannot know.",
			r.JobsDeadLettered))
	}
	if r.JobsRestored > 0 {
		lines = append(lines, fmt.Sprintf(
			"jobs_restored=%d: leases are not persisted; a restart expires every lease at once, so in-flight jobs returned to ready with attempt counts intact.",
			r.JobsRestored))
	}
	return lines
}

func (ts *toolset) recoveryReport(_ context.Context, _ *sdk.CallToolRequest, _ emptyIn) (*sdk.CallToolResult, recoveryOut, error) {
	rep, startedAt, dir, syncPolicy, err := ts.be.Recovery()
	if err != nil {
		return nil, recoveryOut{}, mapError(err)
	}
	return nil, recoveryOut{
		Report:         recoveryReportToOut(rep),
		StartedAt:      startedAt,
		Dir:            dir,
		Sync:           syncPolicy,
		Interpretation: interpretRecovery(rep),
	}, nil
}
