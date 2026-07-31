// Package mcp is an MCP (Model Context Protocol) server over a jobq queue,
// using the official github.com/modelcontextprotocol/go-sdk. It lives in
// its own Go module (see mcp/go.mod) so the root jobq module can keep its
// "standard library only" claim as a CI-checked fact — see
// docs/specs/2026-07-31-jobq-p3.5-mcp.md for the full reasoning, including
// why an MCP surface justifies pulling in dependencies now that it did not
// in P3.
//
// The eleven tools registered by NewServer are a thin layer over Backend
// (client.go's jobq.Client seam, plus the inspection methods jobqhttp.Client
// already has, plus Peek — the one genuinely new engine primitive P3.5
// adds). MCP changes nothing about the threat model: jobqd has no auth,
// which is why it binds 127.0.0.1 by default; this server speaks stdio to
// its local client and, in -addr mode, HTTP to a jobqd that must also be
// local or explicitly trusted. Pointing -addr at a remote jobqd puts an
// unauthenticated write API on the network.
package mcp

import (
	"time"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/aleks-drozy/jobq/jobqhttp"
)

// Version is the jobq-mcp server's reported implementation version.
const Version = "0.1.0"

var _ Backend = (*jobqhttp.Client)(nil)

// falsePtr and trueVal back the tool annotations below: ToolAnnotations
// takes *bool for hints whose zero value (false) is not the same as "not
// set" (DestructiveHint defaults to true when unset), and a plain bool for
// ReadOnlyHint/IdempotentHint, whose zero value already means "no".
func falsePtr() *bool { b := false; return &b }

// ServerOptions configures NewServer. Every field has a workable zero value
// except Backend (in the constructor's own argument).
type ServerOptions struct {
	// MaxWait caps jobq_dequeue's wait_ms, deliberately below typical MCP
	// client tool-call timeouts. Default 10s.
	MaxWait int64 // milliseconds; 0 means "use the 10s default"
	// MaxPayload caps jobq_enqueue's payload size. Default 256 KiB — well
	// under jobqd's own 1 MiB default; a quarter-megabyte payload from an
	// LLM is a mistake, not a use case.
	MaxPayload int
	// ReadOnly registers only the five read-only tools (jobq_peek,
	// jobq_list_topics, jobq_topic_stats, jobq_health,
	// jobq_recovery_report) — no mutation is reachable at all.
	ReadOnly bool
	// Mode and Addr are surfaced verbatim by jobq_health so a caller can
	// tell an embedded server from one proxying a remote jobqd. Mode is
	// "local" or "http"; Addr is set only for "http".
	Mode string
	Addr string
}

const (
	defaultMaxWaitMs  = 10_000
	defaultMaxPayload = 256 * 1024
	defaultServerMode = "local"
)

// NewServer builds the MCP server over be. Register with
// server.Run(ctx, &sdk.StdioTransport{}) (see cmd/jobq-mcp/main.go), or
// connect it directly for tests (server_test.go uses mcp.NewInMemoryTransports).
func NewServer(be Backend, opts ServerOptions) *sdk.Server {
	if opts.MaxWait <= 0 {
		opts.MaxWait = defaultMaxWaitMs
	}
	if opts.MaxPayload <= 0 {
		opts.MaxPayload = defaultMaxPayload
	}
	if opts.Mode == "" {
		opts.Mode = defaultServerMode
	}

	ts := &toolset{
		be:         be,
		maxWait:    time.Duration(opts.MaxWait) * time.Millisecond,
		maxPayload: opts.MaxPayload,
		mode:       opts.Mode,
		addr:       opts.Addr,
	}

	s := sdk.NewServer(&sdk.Implementation{Name: "jobq-mcp", Version: Version}, nil)

	closedWorld := falsePtr()
	readOnly := &sdk.ToolAnnotations{ReadOnlyHint: true, OpenWorldHint: closedWorld}
	nonIdempotentWrite := &sdk.ToolAnnotations{OpenWorldHint: closedWorld}
	settleWrite := &sdk.ToolAnnotations{OpenWorldHint: closedWorld, IdempotentHint: true, DestructiveHint: falsePtr()}

	// Read-only tools are always registered, in both modes.
	sdk.AddTool(s, &sdk.Tool{
		Name: "jobq_peek", Annotations: readOnly,
		Description: "READ-ONLY inspection of a topic (or a .dlq). Does NOT consume an attempt, take a lease, " +
			"or write to the log. This is the correct way to inspect a dead-letter queue — jobq_dequeue would " +
			"burn one of the job's attempts on every look, and a DLQ has no onward DLQ, so a handful of " +
			"\"just checking\" dequeues can permanently destroy the job.",
	}, ts.peek)
	sdk.AddTool(s, &sdk.Tool{
		Name: "jobq_list_topics", Annotations: readOnly,
		Description: "List known topics, split into ordinary topics and their dead-letter (.dlq) counterparts, " +
			"so the caller doesn't need to know the .dlq naming convention to find dead letters.",
	}, ts.listTopics)
	sdk.AddTool(s, &sdk.Tool{
		Name: "jobq_topic_stats", Annotations: readOnly,
		Description: "Consistent per-topic counters, taken inside the topic's own goroutine so they agree " +
			"with each other. Omit topic for every topic. An unknown topic reports zeroes.",
	}, ts.topicStats)
	sdk.AddTool(s, &sdk.Tool{
		Name: "jobq_health", Annotations: readOnly,
		Description: "Process health: status, whether the queue is durable (backed by a WAL), and uptime " +
			"since this process (or the jobqd it's proxying) started.",
	}, ts.health)
	sdk.AddTool(s, &sdk.Tool{
		Name: "jobq_recovery_report", Annotations: readOnly,
		Description: "What write-ahead-log replay found at process start — A SNAPSHOT FROM STARTUP, not live " +
			"state. Includes a plain-English interpretation of the flags (crash vs. clean shutdown, what was " +
			"lost, what was inferred).",
	}, ts.recoveryReport)

	if opts.ReadOnly {
		return s
	}

	sdk.AddTool(s, &sdk.Tool{
		Name: "jobq_enqueue", Annotations: nonIdempotentWrite,
		Description: "Add a job to a topic. The topic is created on first use. Names ending in .dlq are " +
			"reserved for dead-letter queues and rejected.",
	}, ts.enqueue)
	sdk.AddTool(s, &sdk.Tool{
		Name: "jobq_dequeue", Annotations: nonIdempotentWrite,
		Description: "Lease the next ready job from a topic. CONSUMES ONE ATTEMPT of the job's budget — to " +
			"inspect jobs without consuming an attempt, use jobq_peek instead.",
	}, ts.dequeue)
	sdk.AddTool(s, &sdk.Tool{
		Name: "jobq_ack", Annotations: settleWrite,
		Description: "Mark a leased job complete. A second ack on the same lease returns unknown_lease.",
	}, ts.ack)
	sdk.AddTool(s, &sdk.Tool{
		Name: "jobq_nack", Annotations: settleWrite,
		Description: "Return a leased job for retry, consuming one attempt. A second nack on the same lease " +
			"returns unknown_lease.",
	}, ts.nack)
	sdk.AddTool(s, &sdk.Tool{
		Name: "jobq_extend", Annotations: settleWrite,
		Description: "Lengthen a lease. Extend only lengthens a lease — it never shortens one; use jobq_nack " +
			"to release a job early.",
	}, ts.extend)
	sdk.AddTool(s, &sdk.Tool{
		Name: "jobq_dlq_requeue", Annotations: nonIdempotentWrite,
		Description: "Move jobs out of a dead-letter queue back onto their source topic, as new jobs with " +
			"new IDs (there is no in-place move). Internally: dequeue-from-DLQ, then enqueue-to-source, then " +
			"ack-the-DLQ-lease, in that order — never ack-first, so a fault between the two risks a duplicate, " +
			"never a loss. Omit job_ids to take the first `limit` in FIFO order (safe: never nacks anything). " +
			"Pass job_ids (from jobq_peek) to requeue specific jobs: selection is planned from a read-only " +
			"jobq_peek snapshot first, so a job whose pass-over would consume its final attempt is never " +
			"dequeued at all — it is left untouched and blocks the scan behind it, reported in `skipped`/" +
			"`not_reached` rather than destroyed. A safe job passed over to reach a named one costs it at most " +
			"one attempt per call, reported in `skipped`. Every requested ID lands in exactly one of " +
			"`requeued`, `not_found` (absent from the scanned window), or `not_reached` (present but not " +
			"processed this call, with a reason) — nothing is silently stranded. Poison jobs can be left in " +
			"the DLQ for a human by omitting their IDs.",
	}, ts.dlqRequeue)

	return s
}
