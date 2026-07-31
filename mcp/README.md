# jobq-mcp

An MCP (Model Context Protocol) server over a [jobq](../README.md) queue,
built on the official
[`github.com/modelcontextprotocol/go-sdk`](https://github.com/modelcontextprotocol/go-sdk).

This is a **separate Go module** (`github.com/aleks-drozy/jobq/mcp`) so the
root `jobq` module can keep its "standard library only" claim as a fact CI
checks, not a sentence that quietly goes stale — see
[`docs/specs/2026-07-31-jobq-p3.5-mcp.md`](../docs/specs/2026-07-31-jobq-p3.5-mcp.md)
for the full reasoning, including why an MCP surface justifies dependencies
now that it didn't in P3.

## Why this isn't "the first job-queue MCP server"

It isn't, and the README that would claim it is would be wrong. MCP
adapters over message queues and brokers already exist — `mcp-kafka`,
`kafka-schema-reg-mcp`, AWS's own SQS/SNS MCP server, `mcp-server-rabbitmq`,
"Queue Pilot" (a unified Kafka/RabbitMQ adapter with 14+ tools). What's
different here isn't the protocol layer, it's what's underneath it: every
one of those servers is a client adapter pointed at somebody else's broker
— the durability story belongs to Kafka or SQS, and the MCP layer forwards
a vendor status API. In jobq, the engine is this repository's own segmented,
CRC-checked write-ahead log with deterministic replay and a kill-the-process
harness (`crash_test.go` in the root module) that proves it. `jobq_recovery_report`
returns *this repo's* `RecoveryReport`, from *this repo's* replay path — see
the [demo](demo/) for what that buys an agent that has to reason about a
crash it didn't witness.

## Install

```sh
cd mcp
go build -o jobq-mcp.exe ./cmd/jobq-mcp   # Windows
go build -o jobq-mcp     ./cmd/jobq-mcp   # everywhere else
```

## Running it

**Against a running `jobqd` (the default — recommended):**

```sh
jobqd -dir ./data &                 # or already running elsewhere
jobq-mcp -addr http://127.0.0.1:8080
```

**Embedded, with its own durable queue (no `jobqd` needed):**

```sh
jobq-mcp -dir ./data -sync always
```

`-dir` and `-addr` are mutually exclusive. `-dir` takes the same directory
lock `jobqd` does (`acquireDirLock`), so it **cannot** coexist with a
`jobqd` already running on that directory — the error says so outright
rather than surfacing a raw lock-file error.

Flags:

| Flag | Default | Meaning |
|---|---|---|
| `-addr` | `http://127.0.0.1:8080` (env `JOBQ_ADDR`) | HTTP backend; default mode |
| `-dir` | *(none)* | embedded durable queue; mutually exclusive with `-addr` |
| `-sync` | `always` | fsync policy for `-dir` mode: `always`, `interval`, `never` |
| `-max-wait` | `10s` | cap on `jobq_dequeue`'s `wait_ms`, below typical MCP client timeouts |
| `-max-payload` | `262144` (256 KiB) | cap on `jobq_enqueue`'s payload size |
| `-read-only` | `false` | register only the five read-only tools — no mutation reachable at all |

## Claude Desktop / Claude Code config

```json
{
  "mcpServers": {
    "jobq": {
      "command": "/absolute/path/to/jobq-mcp",
      "args": ["-addr", "http://127.0.0.1:8080"]
    }
  }
}
```

For a read-only inspection server (safe to point at anything real):

```json
{
  "mcpServers": {
    "jobq-readonly": {
      "command": "/absolute/path/to/jobq-mcp",
      "args": ["-addr", "http://127.0.0.1:8080", "-read-only"]
    }
  }
}
```

## Security posture

MCP changes nothing about the threat model. `jobqd` has no auth, which is
why it binds `127.0.0.1` by default (see the root README's "Not in v1").
`jobq-mcp` speaks stdio to its local client and HTTP to whatever `-addr`
names. **Pointing `-addr` at a remote `jobqd` puts an unauthenticated write
API on the network**, reachable through this process's stdio — the flag
help says so, and this README says so too.

## Tools

Eleven tools. Five are read-only (`readOnlyHint: true`, registered under
`-read-only` too); the rest mutate the queue.

| Tool | Read-only? | Notes |
|---|---|---|
| `jobq_enqueue` | no | creates the topic on first use; rejects `.dlq`-suffixed names |
| `jobq_dequeue` | no | **consumes one attempt** — see `jobq_peek` for inspection |
| `jobq_ack` | no | second ack on a settled lease → `unknown_lease` |
| `jobq_nack` | no | consumes an attempt; may dead-letter the job |
| `jobq_extend` | no | only lengthens a lease, never shortens |
| `jobq_peek` | **yes** | no lease, no attempt consumed, no log write — the only safe way to inspect a `.dlq` |
| `jobq_list_topics` | **yes** | split into ordinary topics and their `.dlq` counterparts |
| `jobq_topic_stats` | **yes** | omit `topic` for every topic; unknown topic reports zeroes |
| `jobq_dlq_requeue` | no | compound: dequeue→enqueue→ack, in that order; peek-gated `job_ids` selection — see below |
| `jobq_health` | **yes** | status, durability, uptime, mode |
| `jobq_recovery_report` | **yes** | the process-start `RecoveryReport`, plus a plain-English interpretation |

### Why `jobq_peek` exists

`jobq_dequeue` is the engine's only other read path into a topic, and it
mutates: every delivery increments `Job.Attempts`. A dead-letter queue has
no onward DLQ (`queue.go`'s `retire`), so "just look at the DLQ" implemented
as dequeue-then-nack burns one of the job's five default attempts on every
look — five such "peeks" permanently destroys the evidence, via
`Stats.Dropped`. `jobq_peek` is a genuinely new engine primitive
(`Queue.Peek` in the root module) added specifically so an agent can
inspect a queue, including a DLQ, without cost. `server_test.go`'s
attempt-burn regression test is the guard: twenty `jobq_peek` calls on a
dead-lettered job must never move it toward `Stats.Dropped`.

### `jobq_dlq_requeue` ordering

Per job: **dequeue-from-DLQ → enqueue-to-source → ack-the-DLQ-lease**, in
that order, never ack-first. A crash between the enqueue and the ack means
the job is requeued *and* returns to the DLQ on lease expiry — a duplicate,
which jobq's at-least-once contract permits. Ack-first would risk losing
the job outright instead. The requeued job is a **new job with a new ID**
(the engine has no move primitive), so `DeadLetteredAttempts` history does
not follow it — which is why the tool returns both the old and new IDs.

**Selecting specific jobs via `job_ids` is peek-gated, not nack-and-hope.**
The obvious fix — "just don't nack a job on its last attempt" — doesn't
work: by the time a nack could be refused, `Dequeue` has already consumed
the attempt, and the only ways off a lease are ack (deletes), nack (drops
the job if exhausted), or expiry (same drop, later). So the tool never gets
that far for a job it would destroy: it first takes a read-only `jobq_peek`
snapshot of the DLQ head and plans the whole call before touching anything.

- A non-target one nack away from destruction (`Attempts+1 >= MaxAttempts`)
  is **never dequeued** — left untouched, reported in `skipped` with an
  explicit reason, and it blocks whatever `job_ids` named behind it
  (reported in `not_reached`). Calling again with the same state produces
  the identical result — the blocker never erodes, no matter how many times
  a scan is asked to pass over it.
- A safe non-target is dequeued **at most once** per call; its lease is
  held (not nacked) until the whole plan finishes, then released — one
  attempt spent, reported in `skipped`.
- If the DLQ changes between the peek and execution, the stale plan is
  abandoned: a job the race put on its final attempt is *rescued* (requeued
  instead of nacked — destruction is the only other option at that point)
  and reported separately in `rescued`.
- Every ID in `job_ids` ends up in exactly one of `requeued`, `not_found`
  (never in the scanned window), or `not_reached` (seen, but not processed
  this call — reason `limit_reached`, blocked by another job, or a
  concurrent DLQ change). Nothing is silently stranded.

What this can't remove: a blocker still blocks anything named behind it in
the same call (name the blocker itself to clear it), and a target dequeued
on its final attempt whose source-`Enqueue` then fails is still destroyed
on lease expiry — inherent to enqueue-before-ack, not specific to
`job_ids`. Both need a dequeue-by-ID engine primitive to fully close, which
is out of scope for now (see "Deliberately absent" below). The regression
guards are `server_test.go`'s `TestDLQRequeueNeverDestroysNonTargets`,
`TestDLQRequeueRefusesToDestroyBlocker`, `TestDLQRequeueAccountsForEveryNamedID`,
and `TestDLQRequeueTranscriptScenario`.

### Payload encoding

Input: exactly one of `payload` (UTF-8 text) or `payload_base64`. Both or
neither is a validation error.

Output: `payload` when the bytes are valid UTF-8 and ≤ 4 KiB; otherwise
`payload_base64` plus `payload_bytes` (the *true* length) and
`payload_truncated: true` if only the first 4 KiB is shown. Never silently
lossy — `payload_bytes` always reflects the real size even when the shown
payload is cut short.

### Errors

Sentinel errors map to a stable `code`:

| Code | jobq sentinel |
|---|---|
| `unknown_lease` | `ErrUnknownLease` |
| `empty_topic` | `ErrEmptyTopic` |
| `reserved_topic` | `ErrReservedTopic` |
| `non_positive_duration` | `ErrNonPositiveDuration` |
| `closed` | `ErrClosed` |
| `invalid_payload` | payload encoding policy violated |
| `payload_too_large` | over `-max-payload` |
| `internal` | anything unmapped — a bug, or a transport failure |

`ErrNoJob` is **not** an error on this surface: `jobq_dequeue` returns
`{"empty": true}`.

### Deliberately absent

No purge, no delete, no drain-all, no topic deletion. The engine has no
such primitive, and adding a destructive one whose only consumer is an LLM
was not a trade worth making. See the root README's "Not in v1" for the
rest of what jobq itself doesn't do (replication, clustering, auth, TLS,
priorities, cron).

## Tests

```sh
cd mcp
go test -race ./...
```

`server_test.go`'s `TestToolSuite` is the headline test: one behaviour
suite run twice — once against a `Backend` embedding a local `*jobq.Queue`,
once against a `Backend` that's a `*jobqhttp.Client` talking to a live
`httptest` server. Any divergence in tool output or error mapping fails the
build, the same discipline `jobqhttp/roundtrip_test.go` established for the
HTTP layer in P3. `TestAttemptBurnRegression` is the guard for `jobq_peek`'s
entire reason for existing (see above); `TestDLQRequeueOrdering` fault-injects
a failure between enqueue and ack to prove duplicate-not-loss.
`TestDLQRequeueNeverDestroysNonTargets`, `TestDLQRequeueRefusesToDestroyBlocker`,
`TestDLQRequeueAccountsForEveryNamedID`, `TestDLQRequeueTranscriptScenario`,
and `TestDLQRequeueHeldLeaseSurvivesFault` guard the peek-gated `job_ids`
selection described above.

## Demo

[`demo/`](demo/) runs a real crash, a real recovery, and a real MCP client
walking through the diagnosis — see [`demo/README.md`](demo/README.md).
