# jobq

A durable job queue written from scratch in Go, standard library only.

At-least-once delivery, leases (visibility timeouts), automatic retries with
per-job attempt budgets, delayed jobs, and dead-letter queues, built to be
explained, not just to run. The design decisions, the failure modes, and the
costs of each are documented rather than assumed.

**Status: P3.5 complete (HTTP + CLI + MCP).** A segmented, CRC-checked
write-ahead log with group commit, deterministic replay, and a crash
harness that kills the process cold and proves nothing acknowledged is
lost, plus an HTTP surface (`cmd/jobqd`), a CLI (`cmd/jobq`) that runs
jobs for real, retries them, and dead-letters them live in a terminal, and
an MCP server (`mcp/`, own module, own dependencies, see below) that lets
an agent enqueue, inspect, and recover a queue through eleven typed tools.
See `mcp/README.md` and `mcp/demo/` for a real crash, a real recovery, and
a real MCP client diagnosing it. P4 is the full design deep-dive.
Replication (three nodes on a from-scratch Raft) is in progress; see below.

```go
q, report, _ := jobq.Open("./data", jobq.WithSync(jobq.SyncAlways))
defer q.Close()
log.Printf("recovered: %+v", report) // replay says exactly what it found

// or purely in-memory: q := jobq.New()

q.Enqueue("emails", []byte(`{"to":"a@b.ie"}`), jobq.WithMaxAttempts(3))

job, lease, err := q.Dequeue("emails", 30*time.Second)
if err == nil {
    if send(job.Payload) == nil {
        q.Ack(lease)      // done
    } else {
        q.Nack(lease)     // retry now; after 3 attempts it lands in "emails.dlq"
    }
}
```

## Quickstart: a queue you can actually run

```sh
go build -o jobqd.exe ./cmd/jobqd
go build -o jobq.exe  ./cmd/jobq

./jobqd.exe -dir ./data &        # durable, binds 127.0.0.1:8080 by default

./jobq.exe enqueue --payload '{"to":"a@b.ie"}' --max-attempts 3 emails
./jobq.exe dequeue --wait 5s emails
./jobq.exe stats emails

# curl works too. The enqueue body is the raw payload when
# Content-Type isn't application/json:
curl -X POST http://127.0.0.1:8080/v1/topics/emails/jobs -d 'hello'
```

`jobq work` is the fastest way to see the whole P1 contract (retries,
attempt budgets, dead-lettering) happen live: point it at a command that
exits non-zero on purpose and watch a job retry, exhaust its attempts, and
land in `emails.dlq`.

```sh
# a "worker" that always fails, to watch retries burn the budget
./jobq.exe enqueue --max-attempts 3 emails --payload poison
./jobq.exe work emails -- sh -c 'exit 1'
./jobq.exe stats emails   # DeadLetters: 1
```

The payload arrives on the worker's stdin; `JOBQ_JOB_ID`, `JOBQ_TOPIC`,
`JOBQ_ATTEMPT`, and `JOBQ_MAX_ATTEMPTS` arrive in its environment. Exit 0
acks, non-zero nacks; a SIGINT while a job is running nacks it too,
instead of abandoning it to lease expiry.

There is no authentication (see "Not in v1" below), which is why `jobqd`
binds `127.0.0.1` by default rather than `0.0.0.0`.

## Design decisions

**Actor per topic, not a global mutex.** Each topic owns its state in a
single goroutine; callers communicate over channels. Unrelated topics never
contend, and "expire this lease exactly once" is not a race: it is just
another message in the same serialized stream.

**Lease expiry is evaluated lazily, not by a timer.** Before any read of
queue state, expired leases are swept from a deadline min-heap. This means
expiry is observed at exactly the moment it becomes true, with no timer
goroutine racing the actor and no wakeups on an idle queue.

**At-least-once, deliberately.** A consumer that dies after doing its work
but before acking will see the job again. Exactly-once delivery is not
offered because it cannot be honestly provided by a queue alone: the queue
exposes `Job.ID` and `Job.Attempt` so consumers can key their side effects
and make retries idempotent.

**Dead-letter queues are ordinary topics.** `emails.dlq` is consumed with the
same API as `emails`. A job entering the DLQ gets a fresh attempt budget so it
can be drained and retried, and its failure history is preserved in
`DeadLetteredAttempts`, a field nothing resets, because evidence that is
overwritten by normal use is not evidence.

**Time is injectable.** Every time-dependent behaviour reads a clock
function, so tests drive lease expiry deterministically instead of sleeping.

## Durability

The contract reduces to two rules: an enqueue is durable before `Enqueue`
returns (under `SyncAlways`), and an acked job never comes back. Everything
else can be lost and re-derived at the cost of a duplicate, which
at-least-once permits.

**Payloads are stored unencrypted.** The WAL directory and its segment/lock
files are created with restrictive permissions (`0700`/`0600`, owner-only),
but job payload bytes themselves are written to disk in plain text. There
is no at-rest encryption. Do not enqueue secrets or other sensitive data
without encrypting them yourself before calling `Enqueue`.

**Restarts expire every lease.** Leases are not persisted: a job in flight
at crash time returns to ready with its attempt count intact, or
dead-letters, if the crash consumed its last attempt. Recovery introduces no
semantics a consumer hasn't already seen from an ordinary timeout, and the
inference is deterministic, so it is never re-logged.

**The log is one queue-wide segmented WAL, group-committed.** Actors hand
encoded records to a single committer goroutine; while one batch is inside
`fsync`, everything arriving accumulates into the next. Frames are
CRC-checked with the checksum covering the length prefix; segment headers
are synced at creation because Windows cannot fsync a directory, so file
contents (never file names) are the source of truth. A torn tail is
truncated, counted, and reported; corruption before the tail refuses to
guess.

**The crash harness proves it.** A child process enqueues and consumes under
`SyncAlways`, reporting every acknowledged operation; the parent kills it
cold (`Process.Kill`, no flush) at a random moment and recovers the
directory. Across the standard five rounds: hundreds of acknowledged
enqueues per round, zero lost, zero acked jobs resurrected, duplicates
counted and permitted.

### What each sync policy costs

256-byte payloads, Ryzen 7 5700U, NVMe SSD, Windows 11, Go 1.26. One
machine's numbers, honestly labelled; run `go test -bench BenchmarkEnqueue`
on yours.

| Policy | Sequential | 16 concurrent producers | A crash loses |
|---|---|---|---|
| `SyncAlways` | 550 µs/op (~1.8k/s) | **65 µs/op (~15k/s)** | nothing acknowledged |
| `SyncInterval` (5 ms) | 6.2 µs/op | 5.0 µs/op | ≤ 5 ms of acked enqueues |
| `SyncNever` | 5.6 µs/op | 4.7 µs/op | an unbounded suffix |

The parallel column is group commit doing its job: sixteen producers share
each fsync instead of paying for sixteen.

## Tests

```
go test -race ./...
```

Beyond per-behaviour table tests, two properties are checked directly:

- **Conservation.** Under thousands of randomized, seeded interleavings of
  enqueue/dequeue/ack/nack/expiry across multiple topics, every job that
  entered the queue is accounted for in exactly one place: ready, in flight,
  acked, or dead-lettered. Leaks and double-counts are silent in ordinary
  use; this is what catches them.
- **Unique settlement, honest overlap.** The queue never hands a job to a
  second consumer without an intervening lease expiry or nack, but once a
  lease expires, at-least-once means the previous holder may still be
  working while the redelivery is in someone else's hands. That overlap is
  the contract, not a bug (`Job.ID` and `Job.Attempts` exist so consumers
  can be idempotent), and what IS guaranteed is pinned under the race
  detector: for each job, exactly one lease's Ack ever succeeds; every
  superseded lease is rejected at settle time. The test asserts it actually
  witnessed an overlap, so it cannot silently stop testing the scenario.

P3 adds the same discipline over HTTP: `roundtrip_test.go` runs one
behaviour suite twice (once against `*jobq.Queue`, once against
`*jobqhttp.Client` over a live server) so sentinel errors, attempt
counting, and DLQ routing can't quietly diverge between the two;
`conservation_test.go` reruns the conservation property with concurrent
HTTP workers under `-race`; `restart_test.go` ties the HTTP surface to P2's
durability guarantee directly.

## MCP

`mcp/` is an MCP server over this queue, built on the official
`github.com/modelcontextprotocol/go-sdk`. It is a **separate Go module**
(`github.com/aleks-drozy/jobq/mcp`, with its own `go.mod`) specifically so
this root module's "standard library only" claim stays true: `go list -m all`
here prints exactly `github.com/aleks-drozy/jobq`, and CI checks that, not
just this sentence. See `mcp/README.md` for the eleven tools, and
`docs/specs/2026-07-31-jobq-p3.5-mcp.md` for why MCP was deferred in P3 and
isn't anymore, why it needed one new engine primitive (`Queue.Peek`, a
read-only inspection path, because `Dequeue` is the only other one and it
mutates), and the crash-recovery-diagnosis demo in `mcp/demo/`.

## Replication (in progress)

jobq can run as three nodes that agree on one log of queue operations, so
the queue survives losing any one of them. It keeps the two rules the
single node keeps, cluster-wide:

1. An enqueue acknowledged to the caller survives the loss of any minority
   of nodes.
2. An acked job never comes back.

Delivery is still at-least-once. A change of leader ends every lease, so a
job in flight at that moment is delivered again.

```
# three nodes on one machine (give each its own -dir and ports)
jobqd -id 1 -dir ./n1 -addr 127.0.0.1:8081 -peer-addr 127.0.0.1:9091 \
      -peers 1=http://127.0.0.1:9091,2=http://127.0.0.1:9092,3=http://127.0.0.1:9093 \
      -advertise 1=http://127.0.0.1:8081,2=http://127.0.0.1:8082,3=http://127.0.0.1:8083
# ... same for -id 2 and -id 3

# the CLI and jobqhttp.Client take every node's address and follow the leader
jobq --addr http://127.0.0.1:8081,http://127.0.0.1:8082,http://127.0.0.1:8083 enqueue --payload hi work
```

or `docker compose up --build` for the same cluster in containers.

**How it is built.** Four layers, each testable without the one above:

| Layer | What it is |
|---|---|
| `raft/` | The Raft algorithm from scratch: election, replication, the commit rule. A node does no I/O and owns no goroutines. |
| `raftlog.go` | The Raft log on the same CRC-framed write-ahead log the single node uses. One fsync covers a whole batch of proposals. |
| `cluster/` | The driver that owns the timer, network and disk, and the state machine: an ordinary in-memory `jobq.Queue` fed only by committed log entries. |
| `jobqhttp`, `jobqd` | The same HTTP API as a single node. A follower answers `421 not_leader` with the leader's address; the client follows it. |

**Three decisions worth knowing.**

- *Every change goes through the log, `Dequeue` included.* Handing out a
  lease changes state. A leader cut off from the cluster still believes it
  leads; if it could lease jobs on its own word it would hand out a job the
  real leader is also handing out. `TestCutOffLeaderCannotHandOutJobs` is
  that scenario. (An empty poll is the one thing a leader answers alone, so
  idle consumers do not grow the log.)
- *Replicas never read their own clock or random source.* The leader stamps
  each command with the time; job and lease IDs come from the log position.
  Same log in, same queue out, down to the IDs.
- *A change of leader ends every lease*, the same rule a single node applies
  on restart. Consumers already handle a lease timing out, and lease
  deadlines never have to be compared across two machines' clocks.

**How it is tested.**

- `raft/`: a seeded simulator drives whole clusters through dropped,
  duplicated and reordered messages, crashes, restarts, partitions and slow
  clocks, and checks the Raft safety properties after every step. 10,000
  seeds run clean under the race detector. It also plants three real Raft
  bugs one at a time and must catch each.

  ```
  go test ./raft -run 'TestSimulation$' -raft.sims=10000
  ```
- `cluster/`: three nodes in one process. A 500-step random history, with a
  follower restarted halfway, must leave all three holding identical queues
  down to job IDs and lease order. The conservation property runs under
  concurrent producers and consumers while the leader is shut down four
  times.
- `cmd/jobqd`: the cluster crash harness. Three real `jobqd` processes with
  real disks and real HTTP between them; the test kills the leader cold
  three times, then all three nodes at once, while clients keep working.
  Across 15 consecutive runs on a Windows laptop: 20,117 acknowledged
  enqueues, 45 leader kills, 15 whole-cluster kills, 0 lost, 0 acked jobs
  delivered again.

  ```
  go test ./cmd/jobqd -run TestClusterLosesNothing -v
  ```

**What this does not do yet.** The log is never compacted: it grows forever
and a restarted node replays all of it (snapshots are next). A client that
retries after a lost reply can enqueue a job twice (request IDs are next).
Membership is fixed. Reads (`stats`, `peek`) come from whichever node you
ask and can be stale. There is no authentication on either port, so the
peer port must be reachable only by the other nodes. Killing a process is
not cutting the power: these tests do not prove fsync is honoured by the
disk. Nothing here has been measured for speed, and it has run only on
loopback, never across machines.

See `docs/specs/2026-11-11-jobq-r1-raft.md` and
`docs/specs/2026-11-12-jobq-r2-replicated-queue.md` for the designs, the
numbers, and how far to trust them.

## Not in v1

Consumer-group fan-out, priorities, cron scheduling, authentication, TLS,
rate limiting. v1 is the single node, one machine, done properly;
replication is being built on top of it (see above) and is not finished.
