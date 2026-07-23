# jobq

A durable job queue written from scratch in Go, standard library only.

At-least-once delivery, leases (visibility timeouts), automatic retries with
per-job attempt budgets, delayed jobs, and dead-letter queues — built to be
explained, not just to run. The design decisions, the failure modes, and the
costs of each are documented rather than assumed.

**Status: P2 complete — durable.** A segmented, CRC-checked write-ahead log
with group commit, deterministic replay, and a crash harness that kills the
process cold and proves nothing acknowledged is lost. P3 adds an HTTP
surface, P4 the full design deep-dive.

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

## Design decisions

**Actor per topic, not a global mutex.** Each topic owns its state in a
single goroutine; callers communicate over channels. Unrelated topics never
contend, and "expire this lease exactly once" is not a race — it is just
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
`DeadLetteredAttempts` — a field nothing resets, because evidence that is
overwritten by normal use is not evidence.

**Time is injectable.** Every time-dependent behaviour reads a clock
function, so tests drive lease expiry deterministically instead of sleeping.

## Durability

The contract reduces to two rules: an enqueue is durable before `Enqueue`
returns (under `SyncAlways`), and an acked job never comes back. Everything
else can be lost and re-derived at the cost of a duplicate, which
at-least-once permits.

**Restarts expire every lease.** Leases are not persisted: a job in flight
at crash time returns to ready with its attempt count intact — or
dead-letters, if the crash consumed its last attempt. Recovery introduces no
semantics a consumer hasn't already seen from an ordinary timeout, and the
inference is deterministic, so it is never re-logged.

**The log is one queue-wide segmented WAL, group-committed.** Actors hand
encoded records to a single committer goroutine; while one batch is inside
`fsync`, everything arriving accumulates into the next. Frames are
CRC-checked with the checksum covering the length prefix; segment headers
are synced at creation because Windows cannot fsync a directory, so file
contents — never file names — are the source of truth. A torn tail is
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
  entered the queue is accounted for in exactly one place — ready, in flight,
  acked, or dead-lettered. Leaks and double-counts are silent in ordinary
  use; this is what catches them.
- **Unique settlement, honest overlap.** The queue never hands a job to a
  second consumer without an intervening lease expiry or nack — but once a
  lease expires, at-least-once means the previous holder may still be
  working while the redelivery is in someone else's hands. That overlap is
  the contract, not a bug (`Job.ID` and `Job.Attempts` exist so consumers
  can be idempotent), and what IS guaranteed is pinned under the race
  detector: for each job, exactly one lease's Ack ever succeeds — every
  superseded lease is rejected at settle time. The test asserts it actually
  witnessed an overlap, so it cannot silently stop testing the scenario.

## Not in v1

Replication, clustering, consumer-group fan-out, priorities, cron scheduling,
authentication. Single node, one machine, done properly.
