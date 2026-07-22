# jobq

A durable job queue written from scratch in Go, standard library only.

At-least-once delivery, leases (visibility timeouts), automatic retries with
per-job attempt budgets, delayed jobs, and dead-letter queues — built to be
explained, not just to run. The design decisions, the failure modes, and the
costs of each are documented rather than assumed.

**Status: P1 complete — core engine.** In-memory, fully tested. P2 adds the
write-ahead log and crash recovery, P3 an HTTP surface, P4 benchmarks and
the design deep-dive.

```go
q := jobq.New()
defer q.Close()

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
The test suite runs in under a second.

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
- **No double delivery.** Consumers racing against expiring leases never hold
  the same job simultaneously, verified under the race detector.

## Not in v1

Replication, clustering, consumer-group fan-out, priorities, cron scheduling,
authentication. Single node, one machine, done properly.
