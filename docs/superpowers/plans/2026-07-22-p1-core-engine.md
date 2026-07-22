# jobq P1: Core Engine Implementation Plan

> Inline-execution deviation as established (dublin-bikes P2/P3 precedent):
> interfaces and verification binding; code appears once in the TDD build.

**Global constraints:** Go stdlib only; `gofmt`-clean; table-driven tests;
every public type/function documented; no locks on topic hot paths (actor
model per spec).

### Task A: module + core types (`jobq.go`, `job.go`)
- `go mod init github.com/aleks-drozy/jobq`
- `Job{ID, Topic, Payload []byte, Attempts, MaxAttempts int, EnqueuedAt, NotBefore time.Time}`,
  `Lease{ID, JobID, Deadline}`, options structs, sentinel errors
  (`ErrNoJob`, `ErrUnknownLease`, `ErrTopicClosed`).
- Test: option defaulting (maxAttempts 5), error identities.

### Task B: topic actor (`topic.go`)
- `newTopic(name)` spawns the actor goroutine; requests via channels:
  `enqueue`, `dequeue` (blocking with ctx or immediate `ErrNoJob`), `ack`,
  `nack`, `extend`, `stats`, `close`.
- Ready jobs: FIFO list honoring `NotBefore` (delay); in-flight: map
  leaseID→job + deadline min-heap; injectable `clock` (function field) so
  tests never sleep.
- Tests: FIFO order; delayed job invisible until NotBefore; dequeue-empty;
  ack completes; nack requeues with attempts+1; extend moves deadline;
  expiry redelivers; attempts > max → DLQ handoff (callback into Queue).

### Task C: queue façade + DLQ (`queue.go`)
- `New()`, `q.Enqueue/Dequeue/Ack/Nack/Extend/Stats/Close`; topics created
  lazily; `<topic>.dlq` topics materialize on first dead-letter; DLQ jobs
  carry original attempts + a `DeadLetteredAt`.
- Tests: cross-topic isolation; dlq receives after max attempts; dequeue
  from dlq works (it is a topic); Close drains cleanly.

### Task D: invariant property test (`invariant_test.go`)
- Randomized driver (seeded, `-run` reproducible): N producers / M
  consumers doing enqueue/ack/nack/expire (clock injected) for K steps;
  at quiescence assert `enqueued == acked + ready + inflight + dlq`, no job
  acked twice, no delivery after ack.
- Verification: `go test ./... -count=1` green + `go vet` clean; commit per
  task; push to a new public repo `jobq` at the end of P1 (Alex has standing
  authorization for this project line).
