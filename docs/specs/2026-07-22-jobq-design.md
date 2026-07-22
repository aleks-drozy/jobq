# jobq — Design Spec

Date: 2026-07-22 · Status: Alex authorized start; system/language forks
delegated to Claude (queue over KV, Go over Python/C++ — rationale in the
decision log below and vault 21-jobq).

## What

A single-node, durable job queue built from scratch in Go, standard library
only. The deliverables are three: the working system, the crash-recovery and
delivery-semantics proofs, and an honest benchmark writeup. This is the
systems-fundamentals portfolio piece: the value is being able to defend
every design decision in an interview.

## Semantics (v1 — SQS-model work queue)

- **Topics** hold jobs. Producers `Enqueue(topic, payload, opts)` with
  optional delay and `maxAttempts` (default 5).
- Competing consumers `Dequeue(topic, visibilityTimeout, batch)` and receive
  **leases**: the job becomes invisible until acked, nacked, or the lease
  expires. Expiry or `Nack` returns it to ready with `attempts+1`.
- `Ack(leaseID)` completes a job; `Extend(leaseID, d)` prolongs a lease.
- Jobs exceeding `maxAttempts` move to the topic's **dead-letter queue**
  (`<topic>.dlq`), which is itself a topic.
- **Delivery contract: at-least-once.** Exactly-once is explicitly a
  non-goal, and the design writeup must explain why (dedup belongs to the
  consumer via idempotency keys; the queue exposes `attempt` so consumers
  can implement it).

## Durability

- Segmented **append-only WAL**: every state transition (ENQUEUE, ACK,
  NACK, EXPIRE, DLQ) is a length-prefixed, CRC-checked record. Boot =
  replay. Torn tails (partial last record) are truncated, counted, logged.
- **fsync policy is configurable and benchmarked**: `always` (fsync per
  append), `interval` (batched, default 5 ms), `never` (OS decides). The
  benchmark table showing what each costs and what each risks IS the
  portfolio story.
- Compaction v1: snapshot current state to a checkpoint file, then drop
  fully-acked WAL segments. No log rewriting.

## Concurrency model

One writer goroutine per topic serializes WAL appends and state mutations
(no locks on the hot path); dequeues are served from the same loop via
request channels. Lease expiry: a min-heap of deadlines drained by a single
timer goroutine per topic. This "actor per topic" model is a deliberate,
defensible choice over a global mutex — and the writeup contrasts the two.

## Surfaces

1. In-process Go library (the core; everything else wraps it).
2. Thin HTTP/JSON server (stdlib `net/http`) + tiny Go client + a CLI demo
   (`jobq enqueue/work`). Binary protocol is v2, not v1.

## Non-goals (v1)

Replication/clustering, consumer-group fan-out (v2), exactly-once delivery,
auth, priorities, cron-style scheduling.

## Proofs (the honesty brand, systems edition)

- **Invariant property test**: under randomized enqueue/ack/nack/expiry
  interleavings, `enqueued == acked + inflight + ready + dlq` at every
  quiescent point, and no job is ever lost or delivered after ack.
- **Crash-recovery harness**: a child process is killed at random points
  under load; the parent replays the WAL and asserts the invariants and the
  at-least-once contract (duplicates allowed, losses never).
- **Benchmarks with methodology**: enqueue/dequeue throughput and p50/p99
  latency across fsync policies and payload sizes, hardware documented,
  variance reported, no cherry-picking.

## Milestones

- **P1** — core in-memory engine: full semantics (leases, expiry, DLQ,
  delays), table-driven tests + invariant property test.
- **P2** — WAL durability + recovery + crash harness.
- **P3** — HTTP server, client, CLI demo.
- **P4** — benchmarks + design writeup (the README becomes a deep-dive).

## Decision log

- **Queue over KV store**: rarer as a portfolio piece than a Redis clone;
  richer interview surface (delivery semantics, visibility timeouts,
  backpressure, idempotency); Alex's own services can consume it later.
- **Go over Python/C++**: second backend language for a Python/TS profile;
  goroutines/channels fit the actor-per-topic design; Dublin-market
  relevant. Stdlib-only to keep the "from scratch" claim honest.
- **SQS-model before consumer groups**: competing-consumer work queues and
  fan-out log groups are different machines; v1 ships one machine whole
  rather than two machines half.
