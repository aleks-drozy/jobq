# jobq R2: the queue on Raft

Status: built 2026-11-12 on `feat/replicated-queue`. Second of four
replication milestones (R1 Raft core, R2 the queue on Raft, R3 snapshots
and AWS, R4 fault injection on real machines).

## What R2 is

R1 produced a Raft node that agrees on a log and does nothing with it. R2
puts the queue on top: three `jobqd` processes that agree on one log of
queue operations, each applying it to its own copy of the queue, behind
the same HTTP API a single node serves.

The contract is the single-node one, cluster-wide:

1. An enqueue acknowledged to the caller survives the loss of any minority
   of nodes.
2. An acked job never comes back.

Everything else at-least-once already allows is still allowed: a job
delivered twice, and now also an enqueue applied twice when a client
retries after a lost reply.

## The layers

| Layer | Owns | Knows nothing about |
|---|---|---|
| `raft/` (R1) | election, replication, commit | time, network, disk, queues |
| `raftlog.go` | the Raft log on disk | Raft's rules, queues |
| `cluster/` | timer, network, disk; the state machine | HTTP clients |
| `jobqhttp`, `cmd/jobqd` | the wire format, leader hints, retries | Raft |

`cluster.Node` implements `jobq.Client`, the interface `*jobq.Queue` and
`jobqhttp.Client` already implement, so the HTTP handler serves a cluster
node through the code path it serves a local queue.

## Decisions

**Every change goes through the log, Dequeue included.** A dequeue is a
write: it issues a lease and counts an attempt. A leader that has been cut
off still believes it leads until it hears otherwise. If it could lease
jobs on its own word, it and the real leader would hand the same job to
two consumers, and the cut-off leader's later ack would be acknowledged
and then thrown away. With the dequeue in the log, the cut-off leader
cannot commit it and the caller gets "outcome unknown" instead of a job.
`TestCutOffLeaderCannotHandOutJobs` builds exactly that partition.

**An empty poll is the one exception.** An idle consumer polls forever. If
every empty poll were a log entry, the log would grow with nobody doing
any work. So the leader answers "nothing ready" from its own state, using
a read (`Queue.Pending`) that takes the time as an argument and changes
nothing. A stale leader can therefore wrongly say "nothing ready", which
is harmless, but it can never wrongly hand out a job.

**Replicas are deterministic because the queue is given nothing local to
read.** The state machine is an ordinary in-memory `jobq.Queue` with three
things replaced:

- its clock is the timestamp the leader wrote into the entry being
  applied. A new leader whose wall clock is behind never moves it back.
- its job and lease IDs come from the entry's log index, not from
  `crypto/rand`.
- its reads observe nothing. On a single node, `Stats` and `Peek` notice
  expired leases and move those jobs back to ready. Here that would let a
  replica that was asked a question reorder its ready list relative to one
  that was not.

One more change was needed in the queue itself: leases with the same
deadline now expire in lease-ID order. Before, the order depended on the
shape of each replica's heap.

`TestReplicasEndIdentical` checks all of it at once: 500 random operations
across two topics with delays, nacks, extensions and clock jumps, reads
aimed at one node at a time, and a follower restarted halfway so it must
rebuild from the log alone. All three nodes must then render identically,
down to job IDs and the order of in-flight leases. Removing the ID source
or the passive reads each makes it fail.

**A change of leader ends every lease.** Each new leader's first entry is
a no-op (R1 needs it for the commit rule). Applying it expires every
lease, which is the rule a single node already applies on restart. The
alternative is to carry lease deadlines across leaders, which means
comparing one machine's clock with another's. Consumers already cope with
a lease timing out, so this costs a redelivery and adds no new behaviour.

**One fsync per batch, and nothing leaves before it.** The Raft log is
stored as records in the existing write-ahead log (three new record
types: entry, truncate, term-and-vote). Truncation is a record rather
than an edit so the file stays append-only and a torn write can only be
at the tail. Writes are buffered; the node driver takes a burst of input,
calls `Sync` once, and only then sends messages, applies entries or
answers clients. R1's rule was "persist, then speak"; this is the same
rule with the persist moved to the end of the burst. A crash before the
sync loses only what nobody outside the process had heard about.

**A follower says who leads.** A write sent to a follower gets
`421 not_leader` and the leader's client address. The client follows the
hint, rotates through its list when a node cannot be reached, and backs
off between tries. It retries only what another node could answer
differently: a clean refusal, a node shutting down, or no connection at
all. "Outcome unknown" (`503 unavailable`) is returned to the caller,
because retrying it is a decision about duplicates that belongs to them.

## Results

Run on one Windows 11 laptop, all nodes on loopback.

**Cluster crash harness** (`cmd/jobqd`,
`TestClusterLosesNothingAcknowledgedAcrossLeaderKills`). Three `jobqd`
processes with their own directories under `-sync always`, Raft over
loopback HTTP. Three producers and six consumers run against them through
one `jobqhttp.Client`. The test kills the leader with `Process.Kill` three
times, restarting it each time, then kills all three at once and restarts
them.

15 consecutive runs: 20,117 acknowledged enqueues, 45 leader kills, 15
whole-cluster kills. 0 acknowledged enqueues lost. 0 acked jobs delivered
again. In every run the three nodes ended with identical counts satisfying
`Enqueued = Acked + DeadLetters`. Between 2 and 10 jobs per run were
delivered twice because their lease died with a leader.

**Does the harness catch anything?** With the node changed to acknowledge
an enqueue when it is proposed instead of when it commits, the harness
reported lost jobs in 3 of 3 runs (3, 14 and 30 of them).

**In-process conservation** (`cluster`,
`TestConservationAcrossLeaderKills`), under the race detector: about 750
acknowledged enqueues per run with the leader shut down four times; no
loss, no ghosts, all nodes identical. It fails itself if no kill catches a
job in flight.

## A bug the harness found that was not in the cluster

The first runs of the crash harness failed about one time in five, with
the test process running out of threads or the nodes failing to agree on
a leader for 30 seconds. The cluster was fine. The client was opening a
new TCP connection for most requests: Go's default transport keeps two
idle connections per host, and nine workers sharing one host need nine.
Thousands of short-lived connections per run, run after run, used up the
machine's local ports, after which nothing on loopback could connect.

`jobqhttp.NewClient` now builds its own transport with a pool sized for
many workers and one host, `TestClientReusesConnections` pins it, and the
retry loop backs off instead of trying every 25ms. This bug predates
replication. It needed a test that made thousands of requests to find.

## What R2 does not do

- **No snapshots, no compaction.** The log grows forever. A restarted node
  replays all of it, and holds all of it in memory. R3.
- **A retried enqueue can land twice.** The client cannot tell a lost
  request from a lost reply. R3 adds request IDs.
- **An ack whose reply is lost reports failure.** The retry finds the lease
  gone and returns `ErrUnknownLease`. The job is settled; the caller is
  told otherwise. Consistent with at-least-once, and worth knowing.
- **Reads are local and can be stale.** `stats` and `peek` come from
  whichever node is asked. A lease past its deadline shows as in flight
  until the next operation on its topic.
- **Lease IDs are guessable.** They are derived from the log index. With
  no authentication anywhere that changes nothing yet; it will matter
  when auth exists.
- **No authentication on either port.** Whoever can reach the peer port
  can speak Raft to the node. `jobqd` binds both to loopback by default.
- **A killed node leaves its lock file.** `-break-lock` removes it on
  start, for use under a supervisor that guarantees one process per
  directory. Without that guarantee it is unsafe.
- **Process kills are not power cuts.** A killed process's writes are
  still in the operating system's cache. These tests show the protocol
  never acknowledges before a majority has written; they do not show the
  disk honours fsync.
- **Fixed membership, no pre-vote.** As R1.
- **Nothing has been measured, and nothing has left loopback.** No latency
  or throughput numbers are claimed. The cluster has never run across
  machines or under real packet loss. The Dockerfile and compose file were
  written without a Docker daemon to run them on and are untested. R3 and
  R4.
