# jobq R3: snapshots, enqueue once, and running it

Status: built 2026-11-13 on `feat/snapshots-dedup-metrics`. Third of four
replication milestones (R1 Raft core, R2 the queue on Raft, R3 this, R4
fault injection on real machines).

**The AWS half of this milestone is written and not run.** There was no
Terraform binary, no Docker and no AWS account on the machine it was built
on. `deploy/terraform` and the AWS sections of `RUNBOOK.md` have never been
applied or followed. Everything else below was run, and the numbers are
from those runs.

## What R3 is

R2 left a cluster that kept its promises and could not be left running:
its log grew forever, a restart replayed all of it, and a client that
retried an enqueue could add the job twice. R3 closes those three, and
adds what it takes to look after a cluster: metrics, a status command, and
a runbook.

| | R2 | R3 |
|---|---|---|
| Log on disk | grows forever | bounded: a snapshot plus the last few thousand entries |
| Restart | replays the whole log | loads a snapshot, replays what follows |
| Node far behind | sent every entry | sent the snapshot |
| Retried enqueue | may add a second job | adds one |
| Leader cut off from a majority | believes it leads until told otherwise | steps down |
| Looking inside | `GET /v1/cluster` | plus `GET /metrics`, `jobq cluster` |

The contract is unchanged. Delivery is still at-least-once.

## Decisions

**A snapshot is the queue's whole state, and a queue rebuilt from one is
the same queue.** `Queue.State` copies out every topic's ready jobs,
leases and counters in an order that does not depend on how the queue got
there; `NewFromState` builds a queue from that. Leases come back live,
with their IDs and deadlines. This could not be simpler by ending every
lease at a snapshot, the way a restart does: nodes snapshot at different
points in the log, so a node that restored from a snapshot would differ
from one that replayed past the same index.

The test is the property itself. One queue runs a random history straight
through. A second is replaced by a copy built from its own state at random
points along the way. They must end equal down to lease IDs and the order
of every list (`TestQueueRebuiltFromStateBehavesTheSame`, 30 seeds of 400
steps).

**Every node snapshots on its own schedule.** A node counts the entries it
has applied since its last snapshot and takes another at 16,384. Leaders
do not coordinate it and followers do not wait for it. A snapshot holds
only committed state, so it needs nobody's agreement.

**A few thousand entries are kept behind each snapshot.** Dropping the log
right up to the snapshot would mean a follower one entry behind at that
moment could only be caught up by sending it the entire queue. Keeping the
last 4,096 entries means a follower has to be that far behind before the
snapshot is cheaper than the log.

**Compaction on disk is a rewrite, made atomic by ordering.** The log is
append-only segments, and the entries worth keeping sit in the middle of
old ones. So compaction writes the snapshot to its own file, starts a
fresh segment, writes the kept entries into it again marked as staged,
then writes one record that swaps the staged entries in for the whole log.
Only after that record is on disk are the old segments deleted.

A crash before the swap record leaves the old log in force and the staged
entries ignored. A crash after it leaves the new one. Old segments are
deleted newest first, so a crash part way leaves a prefix of the old log,
which replays cleanly and is then replaced by the swap. Deleting oldest
first could leave a middle with no beginning. Each of those crash points
has a test that rebuilds the directory as the crash would have left it.

**A node whose snapshot is unreadable refuses to start.** Its log begins
after the snapshot. Starting empty instead would let it vote as though it
had never held the entries it acknowledged. The error says so, and the
runbook says what to do.

**An enqueue carries a request ID, and the queue remembers it for five
minutes.** `jobqhttp.Client` puts a random ID on every enqueue and the
same one on every retry of it. The replicated queue keeps a table of
recent IDs; an enqueue whose ID is in the table adds nothing and returns
the first one's job ID.

The table is replicated state. It is in the snapshot, it is forgotten by
the queue's own clock (the leaders' timestamps, never a node's wall
clock), and forgetting happens while applying an enqueue, so every node
drops the same ID at the same log entry. The window is a constant for that
reason: it only has to outlast a client's retries, which span seconds.

This makes the enqueue happen once. It does not make delivery happen once,
and the queue still refuses to pretend it does. A single-node queue
ignores the ID: it has no retrying client in front of it.

**A leader that loses touch with a majority steps down.** R1's leader kept
believing it led until a higher term reached it. It could not commit
anything, so nothing was unsafe, but it answered "who leads?" wrongly for
as long as a partition lasted. That would have made the one alarm in this
milestone a lie: a cut-off leader would report that the cluster had a
leader. Now a leader that goes an election timeout without an answer from
a majority becomes a follower in the same term.

**Metrics are written by hand.** `GET /metrics` is the Prometheus text
format: a few lines of text. The root module still depends on nothing but
the standard library.

## Results

One Windows 11 laptop, all nodes on loopback.

### What snapshots change

`TestMeasureSnapshots` in `cluster`: one durable node, fsync on every
batch, 500,000 jobs of 256 bytes. Each is enqueued and nine in ten are
then leased and acked, so the log is 1,400,001 entries and the queue ends
up holding 50,000 jobs. One run of each.

| | No snapshots | Snapshot every 16,384 |
|---|---|---|
| Log on disk | 209.2 MB | 1.6 MB |
| Snapshot on disk | none | 16.5 MB |
| Restart until serving | 9.12 s | 0.11 s |
| Snapshots taken | 0 | 85 |

The snapshot is the size of what is queued; the log no longer depends on
how much has ever passed through.

The cost: taking that 16.5 MB snapshot stopped the node for 99 ms. The
slowest single job (enqueue, lease, ack) took 108 ms with snapshots and
67 ms without.

### Cluster crash harness

`TestClusterLosesNothingAcknowledgedAcrossLeaderKills` in `cmd/jobqd`,
now with `-snapshot-every 300 -snapshot-keep 50` so every run crosses
dozens of snapshots. Three real `jobqd` processes; the leader is killed
three times, then all three at once.

15 consecutive runs:

| | |
|---|---|
| Acknowledged enqueues | 19,912 |
| Leader kills, whole-cluster kills | 45, 15 |
| Snapshots taken, snapshots loaded | 549, 101 |
| Acknowledged enqueues lost | 0 |
| Acked jobs delivered again | 0 |
| Jobs on the nodes that no client was told about | 0 |

The last row is new. The harness now requires that the nodes hold no more
enqueues than were acknowledged plus calls that failed outright, so a
retry that added a second job would fail it.

**Does it catch anything?** With a loaded snapshot deliberately not
applied to the queue, the harness reported lost jobs in 3 of 3 runs (478,
363 and 493 of them).

### Raft simulation

The simulator now compacts logs at random, on every node independently,
and models the state machine as a running hash of applied entries, so a
snapshot's contents have exactly one right value. 10,000 seeds: 165,306
compactions, 7,821 snapshots installed from a leader, 34,374 restarts
from a snapshot, no safety violation. A fourth planted bug (installing a
snapshot older than what the node has committed) is caught.

### Do the tests bite?

Eleven things were broken on purpose, one at a time, and the named test
had to fail. Three were not caught at first: two tests were sharpened,
and one guard in the Raft code turned out to be redundant and was
deleted. All eleven are caught now. They include: a snapshot that omits the request ID
table, a restored topic that forgets a counter, staged entries from an
abandoned compaction leaking into the next, a compaction that does not
rewrite the term and vote, and a follower that keeps its old log when it
installs a snapshot.

## Two bugs the work found in older code

**A node killed while creating a log segment would not start again.** A
segment file is created and then its header is written and synced. Killed
in between, the node left a zero-byte file, and recovery called that
corruption. It dates from the write-ahead log in July; it took a test that
starts new segments constantly, which compaction does, to hit the window.
The cluster crash harness found it. A file with no usable header is now
treated as a torn write when it is the newest file and holds no records,
and as corruption anywhere else.

**A process that opened its log and died before writing anything would
not start again.** The next start tried to create a segment under the
name the empty one already had. Found by reading the code while adding
compaction; fixed by reusing the empty segment.

## What R3 does not do

- **The AWS deployment is untested.** See the note at the top.
- **A snapshot stops the node that takes it.** About 6 ms per megabyte
  here. At the default tick an election fires after 150 ms of silence, so
  a queue holding more than about 25 MB of jobs can have its leader look
  dead each time it snapshots. That threshold is arithmetic from one
  measurement, not something a test crossed. The cluster stays correct; the jobs in
  flight are redelivered. Writing the snapshot off the node's main loop is
  the fix and is not done.
- **The single-node queue's log still grows forever.** Compaction here is
  for a replicated node's Raft log. `jobq.Open` is untouched. The pieces a
  single-node checkpoint needs (`Queue.State`, a segment boundary on
  demand) now exist; the checkpoint does not.
- **A snapshot travels as one message and is held in memory.** Each node
  keeps its newest snapshot in memory to send to followers, beside the
  queue itself. A queue of several gigabytes does not fit this design.
- **No backups.** A node's data is its snapshot and log. The runbook's
  whole-cluster restore copies one node's disk to the others; anything
  committed after the copy is lost.
- **Membership is still fixed.** Replacing a node means bringing one up
  empty under the old ID, which is safe only if the operator first checks
  the other two agree. The software does not check.
- **An ack whose reply is lost still reports failure.** Request IDs cover
  enqueue only.
- **A request ID reused after five minutes is a new enqueue.**
- **No authentication on either port.** As R2.
- **Process kills are not power cuts.** As R2.
- **Nothing has left loopback.** No latency or throughput figure here
  says anything about a real network. R4.
