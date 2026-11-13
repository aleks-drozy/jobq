# jobq R4, first half: the fault matrix, run locally

Status: built 2026-11-13 on `feat/chaos-harness`. R4 is the last of four
replication milestones (R1 Raft core, R2 the queue on Raft, R3 snapshots
and AWS, R4 fault injection on real machines).

**This is not R4.** R4 is the fault matrix on three machines in three
availability zones. This is the harness that will run it, and the same
matrix run against three nodes in network namespaces on one laptop. The
network between those nodes is a bridge inside one kernel and the three
"disks" are one disk. Every number below is from that, and none of them
says what a real network or three real disks would do. The AWS half of R3
is still unrun, so there are still no machines to point this at.

## What it is

`cmd/jobqchaos` runs producers and consumers against a cluster through
`jobqhttp.Client`, the way any worker pool would, and injects one fault at
a time while they run. A fault is held, undone, and repeated. When a fault
has had its runs the producers stop, the topic is drained, and the two
rules are checked from the client's side of the wire, with the same
accounting as the crash harness in `cmd/jobqd`:

- **Lost** (forbidden): an enqueue the client saw acknowledged that was
  never delivered and completed.
- **Ghost** (forbidden): a job whose ack the client saw acknowledged,
  delivered again.
- **Duplicates** (permitted): jobs delivered more than once. Counted.

It then checks the nodes against the clients: all three hold the same
counts, the counts add up, they hold every acknowledged enqueue and no
more extra jobs than there were enqueue calls that failed.

Each fault gets its own topic and its own workload, so a loss is
attributed to the fault that caused it.

| Fault | What is done | Undone by |
|---|---|---|
| `leader-kill` | the leader's `jobqd` gets SIGKILL | starting it on the same directory |
| `follower-kill` | a follower's `jobqd` gets SIGKILL | starting it |
| `leader-isolate` | the leader is cut off from the other two nodes; clients still reach it | restoring the routes |
| `follower-isolate` | a follower is cut off the same way | restoring the routes |
| `slow-network` | every node's interface: 100 ms delay, 5% loss | removing it |

The plan's fifth fault, a whole instance stopped and started, is an EC2
operation and has no honest local stand-in. It is not in the matrix.

## Decisions

**The harness never touches a node.** Everything it does to one goes
through a hook: an executable run as `HOOK VERB NODE-ID`, with six verbs
(`kill`, `start`, `isolate`, `heal`, `slow`, `fast`). `chaos/netns.sh` is
that hook for the local lab. The matrix runs against real machines by
writing the same six verbs over SSH, and nothing in `jobqchaos` changes.
That is the point of the split: the part that decides whether the contract
held is the part that has already been run.

**A fault that was not seen fails the run.** Counts of zero lost and zero
ghosts mean nothing if the fault never landed. A hook whose `iptables` or
`tc` line quietly failed would produce a perfect report. So each fault
carries its own test of whether it took effect, from what the nodes said
at the end of the hold: a killed node must not answer; a killed or cut-off
leader must have been replaced by another node in a later term; a cut-off
follower must have stopped following; a slow network must have made
writes at least three times slower. `TestHarnessFailsWhenTheFaultIsNotApplied`
runs the whole matrix with a hook that reports success and does nothing,
and requires the report to fail on every fault for that reason and no
other.

**The local lab is network namespaces, and a partition is a route.** Each
node runs in its own namespace on a bridge; the harness sits on the host
side of the bridge, so it reaches every node whatever the nodes can say to
each other. Cutting a node off is two blackhole routes inside its
namespace, one per peer. Packets to the peers are dropped as they leave,
which breaks both directions of every connection between them and leaves
the path to clients alone. The plan said `iptables`; routes need nothing
installed, touch nothing outside the namespace, and do the same job. Slow
and lossy is `tc netem` on the node's interface, as planned.

**The workload is closed-loop, and that shapes one of the numbers.** Each
producer makes one call and waits for it before making the next, as a
worker does. So "writes stopped" here means every producer was stuck in a
call, not that the cluster would have refused a new caller. See the
leader-isolate finding below, where the difference matters.

**Three timings, all from before the fault.** The clock starts just before
the hook is called, because the moment the fault lands inside the hook
cannot be known from outside. Each figure therefore includes the time the
hook took and is an upper bound.

- *First write after the fault*: to the first acknowledgement of a write
  that was sent after the hook had returned. A write sent while the hook
  was still running may have been answered by a node that was about to
  die. The first version of the harness counted those and reported a
  25 ms failover for a killed leader; the election alone took 220 ms.
- *Longest stall*: the longest stretch with no write acknowledged, from
  the fault until the run ended. This catches a stall that comes when the
  fault is undone, not when it is applied.
- *New leader seen*: to the first time any node said it led in a later
  term, polled every 5 ms.

## What the harness is tested with

`go test ./cmd/jobqchaos` runs without root on any platform:

- The verdict is a pure function of what the clients were told and what
  the nodes hold. Seven breakages are planted in a clean run one at a
  time (an acknowledged enqueue that vanished, an acked job delivered
  again, a retry that enqueued twice, node counts that do not add up, and
  so on) and each must be named.
- Each fault's own test of whether it landed must say no when nothing
  happened.
- The harness runs end to end against three real nodes in the test
  process, each with its own directory and listeners, through leader and
  follower kills. The kill there closes the node; it is not SIGKILL. It
  tests the harness, not the queue.
- The do-nothing hook, above.

## What was run

One laptop, 2026-11-13. WSL2 (Ubuntu 26.04, kernel 6.18), three `jobqd`
nodes in network namespaces on one bridge, `-sync always`, all three data
directories on the same virtual disk. Three producers, four consumers.
Each fault held for 3 s, then 2 s of quiet before the next. Linux binaries
cross-compiled on Windows; nothing was installed in WSL.

Three passes: the matrix at ten runs per fault, then two follow-ups to
questions the matrix raised. Every run of each is in
`docs/results/2026-11-13-chaos-local/`, as the harness wrote it.

| Pass | Fault | Runs | Fault seen | Acknowledged enqueues | Lost | Ghosts | Duplicates |
|---|---|---|---|---|---|---|---|
| matrix | `leader-kill` | 10 | 10 | 6,349 | 0 | 0 | 9 |
| matrix | `follower-kill` | 10 | 10 | 5,493 | 0 | 0 | 0 |
| matrix | `leader-isolate` | 10 | 10 | 1,523 | 0 | 0 | 19 |
| matrix | `follower-isolate` | 10 | 10 | 3,675 | 0 | 0 | 26 |
| matrix | `slow-network` | 10 | 10 | 1,635 | 0 | 0 | 27 |
| hold 8 s | `leader-isolate` | 5 | 5 | 1,928 | 0 | 0 | 14 |
| `-tick 50ms` | `slow-network` | 5 | 5 | 898 | 0 | 0 | 2 |
| **all** | | **60** | **60** | **21,501** | **0** | **0** | **97** |

The contract held in every pass. The nodes agreed with each other and with
the clients every time. In the matrix the nodes were restarted 20 times,
took 9 snapshots and loaded 11.

Timings, in milliseconds, as median / maximum across the runs. With ten
runs a 99th percentile is the maximum, so it is not quoted. The first
three columns are timed from just before the fault was injected and
include the time injecting it took (last column).

| Pass | Fault | First write after the fault | Longest stall | New leader seen | Whole again after undo | Write p50, before -> during | Injecting took |
|---|---|---|---|---|---|---|---|
| matrix | `leader-kill` | 406 / 1,731 | 349 / 1,586 | 253 / 1,437 (10 of 10) | 175 / 330 | 19 -> 16 | 66 |
| matrix | `follower-kill` | 105 / 237 | 36 / 112 | never | 220 / 390 | 22 -> 17 | 79 |
| matrix | `leader-isolate` | 3,397 / 3,832 | 3,316 / 3,786 | 376 / 1,039 (10 of 10) | 364 / 815 | 36 -> 3,281 | 31 |
| matrix | `follower-isolate` | 77 / 108 | 401 / 742 | 3,456 / 3,776 (10 of 10) | 481 / 814 | 37 -> 36 | 30 |
| matrix | `slow-network` | 3,635 / 4,246 | 3,342 / 3,984 | 3,562 / 4,245 (10 of 10) | 507 / 1,138 | 35 -> 3,017 | 77 |
| hold 8 s | `leader-isolate` | 5,046 / 5,053 | 5,022 / 5,027 | 245 / 1,051 (5 of 5) | 370 / 494 | 36 -> 36 | 28 |
| `-tick 50ms` | `slow-network` | 940 / 1,164 | 744 / 2,496 | 5,336 (1 of 5) | 65 / 2,159 | 36 -> 397 | 81 |

## What the runs found

None of these breaks the contract. All four are about how long clients
wait, and the first is not what the design intends.

**1. A leader that is cut off stalls its clients for five seconds, not for
one election.** With the leader cut off from the other nodes but still
reachable by clients, another node led within 376 ms (median). Clients
did not get a write through until the partition was healed, 3.3 s later.
Held for 8 s instead, the stall was 5.02 s in five runs out of five.

Five seconds is `cluster.Config.Timeout`, how long a node waits for a
proposal to commit. Every producer had a call in flight on the old leader
when it was cut off. That leader stepped down within an election timeout,
as R3 made it, but stepping down does not answer the calls already
waiting: each sits until the timeout and comes back as "unavailable,
outcome unknown". Only the call after that is told `not_leader` and finds
the new leader. In the 8 s pass all 15 such calls failed that way, and 6 of
the jobs existed afterwards: they had reached a follower before the cut
and the new leader committed them. So "unknown" is the honest answer; the
cost is that it takes five seconds to give.

The stall is the proposal timeout, so it does not grow with the length of
the partition. A caller arriving fresh during it would be redirected at
once; this workload has none, since all its producers were already stuck.

Not changed here. Two ways to shorten it, each a decision about the
product and not about the harness: have a node that steps down answer its
waiting proposals as unknown at once, or have the client retry an
unavailable enqueue on another node, which request IDs already make safe.

**2. A follower returning from a partition unseats the leader, every
time.** Cutting a follower off costs nothing while it is away (first
write 77 ms, no slower than normal). But it stands for election about
five times a second on its own, so it comes back some 15 terms ahead and
the leader steps down for it. In ten runs out of ten that caused an
election on return, with a longest stall of 401 ms median and 742 ms
worst. This is the missing pre-vote, already on the watch list since R1,
now with a number.

**3. Over a 100 ms, 5% loss link the default tick cannot hold a leader.**
The default Raft tick is 10 ms: heartbeats every 30 ms, elections after
150 to 300 ms of silence. A round trip between nodes on that link is 200
ms before any loss. The cluster changed term 15 to 19 times in each 3 s
hold and acknowledged between zero and six writes in it; in three runs of
ten, none. With `-tick 50ms` (elections after 750 to 1,500 ms) the leader
was kept in four runs of five and writes went through at a median of 397
ms each, with a longest stall of 521 to 910 ms in those four. The fifth
was the first run after start-up and saw four changes of term.

This is the timing requirement of Raft, not a defect, and no link between
availability zones is that slow. It does mean `-tick` must be set for the
network, and that nothing tells the operator when it is set wrong except
`jobq_raft_leader_changes_total` climbing.

**4. One election in ten took 1.4 seconds.** Killing the leader normally
cost one new term and about 250 ms. One run went through seven terms and
took 1,437 ms to elect, with two healthy nodes and nothing wrong between
them. Not investigated. Two candidates out of step is the usual cause of
repeated terms and randomised timeouts are meant to make it rare.

## The harness catches a real violation

A `jobqd` was built with one line of `raft/` changed, in a scratch copy,
so that a leader counts an entry as committed once it is in its own log.
Against that build, with the leader cut off, the harness failed three
passes out of three (exit status 1). In the pass where the surviving
nodes drained fully they held 19,532 enqueues against 19,598 acknowledged:
66 lost, and 60 ghosts. In all three the cut-off node stopped answering
after the partition healed and the cluster was never whole again, which
the harness also reports.

## How to run it

On any Linux with root, iproute2 and the `sch_netem` module:

```
GOOS=linux go build -o /var/tmp/jobq-chaos/bin/ ./cmd/jobqd ./cmd/jobqchaos
sudo chaos/local.sh -runs 10
```

`local.sh` builds the lab, runs the matrix, and takes the lab down
whatever happens. Results go to `/var/tmp/jobq-chaos/results` as
`results.json` (every run) and `results.md` (the tables). The exit status
is 0 only if the contract held and every fault was seen.

Against a cluster that already exists, `jobqchaos -nodes ... -hook ...`
with a hook that implements the six verbs for it.

## What this does not show

- **Anything about a real network or real disks.** One kernel, one bridge,
  one disk. Every timing here is this laptop's. In particular the ordinary
  write took 19 to 37 ms at the median, most of it three nodes fsyncing to
  one virtual disk, and it sat near 20 ms in some stretches and near 36 ms
  in others for reasons not looked into.
- **Rare events.** Ten runs per fault. A failure that needs a one-in-a-
  thousand interleaving was not given the chance.
- **A power cut.** SIGKILL loses nothing the kernel already has. As R2 and
  R3.
- **A stopped machine.** The EC2 stop/start fault is not here.
- **The price of replication.** The plan's comparison of one node, three
  nodes in one zone and three across zones was not attempted: only the
  last two would mean anything, and both need machines.
- **A partition of clients from nodes.** Clients reach every node
  throughout. Only the nodes are cut from each other.
- **The hook for real machines.** Not written. Writing it before there is
  a machine to run it on would repeat R3's mistake.
- **CI.** The lab needs root and netem and runs nowhere but here so far.
  CI runs the harness's own tests, not the matrix.
