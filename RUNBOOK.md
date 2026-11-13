# jobq cluster runbook

How to bring up the three-node cluster in `deploy/terraform`, what its one
alarm means, and what to do when a node is lost.

**Status of this document.** The procedures under "Operating" are backed
by tests in this repository, named beside each one. The procedures under
"AWS" were written without an AWS account or a Terraform binary to run
them against. They have not been followed for real. Treat the first run as
a test of the runbook, and fix it where it is wrong.

## What you are running

Three `jobqd` processes, one per availability zone, that agree on one log
of queue operations. The cluster takes writes while any two can talk to
each other. It keeps two promises:

1. An enqueue acknowledged to the caller survives the loss of any one node.
2. An acked job never comes back.

Things it does not do, which shape everything below:

- **Membership is fixed.** There is no command to add or remove a node. A
  replacement takes the dead node's ID and address.
- **Neither port has authentication.** The security groups are the only
  thing between the queue and whoever can reach it.
- **There are no backups.** Each node holds a snapshot and a log. Lose two
  disks and what is on the third is all there is.

## AWS

### Before the first apply: the budget

The cluster bills by the hour for as long as it exists: three instances,
three public IPv4 addresses, three EBS volumes, and a small amount for the
CloudWatch metric and alarm. None of it is large. All of it adds up if it
is left running. Check current prices before you start; this document does
not quote any.

Apply the budget first, in its own directory, and leave it applied:

```
cd deploy/terraform/budget
terraform init
terraform apply -var email=you@example.com -var monthly_limit_usd=10
```

It emails at 50%, 80% and 100% of the limit. It alerts; it does not stop
anything, and billing data is hours behind.

### Bring the cluster up

Build `jobqd` for the instances and apply:

```
GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -o deploy/terraform/build/jobqd ./cmd/jobqd
cd deploy/terraform
terraform init
terraform apply -var alarm_email=you@example.com -var 'client_cidrs=["YOUR.IP.ADDRESS/32"]'
```

`client_cidrs` is who may reach the client API on port 8080. Leave it out
and only the nodes can; you then reach them through a Session Manager port
forward. It refuses `0.0.0.0/0`.

The no-leader alarm starts in ALARM, because nothing has reported yet, and
clears once the nodes are up. Expect one email each way. Confirm the SNS
subscription from your inbox or you will get neither.

Check it works, from a machine in `client_cidrs`:

```
export JOBQ_ADDR=$(terraform output -raw jobq_addr)
jobq cluster
jobq enqueue --payload hello work
jobq work --max-jobs 1 work -- cat
```

`jobq cluster` should list three nodes, one `leader` and two `follower`,
all naming the same leader, and exit 0.

### Take it down

```
cd deploy/terraform
terraform destroy
```

Then confirm nothing is left to bill. In the EC2 console for the region:
no instances, no volumes, no Elastic IPs. In S3: no bucket whose name
starts with the cluster name. In CloudWatch: no `jobq-no-leader` alarm.
The custom metric itself cannot be deleted; it stops costing anything when
nothing publishes to it and expires on its own.

The budget in `deploy/terraform/budget` costs nothing and is meant to stay.

### Getting a shell

There is no SSH. Use Session Manager:

```
aws ssm start-session --region eu-west-1 --target <instance id>
```

`terraform output instance_ids` lists them. On a node:

```
sudo systemctl status jobqd
sudo journalctl -u jobqd -n 100
curl -s localhost:8080/v1/cluster
curl -s localhost:8080/metrics
```

## Operating

### Find the leader

```
jobq --addr http://A:8080,http://B:8080,http://C:8080 cluster
```

It asks each node what it believes and prints one row per node. It exits 0
only when the nodes that answered agree on a leader and that leader
answered. An unreachable node is listed as `unreachable` with the error
underneath.

The columns that matter when something is wrong:

| Column | Meaning |
|---|---|
| `STATE`, `LEADER` | What this node thinks it is, and who it thinks leads. `none` during an election or when cut off. |
| `TERM` | Goes up by one at every election. Nodes on different terms have not heard from each other. |
| `COMMIT`, `LAST` | How far this node's log is known to be agreed, and how far it reaches. A follower far behind the leader's `LAST` is catching up. |
| `SNAPSHOT`, `FIRST` | The log index this node's snapshot covers, and the oldest log entry it still holds. A node whose `LAST` is below another's `FIRST` can only be caught up by being sent the snapshot. That happens on its own. |

### The alarm: `jobq-no-leader`

**What it means.** For ten seconds, no node reported knowing a leader, or
no node reported at all. The cluster is not taking writes. Reads from a
node that is up still answer, from that node's own copy.

Each node publishes `HasLeader` every five seconds: 1 if it knows of a
leader in its term, 0 if not. A leader that cannot reach a majority steps
down within two election timeouts and starts reporting 0, so a leader that
has been cut off does not hold the alarm open.

**What to do.**

1. Run `jobq cluster`. How many nodes answer?
2. **Two or three answer, no leader.** An election is not settling.
   Compare `TERM` across nodes. If it climbs every few hundred
   milliseconds, the nodes cannot hear each other on port 9090: check the
   security group still allows the nodes to reach each other, and that
   nothing changed in the route table. If the term is steady and nobody
   leads, read `journalctl -u jobqd` on each.
3. **One answers.** Two nodes are down or unreachable. One node is not a
   majority and will not lead. Get one of the other two back: see
   "A node will not start" and "Replace a node".
4. **None answer.** Check the instances are running. If they are, check
   `systemctl status jobqd` through Session Manager.
5. **All three answer and agree on a leader.** The cluster is fine and the
   metric is not arriving: check `systemctl status jobq-leader-metric` and
   that the instance role still allows `cloudwatch:PutMetricData`.

### A node restarts

Nothing to do. systemd restarts `jobqd`. It loads its snapshot, replays
the log after it, and rejoins. If the others compacted past what it holds
while it was away, the leader sends it a snapshot.

Every lease ends when the leader changes, so jobs that were in flight at
that moment are delivered again. That is within the queue's contract.

Tested by `TestClusterLosesNothingAcknowledgedAcrossLeaderKills` in
`cmd/jobqd`: three real processes, the leader killed three times and then
all three at once.

### A node will not start

Read `journalctl -u jobqd`. The messages that matter:

- **`... is locked by another process`**. Another `jobqd` has the
  directory, or one crashed and left its lock file. The systemd unit
  passes `-break-lock`, which removes a stale lock at start, so under
  systemd this should not appear. Outside systemd, check nothing else is
  running against the directory and remove `jobq.lock` by hand.
- **`snapshot ... is damaged`** or **`its snapshot cannot be read, so this
  node cannot start`**. The node's log begins after its snapshot and the
  snapshot is gone. The node refuses to start on purpose: starting empty
  would let it vote as if it had never held what it acknowledged. Its disk
  is lost. Go to "Replace a node".
- **`corruption in non-tail segment`**. A log file is damaged somewhere
  other than its end. Same answer: the disk is lost. Go to "Replace a
  node". (Damage at the very end of the newest file is a torn write from
  the crash, and is repaired without comment.)

### Replace a node

Use this when a node's instance or disk is gone for good. The replacement
comes up empty, under the dead node's ID and address.

**This is the dangerous procedure in this document.** A node that returns
with an empty disk has forgotten how it voted and what it held. Raft
assumes that never happens. It is safe here only because of the check in
step 1, and nothing in the software makes that check for you.

1. **Make sure the two survivors agree, before the empty node starts.**

   ```
   jobq --addr http://A:8080,http://B:8080 cluster
   ```

   Both must answer. One must be `leader` and the other a `follower`
   naming it. `TERM` must match, and `LAST` and `COMMIT` must match. If
   they do not, wait and run it again. Do not go on until they do.

   Why: an entry can be committed while it sits on only two of three
   nodes. If one of those two is the node you are about to replace, the
   other survivor may not have it yet. An empty node will vote for anyone,
   and its vote could then elect the survivor that is behind, which would
   discard the entry. Once the survivors hold the same log, there is no
   such entry.

2. **Replace it.** With Terraform, for node 3:

   ```
   terraform apply -replace='aws_instance.node["3"]'
   ```

   By hand: stop `jobqd`, delete everything in `/var/lib/jobq/data`, start
   it again.

3. **Watch it catch up.** `jobq cluster` with all three addresses. The new
   node appears as a `follower`; its `SNAPSHOT` becomes non-zero as the
   leader sends it one, and its `LAST` reaches the leader's.

Never replace two nodes at once, and never start the second replacement
before the first has caught up.

Tested in-process by `TestNodeReplacedWithAnEmptyDiskRejoins` in
`cluster`, which meets the precondition by waiting for it.

### Restore the whole cluster from one node's disk

The last resort: two disks are lost, or you want the cluster back at the
state of a copy you took earlier. Every node is given the same data
directory and the cluster starts again from it.

**You lose whatever was committed after that copy was taken.** If the copy
comes from a node that was behind, that includes acknowledged enqueues and
acks. There is no way to tell from the copy alone.

1. Stop `jobqd` on every node.
2. Pick the source: the surviving node, or your copy. If more than one
   node survives, use the one with the highest `LAST` in its final
   `jobq cluster` output or `raft log:` start-up line.
3. On every other node, delete `/var/lib/jobq/data` and replace it with a
   copy of the source's, `wal/` and `snap/` both. Do not copy `jobq.lock`.
   Make sure the files belong to the `jobq` user.
4. Start `jobqd` on all three. They hold identical logs, elect a leader,
   and carry on.

A node's data directory is a consistent copy only while `jobqd` is
stopped, or if the copy is taken as a single point-in-time snapshot of the
volume.

Tested in-process by `TestWholeClusterRestoredFromOneNodesDisk` in
`cluster`.

### Disk filling up

`jobq_raft_log_bytes` and `jobq_raft_snapshot_file_bytes` in `/metrics`
are what a node's data takes on disk. A node snapshots its queue every
16,384 log entries and deletes the log behind the snapshot, keeping the
last 4,096 entries. So the log stays small and the snapshot is the size of
what is actually queued: payloads of jobs not yet acked, plus bookkeeping.

If the disk is filling, the queue is filling. Look at
`jobq_topic_ready` per topic. Dead-letter topics (`<topic>.dlq`) hold jobs
until something consumes them.

If `jobq_raft_first_index` is 1 on a node that has applied far more than
16,384 entries, it is not snapshotting. Check it was not started with a
negative `-snapshot-every`.

### Snapshots stall a node

Taking a snapshot stops that node until the snapshot is on disk: no
heartbeats, no replies. On the laptop this was measured on, a 16.5 MB
snapshot took about 100 ms. `jobq_snapshot_last_duration_seconds` is the
figure for your node.

If that figure approaches the election timeout (150 ms at the default
`-tick 10ms`), a leader taking a snapshot can look dead to its followers,
and they will elect another. The cluster stays correct, but every leader
change redelivers the jobs in flight. If `jobq_raft_leader_changes_total`
climbs in step with `jobq_snapshots_total`, raise `-tick` on every node.

## Metrics worth watching

All from `GET /metrics` on each node, Prometheus text format.

| Metric | Watch for |
|---|---|
| `jobq_raft_has_leader` | 0 for more than a few seconds |
| `jobq_raft_leader_changes_total` | climbing when nothing was restarted |
| `jobq_raft_follower_lag_entries` | (leader only) a follower that stays behind |
| `jobq_raft_commit_duration_seconds` | (leader only) the cost of a write: fsync plus a round trip to one other node |
| `jobq_topic_ready`, `jobq_topic_in_flight` | queue depth per topic |
| `jobq_snapshot_last_duration_seconds` | approaching the election timeout |
| `jobq_deduplicated_enqueues_total` | climbing means clients are retrying enqueues whose replies they lost |
