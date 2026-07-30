# Demo: the agent diagnoses a crash it did not witness

`demo/main.go` runs the crash-recovery-diagnosis scenario from
[`docs/specs/2026-07-31-jobq-p3.5-mcp.md`](../../docs/specs/2026-07-31-jobq-p3.5-mcp.md)
end to end, for real:

1. Builds the real `jobqd` and `jobq-mcp` binaries from source.
2. Starts `jobqd -dir <tmp>/data -sync always` and seeds a real queue:
   6 poison jobs that fail for real and get dead-lettered *before* the
   crash, 5 jobs left leased (in flight) at kill time, and 30 ordinary jobs
   (20 acked, 10 left ready).
3. Kills `jobqd` **cold** — `Process.Kill`, no signal, no flush, no graceful
   shutdown.
4. Removes the stale lock file the cold kill left behind (`acquireDirLock`
   only detects a *live* holder — a crash's lock is cleared by hand, on
   purpose; see `wal.go`) and restarts `jobqd` on the same directory.
5. **Recording starts here.** Spawns the real `jobq-mcp` binary as a
   subprocess and connects a real MCP client to it over real stdio — the
   same transport a Claude Desktop or Claude Code session would use.
6. Drives that client through the tool sequence an agent would use, given
   nothing but the prompt: *"Something happened to the queue. Find out
   what, and get the work moving again without losing anything."*

Every number the program prints comes from that run. `transcript.txt` in
this directory is one real, captured run — not a canned example. Re-run it
yourself and you'll get different job IDs, timestamps, and ports, but the
same shape: the same interpretation lines, the same DLQ partition, the same
reconciliation.

## Running it

```sh
cd mcp
go run ./demo            # deletes its working directory on exit
go run ./demo -keep      # prints the working directory instead of deleting it
```

No flags are required; `-root` and `-mcp-dir` exist only if you're running
the demo from somewhere other than the `mcp/` directory (they default to
`..` and `.`, i.e. "the jobq root module is the parent of this one").

## The moment that matters

The agent partitions the dead-letter queue using `dead_lettered_at`:

- Jobs with a **real timestamp** failed for real, in the application,
  *before* the crash — a human should look at these.
- Jobs with a **zero timestamp** were dead-lettered by *replay*: they were
  in flight when the process died, with their last attempt already spent.
  `recover.go` sets `DeadLetteredAt = time.Time{}` for exactly this case,
  with the comment *"unknown crash time; zero is honest."*

The agent can tell crash victims from poison because the engine refused to
fabricate a timestamp it did not have. That's a durability decision paying
off as an agent capability, and — because `jobq_peek` is read-only — the
agent can look as many times as it wants while deciding, without spending
any of those jobs' attempt budget to do it.

## A caveat worth knowing before you read the transcript

`Stats.Enqueued` is **not** a cumulative, all-time counter that survives a
restart. Each topic actor seeds it from the jobs replay restored
(`stats.Enqueued = len(ready)` in `topic.go`), then counts new enqueues from
there. So `jobq_topic_stats` right after a restart reports "enqueued" as
*what's live in this process's lifetime*, not "everything that was ever
enqueued to this topic." The closing reconciliation in the demo
(`enqueued == acked + ready + in_flight + dead_letters + dropped`) still
holds exactly, because it's internally consistent with that definition —
just don't read "enqueued: 10" after a crash as "only 10 jobs were ever
enqueued here."

## What isn't simulated

The crash is real (`Process.Kill`), the WAL replay is real, the lock file
is real, the MCP transport is real. What's scripted is the *sequence* of
tool calls — this program plays the agent's part deterministically rather
than putting an actual LLM in the loop, so the transcript is reproducible
and doesn't depend on having model API access to regenerate. The tool
outputs it reacts to, and the numbers embedded in the running text, are not
scripted; `printReconciliation` genuinely fails the program if conservation
doesn't hold.
