package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/aleks-drozy/jobq"
	"github.com/aleks-drozy/jobq/jobqhttp"
)

// cmdWork is the centerpiece and the answer to "there is no runnable demo":
// long-poll dequeue -> run <cmd> with the payload on stdin and
// JOBQ_JOB_ID/JOBQ_TOPIC/JOBQ_ATTEMPT/JOBQ_MAX_ATTEMPTS in the environment
// -> exit 0 acks, non-zero nacks. Point it at a script that fails on
// purpose and watch retries burn the attempt budget and the job land in
// "<topic>.dlq", live, in a terminal.
func cmdWork(g *globalFlags, args []string, newClient clientFactory, stdout, stderr io.Writer) int {
	fs := newSubFlagSet("work", g, stderr)
	visibility := fs.Duration("visibility", 30*time.Second, "lease visibility timeout")
	wait := fs.Duration("wait", 20*time.Second, "long-poll wait per dequeue attempt")
	maxJobs := fs.Int("max-jobs", 0, "stop after this many jobs (0 = run forever)")
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	rest := fs.Args()

	dashIdx := -1
	for i, a := range rest {
		if a == "--" {
			dashIdx = i
			break
		}
	}
	if dashIdx < 0 || dashIdx == 0 || dashIdx == len(rest)-1 {
		fmt.Fprintln(stderr, "usage: jobq work [--visibility 30s] [--wait 20s] [--max-jobs N] <topic> -- <cmd> [args...]")
		return exitUsage
	}
	topic := rest[0]
	command := rest[dashIdx+1:]

	c := newClient(*wait)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	return runWorkLoop(ctx, c, topic, *visibility, *wait, *maxJobs, command, stdout, stderr)
}

// runWorkLoop is the testable core of cmdWork: it takes an already-built
// client and a cancellable context instead of touching flags, signals, or
// os.Args, so tests drive it against an httptest server and a fake command.
func runWorkLoop(ctx context.Context, c *jobqhttp.Client, topic string, visibility, wait time.Duration, maxJobs int, command []string, stdout, stderr io.Writer) int {
	processed := 0
	for {
		if maxJobs > 0 && processed >= maxJobs {
			return exitOK
		}
		select {
		case <-ctx.Done():
			return exitOK
		default:
		}

		job, lease, err := c.DequeueWait(topic, visibility, wait)
		if errors.Is(err, jobq.ErrNoJob) {
			continue
		}
		if err != nil {
			fmt.Fprintf(stderr, "jobq work: dequeue: %v\n", err)
			return exitError
		}

		settled := runOneJob(ctx, c, job, lease, command, stdout, stderr)
		processed++
		if !settled {
			return exitError
		}
	}
}

// runOneJob executes command once for job, then settles the lease: exit 0
// acks, non-zero nacks. A SIGINT/SIGTERM (ctx cancelled) while the command
// is running nacks the in-flight job instead of abandoning it to lease
// expiry, so the queue learns about it immediately rather than waiting out
// the visibility timeout. It returns false only on a settle-call failure
// (network/server error), not on the job's own exit code.
func runOneJob(ctx context.Context, c *jobqhttp.Client, job *jobq.Job, lease jobq.Lease, command []string, stdout, stderr io.Writer) bool {
	cmd := exec.Command(command[0], command[1:]...)
	cmd.Stdin = bytes.NewReader(job.Payload)
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	cmd.Env = append(os.Environ(),
		"JOBQ_JOB_ID="+job.ID,
		"JOBQ_TOPIC="+job.Topic,
		"JOBQ_ATTEMPT="+strconv.Itoa(job.Attempt),
		"JOBQ_MAX_ATTEMPTS="+strconv.Itoa(job.MaxAttempts),
	)

	runErr := make(chan error, 1)
	if err := cmd.Start(); err != nil {
		fmt.Fprintf(stderr, "jobq work: starting command: %v\n", err)
		return settleErr(c, lease, false, stderr)
	}
	go func() { runErr <- cmd.Wait() }()

	select {
	case <-ctx.Done():
		_ = cmd.Process.Kill()
		<-runErr
		fmt.Fprintf(stderr, "jobq work: interrupted, nacking job %s\n", job.ID)
		return settleErr(c, lease, false, stderr)
	case err := <-runErr:
		success := err == nil
		return settleErr(c, lease, success, stderr)
	}
}

func settleErr(c *jobqhttp.Client, lease jobq.Lease, success bool, stderr io.Writer) bool {
	var err error
	if success {
		err = c.Ack(lease)
	} else {
		err = c.Nack(lease)
	}
	if err != nil {
		verb := "ack"
		if !success {
			verb = "nack"
		}
		fmt.Fprintf(stderr, "jobq work: %s: %v\n", verb, err)
		return false
	}
	return true
}
