package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/aleks-drozy/jobq"
	"github.com/aleks-drozy/jobq/jobqhttp"
)

// newSubFlagSet builds a FlagSet for one subcommand that also accepts the
// three global flags, so `jobq enqueue --addr X topic` and
// `jobq --addr X enqueue topic` both work.
func newSubFlagSet(name string, g *globalFlags, stderr io.Writer) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	g.register(fs)
	return fs
}

func fail(stderr io.Writer, format string, args ...any) int {
	fmt.Fprintf(stderr, "jobq: "+format+"\n", args...)
	return exitError
}

// --- enqueue -------------------------------------------------------------

func cmdEnqueue(g *globalFlags, args []string, newClient clientFactory, stdout, stderr io.Writer) int {
	fs := newSubFlagSet("enqueue", g, stderr)
	payloadFlag := fs.String("payload", "", "payload as a literal string")
	fileFlag := fs.String("file", "", "read payload from this file")
	delay := fs.Duration("delay", 0, "delay before the job becomes visible")
	maxAttempts := fs.Int("max-attempts", 0, "max delivery attempts (0 = server default)")
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	rest := fs.Args()
	if len(rest) < 1 {
		fmt.Fprintln(stderr, "usage: jobq enqueue [--payload <str> | --file <path>] [--delay 5s] [--max-attempts 3] <topic> [-]")
		return exitUsage
	}
	topic := rest[0]

	payload, err := resolvePayload(*payloadFlag, *fileFlag, rest[1:], os.Stdin)
	if err != nil {
		return fail(stderr, "%v", err)
	}

	var opts []jobq.EnqueueOption
	if *delay > 0 {
		opts = append(opts, jobq.WithDelay(*delay))
	}
	if *maxAttempts > 0 {
		opts = append(opts, jobq.WithMaxAttempts(*maxAttempts))
	}

	c := newClient(0)
	id, err := c.Enqueue(topic, payload, opts...)
	if err != nil {
		return fail(stderr, "enqueue: %v", err)
	}
	if g.json {
		return printJSON(stdout, stderr, map[string]string{"id": id})
	}
	fmt.Fprintln(stdout, id)
	return exitOK
}

// resolvePayload implements --payload / --file / stdin ("-" or nothing left
// over) as a pure, testable function: exactly one source may be given.
func resolvePayload(payloadFlag, fileFlag string, positional []string, stdin io.Reader) ([]byte, error) {
	sources := 0
	if payloadFlag != "" {
		sources++
	}
	if fileFlag != "" {
		sources++
	}
	useStdin := len(positional) > 0 && positional[0] == "-"
	if useStdin {
		sources++
	}
	if sources > 1 {
		return nil, errors.New("only one of --payload, --file, or - (stdin) may be given")
	}
	switch {
	case payloadFlag != "":
		return []byte(payloadFlag), nil
	case fileFlag != "":
		return os.ReadFile(fileFlag)
	case useStdin:
		return io.ReadAll(stdin)
	default:
		// No source named: read stdin if it looks piped, otherwise empty
		// payload is allowed (a valid, if unusual, job).
		return io.ReadAll(stdin)
	}
}

// --- dequeue ---------------------------------------------------------------

func cmdDequeue(g *globalFlags, args []string, newClient clientFactory, stdout, stderr io.Writer) int {
	fs := newSubFlagSet("dequeue", g, stderr)
	visibility := fs.Duration("visibility", 30*time.Second, "lease visibility timeout")
	wait := fs.Duration("wait", 0, "long-poll wait for a job to become ready")
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	rest := fs.Args()
	if len(rest) != 1 {
		fmt.Fprintln(stderr, "usage: jobq dequeue [--visibility 30s] [--wait 10s] <topic>")
		return exitUsage
	}
	topic := rest[0]

	c := newClient(*wait)
	job, lease, err := c.DequeueWait(topic, *visibility, *wait)
	if errors.Is(err, jobq.ErrNoJob) {
		if !g.json {
			fmt.Fprintln(stderr, "jobq: no job available")
		}
		return exitNoJob
	}
	if err != nil {
		return fail(stderr, "dequeue: %v", err)
	}
	if g.json {
		return printJSON(stdout, stderr, dequeueOutput{Job: job, Lease: lease})
	}
	fmt.Fprintf(stdout, "job:   %s\n", job.ID)
	fmt.Fprintf(stdout, "topic: %s\n", job.Topic)
	fmt.Fprintf(stdout, "lease: %s\n", lease.ID)
	fmt.Fprintf(stdout, "attempt: %d/%d\n", job.Attempt, job.MaxAttempts)
	fmt.Fprintf(stdout, "deadline: %s\n", lease.Deadline.Format(time.RFC3339))
	fmt.Fprintln(stdout, "payload:")
	stdout.Write(job.Payload)
	fmt.Fprintln(stdout)
	return exitOK
}

type dequeueOutput struct {
	Job   *jobq.Job  `json:"job"`
	Lease jobq.Lease `json:"lease"`
}

// --- ack / nack --------------------------------------------------------

func cmdAck(g *globalFlags, args []string, newClient clientFactory, stdout, stderr io.Writer) int {
	return settleCmd("ack", g, args, newClient, stdout, stderr, func(c *jobqhttp.Client, l jobq.Lease) error { return c.Ack(l) })
}

func cmdNack(g *globalFlags, args []string, newClient clientFactory, stdout, stderr io.Writer) int {
	return settleCmd("nack", g, args, newClient, stdout, stderr, func(c *jobqhttp.Client, l jobq.Lease) error { return c.Nack(l) })
}

func settleCmd(name string, g *globalFlags, args []string, newClient clientFactory, stdout, stderr io.Writer, do func(*jobqhttp.Client, jobq.Lease) error) int {
	fs := newSubFlagSet(name, g, stderr)
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	rest := fs.Args()
	if len(rest) != 2 {
		fmt.Fprintf(stderr, "usage: jobq %s <topic> <leaseID>\n", name)
		return exitUsage
	}
	lease := jobq.Lease{Topic: rest[0], ID: rest[1]}
	c := newClient(0)
	if err := do(c, lease); err != nil {
		return fail(stderr, "%s: %v", name, err)
	}
	if g.json {
		return printJSON(stdout, stderr, map[string]bool{"ok": true})
	}
	fmt.Fprintln(stdout, "ok")
	return exitOK
}

// --- extend --------------------------------------------------------------

func cmdExtend(g *globalFlags, args []string, newClient clientFactory, stdout, stderr io.Writer) int {
	fs := newSubFlagSet("extend", g, stderr)
	by := fs.Duration("by", 0, "extend the lease by this duration")
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	rest := fs.Args()
	if len(rest) != 2 {
		fmt.Fprintln(stderr, "usage: jobq extend --by 30s <topic> <leaseID>")
		return exitUsage
	}
	lease := jobq.Lease{Topic: rest[0], ID: rest[1]}
	c := newClient(0)
	deadline, err := c.Extend(lease, *by)
	if err != nil {
		return fail(stderr, "extend: %v", err)
	}
	if g.json {
		return printJSON(stdout, stderr, map[string]string{"deadline": deadline.Format(time.RFC3339Nano)})
	}
	fmt.Fprintln(stdout, deadline.Format(time.RFC3339))
	return exitOK
}

// --- stats / topics / health / recovery ---------------------------------

func cmdStats(g *globalFlags, args []string, newClient clientFactory, stdout, stderr io.Writer) int {
	fs := newSubFlagSet("stats", g, stderr)
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	rest := fs.Args()
	if len(rest) > 1 {
		fmt.Fprintln(stderr, "usage: jobq stats [--json] [topic]")
		return exitUsage
	}
	c := newClient(0)

	if len(rest) == 1 {
		st, err := c.TopicStats(rest[0])
		if err != nil {
			return fail(stderr, "stats: %v", err)
		}
		if g.json {
			return printJSON(stdout, stderr, st)
		}
		fmt.Fprint(stdout, formatStatsTable(map[string]jobq.Stats{rest[0]: st}))
		return exitOK
	}

	all, err := c.Stats()
	if err != nil {
		return fail(stderr, "stats: %v", err)
	}
	if g.json {
		return printJSON(stdout, stderr, all)
	}
	fmt.Fprint(stdout, formatStatsTable(all))
	return exitOK
}

func cmdTopics(g *globalFlags, args []string, newClient clientFactory, stdout, stderr io.Writer) int {
	fs := newSubFlagSet("topics", g, stderr)
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	c := newClient(0)
	topics, err := c.Topics()
	if err != nil {
		return fail(stderr, "topics: %v", err)
	}
	if g.json {
		return printJSON(stdout, stderr, topics)
	}
	for _, t := range topics {
		fmt.Fprintln(stdout, t)
	}
	return exitOK
}

func cmdHealth(g *globalFlags, args []string, newClient clientFactory, stdout, stderr io.Writer) int {
	fs := newSubFlagSet("health", g, stderr)
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	c := newClient(0)
	status, durable, uptime, err := c.Health()
	if err != nil {
		return fail(stderr, "health: %v", err)
	}
	if g.json {
		return printJSON(stdout, stderr, map[string]any{"status": status, "durable": durable, "uptime_ms": uptime.Milliseconds()})
	}
	fmt.Fprintf(stdout, "status:  %s\n", status)
	fmt.Fprintf(stdout, "durable: %v\n", durable)
	fmt.Fprintf(stdout, "uptime:  %s\n", uptime.Round(time.Second))
	return exitOK
}

func cmdRecovery(g *globalFlags, args []string, newClient clientFactory, stdout, stderr io.Writer) int {
	fs := newSubFlagSet("recovery", g, stderr)
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	c := newClient(0)
	report, startedAt, dir, sync, err := c.Recovery()
	if err != nil {
		return fail(stderr, "recovery: %v", err)
	}
	if g.json {
		return printJSON(stdout, stderr, map[string]any{
			"report": report, "started_at": startedAt, "dir": dir, "sync": sync,
		})
	}
	// started_at is named explicitly, not "now": this is a process-start
	// snapshot, not live state (see jobqhttp package doc).
	fmt.Fprintf(stdout, "started_at: %s\n", startedAt.Format(time.RFC3339))
	fmt.Fprintf(stdout, "dir:        %s\n", dir)
	fmt.Fprintf(stdout, "sync:       %s\n", sync)
	fmt.Fprintf(stdout, "%+v\n", report)
	return exitOK
}

func printJSON(stdout, stderr io.Writer, v any) int {
	enc := json.NewEncoder(stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		return fail(stderr, "encoding json: %v", err)
	}
	return exitOK
}
