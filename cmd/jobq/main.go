// Command jobq is the HTTP-only CLI for jobqd. It is HTTP-only, never
// local-dir, on purpose: Open takes an exclusive directory lock, so a CLI
// opening the daemon's data directory would fail outright — one code path,
// and the failure mode is explained instead of hit.
package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/aleks-drozy/jobq/jobqhttp"
)

// Exit codes, stable so `jobq dequeue` etc. are scriptable in a shell loop.
const (
	exitOK    = 0
	exitError = 1
	exitUsage = 2
	exitNoJob = 3
)

// globalFlags are recognized both before the subcommand name (jobq --addr X
// enqueue ...) and after it (jobq enqueue --addr X ...) — each subcommand's
// FlagSet binds the same three flags to this struct, so either order works.
type globalFlags struct {
	addr    string
	timeout time.Duration
	json    bool
}

func defaultGlobalFlags() globalFlags {
	addr := os.Getenv("JOBQ_ADDR")
	if addr == "" {
		addr = "http://127.0.0.1:8080"
	}
	return globalFlags{addr: addr, timeout: 30 * time.Second}
}

// clientFactory builds a Client using the current global flags, raising the
// underlying http.Client's timeout to at least minTimeout when a command
// needs to hold the connection open longer than --timeout (a long-poll
// dequeue's --wait).
type clientFactory func(minTimeout time.Duration) *jobqhttp.Client

func (g *globalFlags) register(fs *flag.FlagSet) {
	fs.StringVar(&g.addr, "addr", g.addr, "jobqd base URL (env JOBQ_ADDR)")
	fs.DurationVar(&g.timeout, "timeout", g.timeout, "per-request timeout")
	fs.BoolVar(&g.json, "json", g.json, "print machine-readable JSON instead of a table")
}

func main() {
	os.Exit(mainRun(os.Args[1:], os.Stdout, os.Stderr))
}

func mainRun(args []string, stdout, stderr io.Writer) int {
	g := defaultGlobalFlags()

	top := flag.NewFlagSet("jobq", flag.ContinueOnError)
	top.SetOutput(stderr)
	g.register(top)
	if err := top.Parse(args); err != nil {
		return exitUsage
	}
	rest := top.Args()
	if len(rest) == 0 {
		fmt.Fprintln(stderr, usage)
		return exitUsage
	}
	cmd, rest := rest[0], rest[1:]

	// newClient reads g at call time (after each subcommand's own FlagSet,
	// which shares g's fields, has parsed), so --addr/--timeout given after
	// the subcommand name still take effect. minTimeout raises the
	// underlying http.Client's timeout for callers doing a long-poll
	// dequeue: http.Client.Timeout is an absolute cap independent of any
	// per-request context, so --wait longer than --timeout would otherwise
	// truncate the wait silently.
	var newClient clientFactory = func(minTimeout time.Duration) *jobqhttp.Client {
		timeout := g.timeout
		if timeout <= 0 {
			timeout = 30 * time.Second
		}
		if minTimeout > timeout {
			timeout = minTimeout
		}
		return jobqhttp.NewClient(g.addr, jobqhttp.WithTimeout(timeout))
	}

	// newClient's type (func(minTimeout time.Duration) *jobqhttp.Client) is
	// what every cmdX function below expects as its client factory.
	switch cmd {
	case "enqueue":
		return cmdEnqueue(&g, rest, newClient, stdout, stderr)
	case "dequeue":
		return cmdDequeue(&g, rest, newClient, stdout, stderr)
	case "ack":
		return cmdAck(&g, rest, newClient, stdout, stderr)
	case "nack":
		return cmdNack(&g, rest, newClient, stdout, stderr)
	case "extend":
		return cmdExtend(&g, rest, newClient, stdout, stderr)
	case "stats":
		return cmdStats(&g, rest, newClient, stdout, stderr)
	case "topics":
		return cmdTopics(&g, rest, newClient, stdout, stderr)
	case "health":
		return cmdHealth(&g, rest, newClient, stdout, stderr)
	case "recovery":
		return cmdRecovery(&g, rest, newClient, stdout, stderr)
	case "work":
		return cmdWork(&g, rest, newClient, stdout, stderr)
	case "-h", "--help", "help":
		fmt.Fprintln(stdout, usage)
		return exitOK
	default:
		fmt.Fprintf(stderr, "jobq: unknown command %q\n\n%s\n", cmd, usage)
		return exitUsage
	}
}

const usage = `jobq — CLI for a jobqd server

Usage: jobq [--addr URL] [--timeout D] [--json] <command> [flags] [args...]

Flags come before positional arguments (stdlib flag package, not getopt):
  jobq enqueue [--payload <str> | --file <path>] [--delay 5s] [--max-attempts 3] <topic> [-]
  jobq dequeue [--visibility 30s] [--wait 10s] <topic>
  jobq ack     <topic> <leaseID>
  jobq nack    <topic> <leaseID>
  jobq extend  --by 30s <topic> <leaseID>
  jobq stats   [topic]
  jobq topics
  jobq health
  jobq recovery
  jobq work    [--visibility 30s] [--wait 20s] [--max-jobs N] <topic> -- <cmd> [args...]

--addr defaults to $JOBQ_ADDR or http://127.0.0.1:8080. For a cluster, give
every node separated by commas; the leader is found and followed for you.
--addr/--timeout/--json may appear before or after the subcommand name, but
still before <topic>.
Exit codes: 0 ok, 1 error, 2 usage, 3 no job available.`
