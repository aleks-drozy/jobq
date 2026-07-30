// Command jobq-mcp is an MCP server over a jobq queue: it talks stdio to an
// MCP client (Claude Desktop, Claude Code, or anything else that speaks
// MCP) and either an embedded durable *jobq.Queue (-dir) or a running
// jobqd over HTTP (-addr, the default). See mcp/README.md for the tool
// list and an example client config.
//
// Security posture: jobq-mcp changes nothing about jobqd's threat model.
// jobqd has no auth, which is why it binds 127.0.0.1 by default. Pointing
// -addr at anything other than a local, trusted jobqd puts an
// unauthenticated write API on the network, reachable through this
// process's stdio.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/aleks-drozy/jobq"
	"github.com/aleks-drozy/jobq/jobqhttp"
	jobqmcp "github.com/aleks-drozy/jobq/mcp"
)

const defaultAddr = "http://127.0.0.1:8080"

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "jobq-mcp:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("jobq-mcp", flag.ContinueOnError)

	defAddr := defaultAddr
	if v := os.Getenv("JOBQ_ADDR"); v != "" {
		defAddr = v
	}
	addr := fs.String("addr", defAddr, "jobqd base URL to talk HTTP to (env JOBQ_ADDR). Default mode. "+
		"Pointing this at anything but a local, trusted jobqd puts an unauthenticated write API on the network.")
	dir := fs.String("dir", "", "open an embedded durable queue at this directory instead of talking HTTP. "+
		"Mutually exclusive with -addr; cannot coexist with a running jobqd on the same directory.")
	maxWait := fs.Duration("max-wait", 10*time.Second, "cap on jobq_dequeue's wait_ms, deliberately below "+
		"typical MCP client tool-call timeouts")
	maxPayload := fs.Int("max-payload", 256*1024, "cap on jobq_enqueue's payload size in bytes")
	readOnly := fs.Bool("read-only", false, "register only the six read-only tools — no mutation is reachable "+
		"at all. The right default for pointing this at anything real.")
	sync := fs.String("sync", "always", "fsync policy for -dir mode: always, interval, or never (see jobq.SyncPolicy)")

	if err := fs.Parse(args); err != nil {
		return err
	}

	explicitAddr, explicitDir := false, false
	fs.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "addr":
			explicitAddr = true
		case "dir":
			explicitDir = true
		}
	})
	if explicitAddr && explicitDir {
		return fmt.Errorf("-addr and -dir are mutually exclusive: pick embedded (-dir) or remote (-addr), not both")
	}

	var (
		be   jobqmcp.Backend
		mode string
		show string
	)
	if *dir != "" {
		policy, err := parseSyncPolicy(*sync)
		if err != nil {
			return err
		}
		q, report, err := jobq.Open(*dir, jobq.WithSync(policy))
		if err != nil {
			if isDirLockedErr(err) {
				return fmt.Errorf("cannot open %q: it is locked by another process — most likely a jobqd "+
					"already running on this directory. jobq-mcp's -dir mode opens its own embedded queue and "+
					"cannot share a directory with a running jobqd; point -addr at that jobqd instead, or stop "+
					"it first: %w", *dir, err)
			}
			return fmt.Errorf("opening %q: %w", *dir, err)
		}
		defer q.Close()
		log.Printf("jobq-mcp: recovered %q: %+v", *dir, report)
		be = jobqmcp.NewLocalBackend(q, report, *dir, *sync)
		mode, show = "local", *dir
	} else {
		be = jobqhttp.NewClient(*addr)
		mode, show = "http", *addr
	}

	server := jobqmcp.NewServer(be, jobqmcp.ServerOptions{
		MaxWait:    maxWait.Milliseconds(),
		MaxPayload: *maxPayload,
		ReadOnly:   *readOnly,
		Mode:       mode,
		Addr:       *addr,
	})

	log.Printf("jobq-mcp: mode=%s target=%s read-only=%v max-wait=%s max-payload=%d",
		mode, show, *readOnly, maxWait, *maxPayload)

	return server.Run(context.Background(), &sdk.StdioTransport{})
}

func parseSyncPolicy(s string) (jobq.SyncPolicy, error) {
	switch s {
	case "always":
		return jobq.SyncAlways, nil
	case "interval":
		return jobq.SyncInterval, nil
	case "never":
		return jobq.SyncNever, nil
	default:
		return 0, fmt.Errorf("invalid -sync %q: want always, interval, or never", s)
	}
}

func isDirLockedErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "is locked by another process")
}
