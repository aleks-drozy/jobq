// Command jobqd is the jobq HTTP daemon: it wraps a *jobq.Queue in
// jobqhttp.NewHandler and serves it.
//
// There is no authentication (see README, "Not in v1"), so the default
// bind address is 127.0.0.1 — binding 0.0.0.0 by default would put an
// unauthenticated write API on the LAN, and a flag flip is a much smaller
// mistake to make than a silent default.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aleks-drozy/jobq"
	"github.com/aleks-drozy/jobq/jobqhttp"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "jobqd:", err)
		os.Exit(1)
	}
}

// config is the parsed, validated result of flag parsing — kept separate
// from run() so tests can build one directly instead of going through
// flag.Parse and a real listen address.
type config struct {
	addr            string
	dir             string
	sync            string
	syncEvery       time.Duration
	maxPayload      int64
	maxWait         time.Duration
	shutdownTimeout time.Duration
}

func parseFlags(args []string) (config, error) {
	fs := flag.NewFlagSet("jobqd", flag.ContinueOnError)
	c := config{}
	fs.StringVar(&c.addr, "addr", "127.0.0.1:8080", "listen address (default binds localhost only: there is no auth)")
	fs.StringVar(&c.dir, "dir", "", "durable data directory; empty means an in-memory queue (nothing survives exit)")
	fs.StringVar(&c.sync, "sync", "always", "fsync policy for a durable queue: always|interval|never")
	fs.DurationVar(&c.syncEvery, "sync-every", 5*time.Millisecond, "flush period for -sync=interval")
	fs.Int64Var(&c.maxPayload, "max-payload", 1<<20, "max enqueue body size in bytes")
	fs.DurationVar(&c.maxWait, "max-wait", 30*time.Second, "hard cap on a dequeue's wait_ms")
	fs.DurationVar(&c.shutdownTimeout, "shutdown-timeout", 10*time.Second, "grace period for in-flight requests on shutdown")
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}
	if _, err := parseSyncPolicy(c.sync); c.dir != "" && err != nil {
		return config{}, err
	}
	return c, nil
}

func run(ctx context.Context, args []string, stdout io.Writer) error {
	c, err := parseFlags(args)
	if err != nil {
		return err
	}
	logger := log.New(stdout, "jobqd: ", log.LstdFlags)

	q, handlerOpts, err := openQueue(c, logger)
	if err != nil {
		return err
	}
	defer q.Close()

	ln, err := net.Listen("tcp", c.addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", c.addr, err)
	}
	logger.Printf("listening on %s", ln.Addr())

	h := jobqhttp.NewHandler(q, handlerOpts...)
	return serve(ctx, ln, h, c.shutdownTimeout, logger)
}

func openQueue(c config, logger *log.Logger) (*jobq.Queue, []jobqhttp.HandlerOption, error) {
	opts := []jobqhttp.HandlerOption{
		jobqhttp.WithMaxPayload(c.maxPayload),
		jobqhttp.WithMaxWait(c.maxWait),
		jobqhttp.WithLogger(logger),
	}
	if c.dir == "" {
		logger.Printf("in-memory queue: nothing survives exit")
		return jobq.New(), opts, nil
	}
	policy, err := parseSyncPolicy(c.sync)
	if err != nil {
		return nil, nil, err
	}
	q, report, err := jobq.Open(c.dir, jobq.WithSync(policy), jobq.WithSyncEvery(c.syncEvery))
	if err != nil {
		return nil, nil, fmt.Errorf("opening %s: %w", c.dir, err)
	}
	// The recovery report is logged here AND served at GET /v1/recovery for
	// the lifetime of the process — an operator should not have to grep
	// startup logs for what replay found.
	logger.Printf("recovery report: %+v", report)
	opts = append(opts, jobqhttp.WithRecovery(report, c.dir, c.sync))
	return q, opts, nil
}

// serve runs h on ln until ctx is cancelled, then shuts down in the order
// that matters: stop accepting and drain in-flight requests FIRST (Go's
// http.Server.Shutdown), THEN the caller closes the queue (via the deferred
// q.Close in run/tests). Reversed, in-flight handlers would fail mid-request
// with ErrClosed/503; in this order, Close also gets to write CLEANSHUT, so
// the next Open reports a clean shutdown instead of a torn tail.
func serve(ctx context.Context, ln net.Listener, h http.Handler, shutdownTimeout time.Duration, logger *log.Logger) error {
	srv := &http.Server{
		Handler: h,
		// ReadHeaderTimeout and IdleTimeout guard against a Slowloris-style
		// client holding a connection open without ever finishing its
		// request headers or reusing it. No WriteTimeout: a long-poll
		// dequeue (-max-wait, up to 30s by default) legitimately holds the
		// response open for that long, and http.Server.WriteTimeout would
		// cut it off mid-wait.
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()

	select {
	case err := <-serveErr:
		if err != nil && err != http.ErrServerClosed {
			return err
		}
		return nil
	case <-ctx.Done():
		logger.Printf("shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			logger.Printf("shutdown: %v", err)
		}
		<-serveErr
		return nil
	}
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
		return 0, fmt.Errorf("unknown -sync %q (want always|interval|never)", s)
	}
}
