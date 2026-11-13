// Command jobqchaos breaks a running jobq cluster on purpose and reports
// what its clients saw.
//
// It runs producers and consumers against the cluster through
// jobqhttp.Client, the way any worker pool would, and while they run it
// injects one fault at a time: a node killed, a node cut off from the
// others, the network between them made slow and lossy. Each fault is held
// for a while, undone, and repeated. When a fault has had its runs the
// producers stop, the queue is drained, and the contract is checked from
// the client's side of the wire:
//
//	LOSS  (forbidden): an enqueue the client saw acknowledged is never
//	       delivered and completed afterwards.
//	GHOST (forbidden): a job whose ack the client saw acknowledged is
//	       delivered again.
//	DUPES (permitted): counted and reported.
//
// It also measures how long writes stopped: from the fault to the first
// acknowledged write sent after it, and the longest gap between
// acknowledged writes until the cluster was whole again.
//
// jobqchaos never touches a node itself. Everything it does to one goes
// through a hook, an executable run as
//
//	HOOK VERB NODE-ID
//
// with these verbs, each of which must return once it has taken effect:
//
//	kill     end the node's jobqd the way a power cut would (SIGKILL)
//	start    start it again on the same data directory
//	isolate  cut the node off from the other nodes, not from clients
//	heal     undo isolate
//	slow     make the node's network slow and lossy
//	fast     undo slow
//
// chaos/netns.sh is that hook for three nodes in network namespaces on one
// Linux machine. The same matrix runs against real machines by writing the
// six verbs for them.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ok, err := run(ctx, os.Args[1:], os.Stdout)
	if err != nil {
		fmt.Fprintln(os.Stderr, "jobqchaos:", err)
		os.Exit(2)
	}
	if !ok {
		os.Exit(1)
	}
}

type config struct {
	nodes     string
	hook      string
	faults    string
	runs      int
	hold      time.Duration
	settle    time.Duration
	drain     time.Duration
	producers int
	consumers int
	pace      time.Duration
	out       string
	seed      int64
}

func parseFlags(args []string) (config, error) {
	fs := flag.NewFlagSet("jobqchaos", flag.ContinueOnError)
	c := config{}
	fs.StringVar(&c.nodes, "nodes", "", "every node's client URL by ID, e.g. 1=http://10.77.0.1:8080,2=http://10.77.0.2:8080,3=http://10.77.0.3:8080")
	fs.StringVar(&c.hook, "hook", "", "executable that applies faults, run as HOOK VERB NODE-ID (verbs: kill start isolate heal slow fast)")
	fs.StringVar(&c.faults, "faults", strings.Join(faultNames(), ","), "faults to inject, in order")
	fs.IntVar(&c.runs, "runs", 5, "times to inject each fault")
	fs.DurationVar(&c.hold, "hold", 3*time.Second, "how long each fault is held before it is undone")
	fs.DurationVar(&c.settle, "settle", 2*time.Second, "quiet time after the cluster is whole again, before the next fault")
	fs.DurationVar(&c.drain, "drain", 3*time.Minute, "how long to wait, after each fault's runs, for the queue to empty and the nodes to agree")
	fs.IntVar(&c.producers, "producers", 3, "goroutines enqueueing")
	fs.IntVar(&c.consumers, "consumers", 4, "goroutines dequeueing and acking")
	fs.DurationVar(&c.pace, "pace", 5*time.Millisecond, "pause between one producer's enqueues")
	fs.StringVar(&c.out, "out", "", "directory to write results.json and results.md into (default: print only)")
	fs.Int64Var(&c.seed, "seed", 0, "seed for choosing which follower to hit (0 = from the clock)")
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}
	if c.nodes == "" || c.hook == "" {
		return config{}, errors.New("-nodes and -hook are required")
	}
	if c.runs < 1 || c.producers < 1 || c.consumers < 1 {
		return config{}, errors.New("-runs, -producers and -consumers must be at least 1")
	}
	if c.seed == 0 {
		c.seed = time.Now().UnixNano()
	}
	return c, nil
}

// parseNodes reads "1=http://a:8080,2=http://b:8080" into nodes sorted by ID.
func parseNodes(s string) ([]*node, error) {
	seen := map[uint64]bool{}
	var out []*node
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, url, ok := strings.Cut(part, "=")
		n, err := strconv.ParseUint(strings.TrimSpace(id), 10, 64)
		url = strings.TrimSuffix(strings.TrimSpace(url), "/")
		if !ok || err != nil || n == 0 || url == "" {
			return nil, fmt.Errorf("-nodes: %q is not ID=URL with a positive numeric ID", part)
		}
		if !strings.HasPrefix(url, "http://") && !strings.HasPrefix(url, "https://") {
			return nil, fmt.Errorf("-nodes: %q must start with http:// or https://", url)
		}
		if seen[n] {
			return nil, fmt.Errorf("-nodes: node %d is listed twice", n)
		}
		seen[n] = true
		out = append(out, newNode(n, url))
	}
	if len(out) < 3 {
		return nil, fmt.Errorf("-nodes: %d nodes listed; a cluster that survives a fault has at least 3", len(out))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].id < out[j].id })
	return out, nil
}

// run is main without the exit: ok is false when the contract was broken,
// a fault was never seen to take effect, or a fault could not be run.
func run(ctx context.Context, args []string, stdout io.Writer) (ok bool, err error) {
	c, err := parseFlags(args)
	if err != nil {
		return false, err
	}
	nodes, err := parseNodes(c.nodes)
	if err != nil {
		return false, err
	}
	faults, err := parseFaults(c.faults)
	if err != nil {
		return false, err
	}
	l := &lab{nodes: nodes, inj: hookInjector{path: c.hook, timeout: 30 * time.Second}, out: stdout}
	rep := runMatrix(ctx, l, faults, c)
	fmt.Fprintln(stdout)
	fmt.Fprint(stdout, rep.markdown())
	if c.out != "" {
		if err := rep.write(c.out); err != nil {
			return false, err
		}
		fmt.Fprintf(stdout, "\nwrote %s and %s\n", filepath.Join(c.out, "results.json"), filepath.Join(c.out, "results.md"))
	}
	return rep.ok(), nil
}

// runMatrix runs every fault in turn against one lab.
func runMatrix(ctx context.Context, l *lab, faults []fault, c config) report {
	rng := rand.New(rand.NewSource(c.seed))
	rep := report{
		Started: time.Now().UTC(), Seed: c.seed, Nodes: len(l.nodes),
		Runs: c.runs, HoldMs: ms(c.hold), SettleMs: ms(c.settle),
		Producers: c.producers, Consumers: c.consumers, PaceMs: ms(c.pace),
	}
	fmt.Fprintf(l.out, "seed %d, %d nodes, %d runs per fault, hold %s, settle %s\n", c.seed, len(l.nodes), c.runs, c.hold, c.settle)
	for _, f := range faults {
		if ctx.Err() != nil {
			break
		}
		rep.Cells = append(rep.Cells, runCell(ctx, l, f, c, rng))
	}
	rep.Interrupted = ctx.Err() != nil
	rep.Finished = time.Now().UTC()
	return rep
}

// sleep waits for d or until ctx is done, and reports whether it slept d.
func sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }
