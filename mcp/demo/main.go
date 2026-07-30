// Command demo runs the crash-recovery-diagnosis scenario from
// docs/specs/2026-07-31-jobq-p3.5-mcp.md end to end, for real: it builds
// the real jobqd and jobq-mcp binaries, seeds a real queue, kills jobqd
// cold (Process.Kill, no flush, no graceful shutdown), restarts it on the
// same directory, then drives a real MCP client — over the real
// jobq-mcp binary, over real stdio — through the tool sequence an agent
// would use to diagnose what happened and get the work moving again.
//
// Every number this program prints comes from that real run. Nothing here
// is a canned transcript.
//
// Run from the mcp/ directory:
//
//	cd mcp && go run ./demo
//
// See mcp/demo/README.md for what to expect and how to read the output.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"time"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/aleks-drozy/jobq"
	"github.com/aleks-drozy/jobq/jobqhttp"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("demo: %v", err)
	}
}

func run() error {
	rootDir := flag.String("root", "..", "path to the jobq root module (contains cmd/jobqd)")
	mcpDir := flag.String("mcp-dir", ".", "path to the jobq mcp module (contains cmd/jobq-mcp)")
	keep := flag.Bool("keep", false, "keep the working directory (binaries + queue data) instead of deleting it")
	flag.Parse()

	work, err := os.MkdirTemp("", "jobq-demo-*")
	if err != nil {
		return err
	}
	if *keep {
		fmt.Println("working directory (kept):", work)
	} else {
		defer os.RemoveAll(work)
	}

	dataDir := filepath.Join(work, "data")
	jobqdBin := filepath.Join(work, binName("jobqd"))
	mcpBin := filepath.Join(work, binName("jobq-mcp"))

	step("building jobqd and jobq-mcp from source")
	if err := goBuild(*rootDir, "./cmd/jobqd", jobqdBin); err != nil {
		return fmt.Errorf("building jobqd: %w", err)
	}
	if err := goBuild(*mcpDir, "./cmd/jobq-mcp", mcpBin); err != nil {
		return fmt.Errorf("building jobq-mcp: %w", err)
	}

	addr, err := freeAddr()
	if err != nil {
		return err
	}

	step(fmt.Sprintf("starting jobqd -dir %s -addr %s -sync always", dataDir, addr))
	jobqd, jobqdLog, err := startJobqd(jobqdBin, dataDir, addr)
	if err != nil {
		return err
	}
	client := jobqhttp.NewClient("http://" + addr)
	if err := waitHealthy(client, 5*time.Second); err != nil {
		return fmt.Errorf("jobqd never became healthy: %w", err)
	}

	poisonIDs, crashVictimIDs, err := seedAndChurn(client)
	if err != nil {
		return err
	}
	fmt.Printf("seeded (ground truth, not shown to the agent): %d poison job(s), %d in-flight-at-kill job(s)\n",
		len(poisonIDs), len(crashVictimIDs))

	step("killing jobqd COLD — Process.Kill, no signal, no flush, mid-flight")
	if err := jobqd.Process.Kill(); err != nil {
		return fmt.Errorf("killing jobqd: %w", err)
	}
	_ = jobqd.Wait()
	jobqdLog.Close()

	step("removing the stale lock file jobqd's cold kill left behind (documented behavior: " +
		"acquireDirLock only detects a live holder; a crash's lock is cleared by hand, on purpose)")
	if err := os.Remove(filepath.Join(dataDir, "jobq.lock")); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("removing stale lock: %w", err)
	}

	step("restarting jobqd on the same directory")
	jobqd2, jobqdLog2, err := startJobqd(jobqdBin, dataDir, addr)
	if err != nil {
		return err
	}
	defer func() { _ = jobqd2.Process.Kill(); _ = jobqd2.Wait(); jobqdLog2.Close() }()
	client2 := jobqhttp.NewClient("http://" + addr)
	if err := waitHealthy(client2, 5*time.Second); err != nil {
		return fmt.Errorf("restarted jobqd never became healthy: %w", err)
	}

	fmt.Println()
	fmt.Println("=================================================================")
	fmt.Println(" RECORDING STARTS HERE — everything below is a real MCP client")
	fmt.Println(" talking to jobq-mcp, talking to the just-restarted jobqd.")
	fmt.Println("=================================================================")
	fmt.Println()

	mcpCmd := exec.Command(mcpBin, "-addr", "http://"+addr)
	transport := &sdk.CommandTransport{Command: mcpCmd}
	mcpClient := sdk.NewClient(&sdk.Implementation{Name: "demo-agent", Version: "v0.0.1"}, nil)
	cs, err := mcpClient.Connect(context.Background(), transport, nil)
	if err != nil {
		return fmt.Errorf("connecting to jobq-mcp: %w", err)
	}
	defer func() {
		_ = cs.Close()
		if mcpCmd.Process != nil {
			_ = mcpCmd.Process.Kill()
		}
		_ = mcpCmd.Wait()
	}()

	return agentTrajectory(cs)
}

// --- setup helpers ---------------------------------------------------------

func binName(base string) string {
	if runtime.GOOS == "windows" {
		return base + ".exe"
	}
	return base
}

func goBuild(dir, pkg, out string) error {
	cmd := exec.Command("go", "build", "-o", out, pkg)
	cmd.Dir = dir
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func freeAddr() (string, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		return "", err
	}
	return addr, nil
}

func startJobqd(bin, dataDir, addr string) (*exec.Cmd, *os.File, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, nil, err
	}
	logPath := filepath.Join(filepath.Dir(dataDir), "jobqd.log")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, nil, err
	}
	cmd := exec.Command(bin, "-dir", dataDir, "-addr", addr, "-sync", "always")
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		logFile.Close()
		return nil, nil, err
	}
	return cmd, logFile, nil
}

func waitHealthy(c *jobqhttp.Client, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		if _, _, _, err := c.Health(); err == nil {
			return nil
		} else {
			lastErr = err
		}
		time.Sleep(50 * time.Millisecond)
	}
	return lastErr
}

func step(msg string) { fmt.Println(">>> " + msg) }

// --- seeding: build a real, reproducible crash scenario ---------------------

// seedAndChurn builds the exact scenario the demo diagnoses:
//   - 6 poison jobs, max_attempts=1, dequeued and nacked for real BEFORE the
//     kill — they land in the DLQ with a genuine DeadLetteredAt.
//   - 5 "crash victim" jobs, max_attempts=1, dequeued but never
//     acked/nacked — still leased when jobqd is killed. Replay finds them
//     in flight with their one attempt already spent and dead-letters them
//     with a ZERO DeadLetteredAt (recover.go: "unknown crash time; zero is
//     honest").
//   - 30 ordinary jobs: 20 acked for real, 10 left ready untouched.
//
// Each category is enqueued and drained immediately, one at a time, so
// strict FIFO dequeue order can't cross categories.
func seedAndChurn(c *jobqhttp.Client) (poisonIDs, crashVictimIDs []string, err error) {
	step("seeding 6 poison jobs and dead-lettering them for real, before the crash")
	for i := 0; i < 6; i++ {
		id, err := c.Enqueue("emails", []byte(fmt.Sprintf("POISON: malformed payload #%d", i)), jobq.WithMaxAttempts(1))
		if err != nil {
			return nil, nil, err
		}
		poisonIDs = append(poisonIDs, id)
		_, lease, err := c.Dequeue("emails", time.Minute)
		if err != nil {
			return nil, nil, fmt.Errorf("dequeuing poison job: %w", err)
		}
		if err := c.Nack(lease); err != nil {
			return nil, nil, fmt.Errorf("nacking poison job: %w", err)
		}
	}

	step("seeding 5 jobs and leaving them leased — these are in flight when jobqd dies")
	for i := 0; i < 5; i++ {
		id, err := c.Enqueue("emails", []byte(fmt.Sprintf("send welcome email #%d", i)), jobq.WithMaxAttempts(1))
		if err != nil {
			return nil, nil, err
		}
		crashVictimIDs = append(crashVictimIDs, id)
		if _, _, err := c.Dequeue("emails", time.Minute); err != nil {
			return nil, nil, fmt.Errorf("dequeuing crash-victim job: %w", err)
		}
		// Deliberately no ack, no nack: the lease is still held when jobqd dies.
	}

	step("seeding 30 ordinary jobs; acking 20 for real, leaving 10 ready")
	for i := 0; i < 30; i++ {
		if _, err := c.Enqueue("emails", []byte(fmt.Sprintf("newsletter #%d", i))); err != nil {
			return nil, nil, err
		}
	}
	for i := 0; i < 20; i++ {
		_, lease, err := c.Dequeue("emails", time.Minute)
		if err != nil {
			return nil, nil, fmt.Errorf("dequeuing ordinary job: %w", err)
		}
		if err := c.Ack(lease); err != nil {
			return nil, nil, fmt.Errorf("acking ordinary job: %w", err)
		}
	}
	return poisonIDs, crashVictimIDs, nil
}

// --- the agent's trajectory, through real MCP tool calls -------------------

func agentTrajectory(cs *sdk.ClientSession) error {
	fmt.Println(`AGENT PROMPT: "Something happened to the queue. Find out what, and get the work moving again without losing anything."`)
	fmt.Println()

	health, err := callTool[map[string]any](cs, "jobq_health", nil)
	if err != nil {
		return err
	}
	printCall("jobq_health", nil, health)
	fmt.Printf("AGENT: durable=%v, uptime_ms=%v — this process just started.\n\n", health["durable"], health["uptime_ms"])

	recovery, err := callTool[map[string]any](cs, "jobq_recovery_report", nil)
	if err != nil {
		return err
	}
	printCall("jobq_recovery_report", nil, recovery)
	if interp, ok := recovery["interpretation"].([]any); ok {
		fmt.Println("AGENT reads the interpretation:")
		for _, line := range interp {
			fmt.Println("  -", line)
		}
	}
	fmt.Println()

	topics, err := callTool[map[string]any](cs, "jobq_list_topics", nil)
	if err != nil {
		return err
	}
	printCall("jobq_list_topics", nil, topics)

	stats, err := callTool[map[string]any](cs, "jobq_topic_stats", nil)
	if err != nil {
		return err
	}
	printCall("jobq_topic_stats", nil, stats)
	fmt.Println("AGENT: that's the shape of the damage. Time to look inside the DLQ — read-only.")
	fmt.Println()

	peek, err := callTool[map[string]any](cs, "jobq_peek", map[string]any{"topic": "emails.dlq", "limit": 20})
	if err != nil {
		return err
	}
	printCall("jobq_peek", map[string]any{"topic": "emails.dlq", "limit": 20}, peek)

	ready, _ := peek["ready"].([]any)
	var realFailures, crashVictims []string
	for _, r := range ready {
		j := r.(map[string]any)
		id, _ := j["id"].(string)
		dla, hasDLA := j["dead_lettered_at"].(string)
		if hasDLA && dla != "" && dla != "0001-01-01T00:00:00Z" {
			realFailures = append(realFailures, id)
		} else {
			crashVictims = append(crashVictims, id)
		}
	}
	fmt.Printf("AGENT partitions the DLQ by dead_lettered_at:\n"+
		"  %d jobs have a real timestamp -> failed for real, in the application, before the crash. Leave them for a human.\n"+
		"  %d jobs have a ZERO timestamp -> dead-lettered by REPLAY: in flight when the process died, with their\n"+
		"    last attempt already spent. The engine refused to fabricate a timestamp it did not have. These are\n"+
		"    crash victims, not poison — safe to requeue.\n\n",
		len(realFailures), len(crashVictims))

	requeue, err := callTool[map[string]any](cs, "jobq_dlq_requeue", map[string]any{
		"topic": "emails", "job_ids": crashVictims,
	})
	if err != nil {
		return err
	}
	printCall("jobq_dlq_requeue", map[string]any{"topic": "emails", "job_ids": crashVictims}, requeue)
	fmt.Println()

	step("lease-expiry verification: does redelivery still work post-recovery?")
	if _, err := callTool[map[string]any](cs, "jobq_enqueue", map[string]any{
		"topic": "lease-check", "payload": "verify redelivery",
	}); err != nil {
		return err
	}
	deq, err := callTool[map[string]any](cs, "jobq_dequeue", map[string]any{
		"topic": "lease-check", "visibility_ms": 1500,
	})
	if err != nil {
		return err
	}
	printCall("jobq_dequeue", map[string]any{"topic": "lease-check", "visibility_ms": 1500}, deq)

	mid, err := callTool[map[string]any](cs, "jobq_topic_stats", map[string]any{"topic": "lease-check"})
	if err != nil {
		return err
	}
	printCall("jobq_topic_stats", map[string]any{"topic": "lease-check"}, mid)
	fmt.Println("AGENT: in_flight=1, as expected — deliberately not acking. Waiting for the lease to expire...")

	time.Sleep(1800 * time.Millisecond)

	deq2, err := callTool[map[string]any](cs, "jobq_dequeue", map[string]any{"topic": "lease-check"})
	if err != nil {
		return err
	}
	printCall("jobq_dequeue", map[string]any{"topic": "lease-check"}, deq2)
	if job, ok := deq2["job"].(map[string]any); ok {
		fmt.Printf("AGENT: attempt=%v after the lease expired — redelivery confirmed working post-recovery.\n\n", job["attempt"])
	}
	if job, ok := deq2["job"].(map[string]any); ok {
		if _, err := callTool[map[string]any](cs, "jobq_ack", map[string]any{
			"topic": "lease-check", "lease_id": deq2["lease"].(map[string]any)["id"],
		}); err != nil {
			return err
		}
		_ = job
	}

	step("closing reconciliation: enqueued == acked + ready + in_flight + dead_letters + dropped")
	final, err := callTool[map[string]any](cs, "jobq_topic_stats", nil)
	if err != nil {
		return err
	}
	printCall("jobq_topic_stats", nil, final)
	return printReconciliation(final)
}

func printReconciliation(stats map[string]any) error {
	topics, _ := stats["topics"].(map[string]any)
	var enq, acked, ready, inflight, dl, dropped int64
	for name, v := range topics {
		s := v.(map[string]any)
		enq += toInt(s["enqueued"])
		acked += toInt(s["acked"])
		ready += toInt(s["ready"])
		inflight += toInt(s["in_flight"])
		dl += toInt(s["dead_letters"])
		dropped += toInt(s["dropped"])
		_ = name
	}
	sum := acked + ready + inflight + dl + dropped
	fmt.Printf("AGENT: sum over every topic — enqueued=%d, acked+ready+in_flight+dead_letters+dropped=%d (%d+%d+%d+%d+%d)\n",
		enq, sum, acked, ready, inflight, dl, dropped)
	if enq != sum {
		return fmt.Errorf("RECONCILIATION FAILED: %d != %d — a job is unaccounted for", enq, sum)
	}
	fmt.Println("AGENT: conservation holds. Every job that entered the queue is accounted for in exactly one place.")
	return nil
}

func toInt(v any) int64 {
	f, _ := v.(float64)
	return int64(f)
}

// --- generic MCP tool-call plumbing ----------------------------------------

func callTool[Out any](cs *sdk.ClientSession, name string, args any) (Out, error) {
	var zero Out
	res, err := cs.CallTool(context.Background(), &sdk.CallToolParams{Name: name, Arguments: args})
	if err != nil {
		return zero, fmt.Errorf("%s: protocol error: %w", name, err)
	}
	if res.IsError {
		text := "(no content)"
		if len(res.Content) > 0 {
			if tc, ok := res.Content[0].(*sdk.TextContent); ok {
				text = tc.Text
			}
		}
		return zero, fmt.Errorf("%s: tool error: %s", name, text)
	}
	raw, err := json.Marshal(res.StructuredContent)
	if err != nil {
		return zero, err
	}
	var out Out
	if err := json.Unmarshal(raw, &out); err != nil {
		return zero, fmt.Errorf("%s: decoding result: %w", name, err)
	}
	return out, nil
}

func printCall(name string, args, result any) {
	fmt.Printf("MCP CALL: %s\n", name)
	if args != nil {
		if b, err := json.Marshal(args); err == nil {
			fmt.Printf("  args:   %s\n", b)
		}
	}
	if b, err := json.MarshalIndent(result, "  ", "  "); err == nil {
		fmt.Printf("  result: %s\n", b)
	}
	fmt.Println()
}
