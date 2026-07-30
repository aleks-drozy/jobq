package main

import (
	"context"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/aleks-drozy/jobq"
	"github.com/aleks-drozy/jobq/jobqhttp"
	jobqmcp "github.com/aleks-drozy/jobq/mcp"
)

func TestAddrAndDirAreMutuallyExclusive(t *testing.T) {
	dir := t.TempDir()
	err := run([]string{"-addr", "http://127.0.0.1:9999", "-dir", dir})
	if err == nil {
		t.Fatal("run succeeded with both -addr and -dir explicitly set, want an error")
	}
	if !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("err = %v, want it to mention mutually exclusive", err)
	}
}

func TestDirOnALockedDirectoryExplainsItself(t *testing.T) {
	dir := t.TempDir()
	// Take the lock ourselves first, the same way a running jobqd would.
	lockPath := filepath.Join(dir, "jobq.lock")
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("seeding a lock file: %v", err)
	}
	defer f.Close()

	err = run([]string{"-dir", dir})
	if err == nil {
		t.Fatal("run succeeded against a locked directory, want an error")
	}
	if !strings.Contains(err.Error(), "jobqd") {
		t.Fatalf("err = %v, want the explanatory message to name jobqd rather than surface a raw lock error", err)
	}
	if !strings.Contains(err.Error(), "locked") {
		t.Fatalf("err = %v, want it to say the directory is locked", err)
	}
}

func TestInvalidSyncPolicyIsRejected(t *testing.T) {
	dir := t.TempDir()
	err := run([]string{"-dir", dir, "-sync", "sometimes"})
	if err == nil {
		t.Fatal("run succeeded with an invalid -sync value, want an error")
	}
	if !strings.Contains(err.Error(), "-sync") {
		t.Fatalf("err = %v, want it to mention -sync", err)
	}
}

// newInMemorySession connects be to a fresh in-memory MCP client/server pair
// running under opts, the same wiring run() does minus stdio.
func newInMemorySession(t *testing.T, be jobqmcp.Backend, opts jobqmcp.ServerOptions) *sdk.ClientSession {
	t.Helper()
	server := jobqmcp.NewServer(be, opts)
	client := sdk.NewClient(&sdk.Implementation{Name: "test-client", Version: "v0.0.0"}, nil)
	t1, t2 := sdk.NewInMemoryTransports()
	ctx := context.Background()
	if _, err := server.Connect(ctx, t1, nil); err != nil {
		t.Fatalf("server.Connect: %v", err)
	}
	cs, err := client.Connect(ctx, t2, nil)
	if err != nil {
		t.Fatalf("client.Connect: %v", err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	return cs
}

// TestDequeueWaitMsEqualToMaxWaitReturnsEmptyNotError pins the fix for the
// bug where newHTTPBackend's http.Client used jobqhttp.NewClient's default,
// hardcoded 10s Timeout: a wait_ms at or above 10s raced that absolute cap
// and lost, turning a legitimate empty long-poll result into a spurious
// "internal" transport error. maxWait is set well above 10s so the old
// hardcoded cap would have fired first and deterministically failed this
// test; the fixed client (maxWait+10s of headroom) must not.
func TestDequeueWaitMsEqualToMaxWaitReturnsEmptyNotError(t *testing.T) {
	q := jobq.New()
	defer q.Close()
	srv := httptest.NewServer(jobqhttp.NewHandler(q))
	defer srv.Close()

	const maxWait = 12 * time.Second
	be := newHTTPBackend(srv.URL, maxWait)
	cs := newInMemorySession(t, be, serverOptions("http", srv.URL, maxWait, 0, false))

	res, err := cs.CallTool(context.Background(), &sdk.CallToolParams{
		Name:      "jobq_dequeue",
		Arguments: map[string]any{"topic": "never-enqueued-to", "wait_ms": maxWait.Milliseconds()},
	})
	if err != nil {
		t.Fatalf("CallTool: protocol error: %v", err)
	}
	if res.IsError {
		t.Fatalf("jobq_dequeue returned an error instead of empty:true: %+v", res.Content)
	}
	m, ok := res.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("StructuredContent = %T, want map[string]any", res.StructuredContent)
	}
	if empty, _ := m["empty"].(bool); !empty {
		t.Fatalf("empty = %v, want true", m["empty"])
	}
}

// TestServerOptionsAddrOnlySetForHTTPMode pins that serverOptions leaves
// Addr empty in local (-dir) mode, matching mcp/server.go's ServerOptions
// doc that Addr is set only for "http" mode.
func TestServerOptionsAddrOnlySetForHTTPMode(t *testing.T) {
	local := serverOptions("local", "http://127.0.0.1:8080", 10*time.Second, 1024, false)
	if local.Addr != "" {
		t.Fatalf("local mode Addr = %q, want empty", local.Addr)
	}
	remote := serverOptions("http", "http://127.0.0.1:8080", 10*time.Second, 1024, false)
	if remote.Addr != "http://127.0.0.1:8080" {
		t.Fatalf("http mode Addr = %q, want the configured addr", remote.Addr)
	}
}

// TestLocalHealthHasNoAddrKey is the end-to-end regression for the same
// bug: a local-backend jobq_health result must not carry an "addr" key at
// all (not merely an empty one), since the wire field is omitempty.
func TestLocalHealthHasNoAddrKey(t *testing.T) {
	dir := t.TempDir()
	q, report, err := jobq.Open(dir)
	if err != nil {
		t.Fatalf("jobq.Open: %v", err)
	}
	defer q.Close()

	be := jobqmcp.NewLocalBackend(q, report, dir, "always")
	cs := newInMemorySession(t, be, serverOptions("local", "http://127.0.0.1:8080", 10*time.Second, 1024, false))

	res, err := cs.CallTool(context.Background(), &sdk.CallToolParams{Name: "jobq_health"})
	if err != nil {
		t.Fatalf("CallTool: protocol error: %v", err)
	}
	if res.IsError {
		t.Fatalf("jobq_health returned an error: %+v", res.Content)
	}
	m, ok := res.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("StructuredContent = %T, want map[string]any", res.StructuredContent)
	}
	if _, present := m["addr"]; present {
		t.Fatalf("result has an addr key in local mode: %+v, want none", m)
	}
}
