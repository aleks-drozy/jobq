package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/aleks-drozy/jobq/jobqhttp"
)

func TestParseSyncPolicy(t *testing.T) {
	cases := map[string]bool{"always": true, "interval": true, "never": true, "bogus": false}
	for s, ok := range cases {
		_, err := parseSyncPolicy(s)
		if (err == nil) != ok {
			t.Errorf("parseSyncPolicy(%q) err=%v, want ok=%v", s, err, ok)
		}
	}
}

func TestParseFlagsDefaults(t *testing.T) {
	c, err := parseFlags(nil)
	if err != nil {
		t.Fatal(err)
	}
	if c.addr != "127.0.0.1:8080" {
		t.Errorf("default addr = %q, want 127.0.0.1:8080 (no auth, must not default to 0.0.0.0)", c.addr)
	}
	if c.dir != "" {
		t.Errorf("default dir = %q, want empty (in-memory)", c.dir)
	}
	if c.sync != "always" {
		t.Errorf("default sync = %q, want always", c.sync)
	}
}

func TestParseFlagsRejectsUnknownSync(t *testing.T) {
	if _, err := parseFlags([]string{"-dir", t.TempDir(), "-sync", "bogus"}); err == nil {
		t.Fatal("expected error for unknown -sync policy")
	}
}

// TestServeShutdownOrdering exercises serve() end to end against a real
// listener: a request must succeed before shutdown, cancelling the context
// must stop the server, and Shutdown must return before the queue's owner
// (the caller, mirroring run()'s defer q.Close()) closes it — proving
// requests are drained before the queue goes away.
func TestServeShutdownOrdering(t *testing.T) {
	q, opts, err := openQueue(config{maxPayload: 1 << 20, maxWait: time.Second}, testLogger(t))
	if err != nil {
		t.Fatal(err)
	}
	h := jobqhttp.NewHandler(q, opts...)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- serve(ctx, ln, h, 2*time.Second, testLogger(t)) }()

	waitForServer(t, addr)

	resp, err := http.Get("http://" + addr + "/v1/health")
	if err != nil {
		t.Fatalf("GET /v1/health before shutdown: %v", err)
	}
	var health struct {
		Status string `json:"status"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&health); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if health.Status != "ok" {
		t.Fatalf("status = %q, want ok", health.Status)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("serve() returned %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("serve() did not return after context cancellation")
	}

	// serve() has returned, i.e. srv.Shutdown completed, before we close
	// the queue here — mirroring run()'s deferred q.Close ordering.
	if err := q.Close(); err != nil {
		t.Errorf("q.Close after shutdown: %v", err)
	}

	if _, err := http.Get("http://" + addr + "/v1/health"); err == nil {
		t.Error("expected connection refused after shutdown, got a response")
	}
}

func TestOpenQueueInMemoryByDefault(t *testing.T) {
	q, _, err := openQueue(config{}, testLogger(t))
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	if _, err := q.Enqueue("t", []byte("x")); err != nil {
		t.Fatal(err)
	}
}

func TestOpenQueueDurable(t *testing.T) {
	dir := t.TempDir()
	q, opts, err := openQueue(config{dir: dir, sync: "always"}, testLogger(t))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := q.Enqueue("t", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	if len(opts) == 0 {
		t.Fatal("expected handler options for a durable queue (WithRecovery at least)")
	}
}

func testLogger(t *testing.T) *log.Logger {
	t.Helper()
	return log.New(&bytes.Buffer{}, "", 0)
}

func waitForServer(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("server at %s never became reachable", addr)
}
