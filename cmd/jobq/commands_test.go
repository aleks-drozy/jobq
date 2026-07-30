package main

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aleks-drozy/jobq"
	"github.com/aleks-drozy/jobq/jobqhttp"
)

// --- resolvePayload: pure function, table-tested -------------------------

func TestResolvePayload(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "payload.txt")
	if err := os.WriteFile(filePath, []byte("from-file"), 0o644); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name        string
		payloadFlag string
		fileFlag    string
		positional  []string
		stdin       string
		want        string
		wantErr     bool
	}{
		{"literal payload", "hello", "", nil, "", "hello", false},
		{"from file", "", filePath, nil, "", "from-file", false},
		{"from stdin via dash", "", "", []string{"-"}, "piped-in", "piped-in", false},
		{"no source falls back to stdin", "", "", nil, "default-stdin", "default-stdin", false},
		{"conflicting payload and file", "x", filePath, nil, "", "", true},
		{"conflicting payload and dash", "x", "", []string{"-"}, "", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolvePayload(tt.payloadFlag, tt.fileFlag, tt.positional, strings.NewReader(tt.stdin))
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil {
				return
			}
			if string(got) != tt.want {
				t.Errorf("payload = %q, want %q", got, tt.want)
			}
		})
	}
}

// --- formatStatsTable: pure function --------------------------------------

func TestFormatStatsTableSortedAndAligned(t *testing.T) {
	out := formatStatsTable(map[string]jobq.Stats{
		"zeta":  {Enqueued: 1},
		"alpha": {Enqueued: 100, Ready: 2, Acked: 3},
	})
	lines := strings.Split(strings.TrimRight(out, "\n"), "\n")
	if len(lines) != 3 {
		t.Fatalf("got %d lines, want 3 (header + 2 rows):\n%s", len(lines), out)
	}
	if !strings.HasPrefix(lines[0], "TOPIC") {
		t.Errorf("header line = %q", lines[0])
	}
	if !strings.HasPrefix(lines[1], "alpha") {
		t.Errorf("expected alpha before zeta (sorted), got line 1 = %q", lines[1])
	}
	if !strings.HasPrefix(lines[2], "zeta") {
		t.Errorf("expected zeta second, got line 2 = %q", lines[2])
	}
}

func TestFormatStatsTableEmpty(t *testing.T) {
	if got := formatStatsTable(nil); got != "(no topics)\n" {
		t.Errorf("empty table = %q", got)
	}
}

// --- end-to-end subcommands against a live httptest server ---------------

func newCLITestServer(t *testing.T) (*jobq.Queue, string) {
	t.Helper()
	q := jobq.New()
	t.Cleanup(func() { _ = q.Close() })
	srv := httptest.NewServer(jobqhttp.NewHandler(q))
	t.Cleanup(srv.Close)
	return q, srv.URL
}

func runCLI(addr string, args ...string) (stdout, stderr string, code int) {
	var out, errBuf bytes.Buffer
	full := append([]string{"-addr", addr}, args...)
	code = mainRun(full, &out, &errBuf)
	return out.String(), errBuf.String(), code
}

func TestCLIEnqueueDequeueAckEndToEnd(t *testing.T) {
	_, addr := newCLITestServer(t)

	out, errOut, code := runCLI(addr, "enqueue", "work", "--payload", "hello")
	if code != exitOK {
		t.Fatalf("enqueue: code=%d stderr=%s", code, errOut)
	}
	id := strings.TrimSpace(out)
	if id == "" {
		t.Fatal("empty job id")
	}

	out, errOut, code = runCLI(addr, "dequeue", "--json", "work")
	if code != exitOK {
		t.Fatalf("dequeue: code=%d stderr=%s", code, errOut)
	}
	var got dequeueOutput
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode dequeue json: %v\n%s", err, out)
	}
	if got.Job.ID != id {
		t.Errorf("dequeued job id = %s, want %s", got.Job.ID, id)
	}

	_, errOut, code = runCLI(addr, "ack", "work", got.Lease.ID)
	if code != exitOK {
		t.Fatalf("ack: code=%d stderr=%s", code, errOut)
	}

	out, _, code = runCLI(addr, "stats", "--json", "work")
	if code != exitOK {
		t.Fatal("stats failed")
	}
	var st jobq.Stats
	if err := json.Unmarshal([]byte(out), &st); err != nil {
		t.Fatal(err)
	}
	if st.Acked != 1 {
		t.Errorf("Acked = %d, want 1", st.Acked)
	}
}

func TestCLIDequeueNoJobExitsThree(t *testing.T) {
	_, addr := newCLITestServer(t)
	_, _, code := runCLI(addr, "dequeue", "--wait", "0s", "never-used")
	if code != exitNoJob {
		t.Errorf("code = %d, want %d (exitNoJob)", code, exitNoJob)
	}
}

func TestCLIAckUnknownLeaseExitsError(t *testing.T) {
	_, addr := newCLITestServer(t)
	_, _, code := runCLI(addr, "ack", "work", "nope")
	if code != exitError {
		t.Errorf("code = %d, want %d (exitError)", code, exitError)
	}
}

func TestCLIMissingArgsExitsUsage(t *testing.T) {
	_, addr := newCLITestServer(t)
	_, _, code := runCLI(addr, "ack", "onlyonearg")
	if code != exitUsage {
		t.Errorf("code = %d, want %d (exitUsage)", code, exitUsage)
	}
}

func TestCLIUnknownCommandExitsUsage(t *testing.T) {
	_, addr := newCLITestServer(t)
	_, _, code := runCLI(addr, "bogus-command")
	if code != exitUsage {
		t.Errorf("code = %d, want %d (exitUsage)", code, exitUsage)
	}
}

func TestCLINoArgsExitsUsage(t *testing.T) {
	var out, errBuf bytes.Buffer
	code := mainRun(nil, &out, &errBuf)
	if code != exitUsage {
		t.Errorf("code = %d, want %d (exitUsage)", code, exitUsage)
	}
}

func TestCLITopicsAndHealth(t *testing.T) {
	q, addr := newCLITestServer(t)
	if _, err := q.Enqueue("alpha", []byte("x")); err != nil {
		t.Fatal(err)
	}

	out, _, code := runCLI(addr, "topics")
	if code != exitOK || strings.TrimSpace(out) != "alpha" {
		t.Errorf("topics: code=%d out=%q", code, out)
	}

	out, _, code = runCLI(addr, "health")
	if code != exitOK || !strings.Contains(out, "status:  ok") {
		t.Errorf("health: code=%d out=%q", code, out)
	}
}

func TestCLIExtend(t *testing.T) {
	q, addr := newCLITestServer(t)
	if _, err := q.Enqueue("work", []byte("x")); err != nil {
		t.Fatal(err)
	}
	_, lease, err := q.Dequeue("work", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	out, errOut, code := runCLI(addr, "extend", "--by", "1m", "work", lease.ID)
	if code != exitOK {
		t.Fatalf("extend: code=%d stderr=%s", code, errOut)
	}
	if strings.TrimSpace(out) == "" {
		t.Error("expected a deadline printed")
	}
}

func TestCLIGlobalFlagsBothOrders(t *testing.T) {
	_, addr := newCLITestServer(t)

	// Flags before subcommand.
	var out1, err1 bytes.Buffer
	code := mainRun([]string{"-addr", addr, "topics"}, &out1, &err1)
	if code != exitOK {
		t.Fatalf("flags-before: code=%d stderr=%s", code, err1.String())
	}

	// Flags after subcommand.
	var out2, err2 bytes.Buffer
	code = mainRun([]string{"topics", "-addr", addr}, &out2, &err2)
	if code != exitOK {
		t.Fatalf("flags-after: code=%d stderr=%s", code, err2.String())
	}
}
