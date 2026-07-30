package main

// The `jobq work` end-to-end test reuses the self-exec pattern from the
// jobq package's own crash_test.go: this test binary re-executes itself as
// the wrapped <cmd>, gated by an environment variable so a normal `go test`
// run never accidentally recurses into it. The fake worker's exit code is
// driven by its stdin payload ("ok" -> 0, "fail" -> nonzero, "sleep" ->
// blocks until killed) so a single tiny binary covers ack, nack, and the
// SIGINT-nacks-the-in-flight-job case.

import (
	"context"
	"fmt"
	"io"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/aleks-drozy/jobq"
	"github.com/aleks-drozy/jobq/jobqhttp"
)

const fakeWorkerEnv = "JOBQ_TEST_FAKE_WORKER"

func TestMain(m *testing.M) {
	if os.Getenv(fakeWorkerEnv) != "" {
		fakeWorkerMain()
		return // unreachable; fakeWorkerMain always exits
	}
	os.Exit(m.Run())
}

// fakeWorkerMain is the process cmdWork's exec.Command actually runs in the
// self-exec tests below. It behaves per its stdin payload.
func fakeWorkerMain() {
	payload, _ := io.ReadAll(os.Stdin)
	fmt.Fprintf(os.Stdout, "attempt=%s max=%s job=%s topic=%s\n",
		os.Getenv("JOBQ_ATTEMPT"), os.Getenv("JOBQ_MAX_ATTEMPTS"),
		os.Getenv("JOBQ_JOB_ID"), os.Getenv("JOBQ_TOPIC"))
	switch string(payload) {
	case "ok":
		os.Exit(0)
	case "fail":
		os.Exit(1)
	case "sleep":
		time.Sleep(30 * time.Second)
		os.Exit(0)
	default:
		os.Exit(1)
	}
}

func fakeWorkerCommand(t *testing.T) []string {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	return []string{exe, "-test.run=^$"}
}

func newWorkTestServer(t *testing.T) (*jobq.Queue, *jobqhttp.Client) {
	t.Helper()
	q := jobq.New()
	t.Cleanup(func() { _ = q.Close() })
	srv := httptest.NewServer(jobqhttp.NewHandler(q))
	t.Cleanup(srv.Close)
	c := jobqhttp.NewClient(srv.URL)
	return q, c
}

// withFakeWorkerEnv sets the gate env var for the duration of the test so
// exec.Command's child inherits it via os.Environ() (runOneJob appends to
// os.Environ(), not a fresh env), and restores it after.
func withFakeWorkerEnv(t *testing.T) {
	t.Helper()
	t.Setenv(fakeWorkerEnv, "1")
}

func TestWorkLoopAcksOnSuccess(t *testing.T) {
	withFakeWorkerEnv(t)
	q, c := newWorkTestServer(t)
	if _, err := c.Enqueue("jobs", []byte("ok"), jobq.WithMaxAttempts(3)); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytesBuf
	code := runWorkLoop(context.Background(), c, "jobs", 10*time.Second, 500*time.Millisecond, 1, fakeWorkerCommand(t), &stdout, &stderr)
	if code != exitOK {
		t.Fatalf("runWorkLoop code = %d, stderr:\n%s", code, stderr.String())
	}
	st := q.Stats("jobs")
	if st.Acked != 1 {
		t.Errorf("Acked = %d, want 1", st.Acked)
	}
	if got := stdout.String(); got == "" {
		t.Error("expected the worker's stdout to be forwarded")
	}
}

func TestWorkLoopNacksOnFailureThenDeadLetters(t *testing.T) {
	withFakeWorkerEnv(t)
	q, c := newWorkTestServer(t)
	if _, err := c.Enqueue("jobs", []byte("fail"), jobq.WithMaxAttempts(2)); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytesBuf
	// max-jobs=2: first delivery nacks (retry), second delivery exhausts
	// the budget and dead-letters — the whole P1 contract, driven by exit
	// codes instead of Ack/Nack calls made directly.
	code := runWorkLoop(context.Background(), c, "jobs", 10*time.Second, 500*time.Millisecond, 2, fakeWorkerCommand(t), &stdout, &stderr)
	if code != exitOK {
		t.Fatalf("runWorkLoop code = %d, stderr:\n%s", code, stderr.String())
	}
	st := q.Stats("jobs")
	if st.DeadLetters != 1 {
		t.Errorf("DeadLetters = %d, want 1", st.DeadLetters)
	}
	dlqSt := q.Stats(jobq.DeadLetterTopic("jobs"))
	if dlqSt.Ready != 1 {
		t.Errorf("dlq Ready = %d, want 1", dlqSt.Ready)
	}
}

func TestWorkLoopNoJobKeepsPolling(t *testing.T) {
	withFakeWorkerEnv(t)
	_, c := newWorkTestServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	var stdout, stderr bytesBuf
	code := runWorkLoop(ctx, c, "never-arrives", 10*time.Second, 100*time.Millisecond, 0, fakeWorkerCommand(t), &stdout, &stderr)
	if code != exitOK {
		t.Fatalf("runWorkLoop code = %d, stderr:\n%s", code, stderr.String())
	}
}

func TestWorkLoopContextCancelNacksInFlightJob(t *testing.T) {
	withFakeWorkerEnv(t)
	q, c := newWorkTestServer(t)
	if _, err := c.Enqueue("jobs", []byte("sleep"), jobq.WithMaxAttempts(3)); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	var stdout, stderr bytesBuf
	done := make(chan int, 1)
	go func() {
		done <- runWorkLoop(ctx, c, "jobs", 30*time.Second, 500*time.Millisecond, 1, fakeWorkerCommand(t), &stdout, &stderr)
	}()

	time.Sleep(200 * time.Millisecond) // let the sleeping worker start
	cancel()

	select {
	case code := <-done:
		if code != exitOK {
			t.Fatalf("runWorkLoop code = %d, stderr:\n%s", code, stderr.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("runWorkLoop did not return after context cancellation")
	}

	st := q.Stats("jobs")
	if st.Ready != 1 {
		t.Errorf("Ready = %d, want 1 (nacked back to ready after SIGINT-equivalent)", st.Ready)
	}
	if st.InFlight != 0 {
		t.Errorf("InFlight = %d, want 0", st.InFlight)
	}
}

func TestWorkUsageErrorsWithoutDoubleDash(t *testing.T) {
	withFakeWorkerEnv(t)
	_, c := newWorkTestServer(t)
	_ = c
	var stdout, stderr bytesBuf
	code := mainRun([]string{"work", "sometopic"}, &stdout, &stderr)
	if code != exitUsage {
		t.Errorf("code = %d, want exitUsage; stderr=%s", code, stderr.String())
	}
}

// bytesBuf is a minimal io.Writer + String() so tests don't need to import
// bytes.Buffer under a different name colliding with cmd's own imports.
type bytesBuf struct{ data []byte }

func (b *bytesBuf) Write(p []byte) (int, error) {
	b.data = append(b.data, p...)
	return len(p), nil
}
func (b *bytesBuf) String() string { return string(b.data) }
