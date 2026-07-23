package jobq

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func openTestWAL(t *testing.T, opts walOptions) (*wal, string) {
	t.Helper()
	dir := t.TempDir()
	w, err := openWAL(dir, opts)
	if err != nil {
		t.Fatalf("openWAL: %v", err)
	}
	t.Cleanup(func() { _ = w.close() })
	return w, dir
}

// readBack decodes every record in every segment, in order.
func readBack(t *testing.T, dir string) []walRecord {
	t.Helper()
	segs, err := listSegments(filepath.Join(dir, "wal"))
	if err != nil {
		t.Fatalf("listSegments: %v", err)
	}
	var out []walRecord
	for _, seg := range segs {
		raw, err := os.ReadFile(seg.path)
		if err != nil {
			t.Fatal(err)
		}
		buf := raw[segmentHeaderSize:]
		for len(buf) > 0 {
			rec, n, err := readRecord(buf)
			if err != nil {
				t.Fatalf("decode in %s: %v", seg.path, err)
			}
			out = append(out, rec)
			buf = buf[n:]
		}
	}
	return out
}

func TestWALPersistsRecordsInOrder(t *testing.T) {
	w, dir := openTestWAL(t, walOptions{policy: SyncAlways})
	ids := []string{}
	for i := 0; i < 20; i++ {
		id := newID()
		ids = append(ids, id)
		if err := <-w.append(walRecord{Type: recAck, JobID: id}, true); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if err := w.close(); err != nil {
		t.Fatal(err)
	}
	recs := readBack(t, dir)
	if len(recs) != 21 { // 20 acks + CLEANSHUT
		t.Fatalf("read back %d records, want 21", len(recs))
	}
	for i, id := range ids {
		if recs[i].JobID != id {
			t.Fatalf("record %d out of order", i)
		}
	}
	if recs[20].Type != recCleanShut {
		t.Errorf("last record = %v, want CLEANSHUT", recs[20].Type)
	}
}

func TestWALGroupCommitReleasesConcurrentWaiters(t *testing.T) {
	w, _ := openTestWAL(t, walOptions{policy: SyncAlways})
	const n = 50
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		go func() {
			errs <- <-w.append(walRecord{Type: recAck, JobID: newID()}, true)
		}()
	}
	for i := 0; i < n; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("waiter %d: %v", i, err)
		}
	}
}

func TestWALIntervalPolicyDoesNotGateAppends(t *testing.T) {
	w, _ := openTestWAL(t, walOptions{policy: SyncInterval, interval: time.Hour})
	done := make(chan error, 1)
	go func() { done <- <-w.append(walRecord{Type: recAck, JobID: newID()}, true) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("append under SyncInterval blocked on fsync")
	}
}

func TestWALRotatesSegments(t *testing.T) {
	w, dir := openTestWAL(t, walOptions{policy: SyncAlways, segmentBytes: 4 << 10})
	payload := bytes.Repeat([]byte("x"), 1024)
	for i := 0; i < 12; i++ {
		rec := walRecord{Type: recEnqueue, JobID: newID(), TopicID: 1,
			MaxAttempts: 5, At: time.Now().UTC(), Payload: payload}
		if err := <-w.append(rec, true); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.close(); err != nil {
		t.Fatal(err)
	}
	segs, err := listSegments(filepath.Join(dir, "wal"))
	if err != nil {
		t.Fatal(err)
	}
	if len(segs) < 3 {
		t.Fatalf("wrote ~12KiB with 4KiB segments but got %d segment(s)", len(segs))
	}
	if recs := readBack(t, dir); len(recs) != 13 {
		t.Fatalf("read back %d records across segments, want 13", len(recs))
	}
	for i, seg := range segs[1:] {
		if seg.baseOffset <= segs[i].baseOffset {
			t.Errorf("segment offsets not increasing: %d then %d", segs[i].baseOffset, seg.baseOffset)
		}
	}
}

func TestWALAppendAfterCloseFails(t *testing.T) {
	w, _ := openTestWAL(t, walOptions{policy: SyncAlways})
	if err := w.close(); err != nil {
		t.Fatal(err)
	}
	if err := <-w.append(walRecord{Type: recAck, JobID: newID()}, true); !errors.Is(err, ErrClosed) {
		t.Fatalf("append after close = %v, want ErrClosed", err)
	}
}

func TestWALRefusesSecondProcess(t *testing.T) {
	_, dir := openTestWAL(t, walOptions{policy: SyncAlways})
	if _, err := openWAL(dir, walOptions{policy: SyncAlways}); err == nil {
		t.Fatal("second openWAL on a locked dir succeeded")
	}
}

func TestSegmentHeaderCorruptionIsRejected(t *testing.T) {
	w, dir := openTestWAL(t, walOptions{policy: SyncAlways})
	if err := <-w.append(walRecord{Type: recAck, JobID: newID()}, true); err != nil {
		t.Fatal(err)
	}
	if err := w.close(); err != nil {
		t.Fatal(err)
	}
	segs, _ := listSegments(filepath.Join(dir, "wal"))
	raw, _ := os.ReadFile(segs[0].path)
	raw[3] ^= 0xff // corrupt the magic
	if err := os.WriteFile(segs[0].path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := listSegments(filepath.Join(dir, "wal")); err == nil {
		t.Fatal("corrupt segment header accepted")
	}
}
