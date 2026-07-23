package jobq

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// logFor writes a sequence of records through a real WAL and closes it
// cleanly unless clean is false, in which case the CLEANSHUT and dir-lock
// release are skipped by killing the wal struct without close().
func logFor(t *testing.T, clean bool, recs ...walRecord) string {
	t.Helper()
	dir := t.TempDir()
	w, err := openWAL(dir, walOptions{policy: SyncAlways})
	if err != nil {
		t.Fatal(err)
	}
	for _, rec := range recs {
		if err := <-w.append(rec, true); err != nil {
			t.Fatal(err)
		}
	}
	if clean {
		if err := w.close(); err != nil {
			t.Fatal(err)
		}
	} else {
		// Simulate a crash: release the lock so the test can reopen, but
		// write no CLEANSHUT.
		if err := w.seg.closeFile(); err != nil {
			t.Fatal(err)
		}
		if err := releaseDirLock(w.lock); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

var (
	tEnq = time.Date(2026, 7, 22, 20, 0, 0, 0, time.UTC)
	j1   = "11111111111111111111111111111111"
	j2   = "22222222222222222222222222222222"
)

func topicRec() walRecord { return walRecord{Type: recTopic, TopicID: 1, TopicName: "mail"} }

func enqRec(id string, max int) walRecord {
	return walRecord{Type: recEnqueue, JobID: id, TopicID: 1, MaxAttempts: max,
		At: tEnq, Payload: []byte("p-" + id[:2])}
}

func TestReplayRestoresReadyJobsInFIFOOrder(t *testing.T) {
	dir := logFor(t, true, topicRec(), enqRec(j1, 5), enqRec(j2, 5))
	st, rep, err := replayDir(filepath.Join(dir, "wal"))
	if err != nil {
		t.Fatal(err)
	}
	if !rep.CleanShutdown {
		t.Error("clean shutdown not detected")
	}
	jobs := st.topicJobs["mail"]
	if len(jobs) != 2 || jobs[0].ID != j1 || jobs[1].ID != j2 {
		t.Fatalf("restored order wrong: %+v", jobs)
	}
	if string(jobs[0].Payload) != "p-11" || jobs[0].MaxAttempts != 5 || !jobs[0].EnqueuedAt.Equal(tEnq) {
		t.Errorf("job fields lost: %+v", jobs[0])
	}
}

func TestReplayTreatsInFlightAsExpired(t *testing.T) {
	// DELIVER without a later settle: restart = lease expiry. The job comes
	// back ready with its attempt count intact.
	dir := logFor(t, false, topicRec(), enqRec(j1, 5),
		walRecord{Type: recDeliver, JobID: j1, Attempts: 3})
	st, rep, err := replayDir(filepath.Join(dir, "wal"))
	if err != nil {
		t.Fatal(err)
	}
	if rep.CleanShutdown {
		t.Error("crash misdetected as clean shutdown")
	}
	jobs := st.topicJobs["mail"]
	if len(jobs) != 1 || jobs[0].Attempts != 3 {
		t.Fatalf("in-flight job not restored with attempts: %+v", jobs)
	}
}

func TestReplayDeadLettersExhaustedInFlight(t *testing.T) {
	// A job crashed mid-final-attempt has no budget left: it must land in
	// the DLQ at replay, not get a bonus delivery.
	dir := logFor(t, false, topicRec(), enqRec(j1, 2),
		walRecord{Type: recDeliver, JobID: j1, Attempts: 2})
	st, _, err := replayDir(filepath.Join(dir, "wal"))
	if err != nil {
		t.Fatal(err)
	}
	if n := len(st.topicJobs["mail"]); n != 0 {
		t.Fatalf("exhausted job still on mail: %d", n)
	}
	dlq := st.topicJobs["mail.dlq"]
	if len(dlq) != 1 || dlq[0].DeadLetteredAttempts != 2 || dlq[0].Attempts != 0 {
		t.Fatalf("exhausted in-flight not dead-lettered correctly: %+v", dlq)
	}
}

func TestReplayAckedAndDroppedJobsStayGone(t *testing.T) {
	dir := logFor(t, true, topicRec(), enqRec(j1, 5), enqRec(j2, 5),
		walRecord{Type: recDeliver, JobID: j1, Attempts: 1},
		walRecord{Type: recAck, JobID: j1},
		walRecord{Type: recDeliver, JobID: j2, Attempts: 1},
		walRecord{Type: recDrop, JobID: j2})
	st, rep, err := replayDir(filepath.Join(dir, "wal"))
	if err != nil {
		t.Fatal(err)
	}
	if n := len(st.topicJobs["mail"]); n != 0 {
		t.Fatalf("terminal jobs resurrected: %d", n)
	}
	if rep.JobsRestored != 0 {
		t.Errorf("JobsRestored = %d, want 0", rep.JobsRestored)
	}
}

func TestReplayHonoursAdoptAndRetryOrder(t *testing.T) {
	// j1 nacked once (RETRY) then adopted into the DLQ; j2 retried after,
	// so the mail FIFO holds only j2.
	dir := logFor(t, true, topicRec(),
		walRecord{Type: recTopic, TopicID: 2, TopicName: "mail.dlq"},
		enqRec(j1, 5), enqRec(j2, 5),
		walRecord{Type: recDeliver, JobID: j1, Attempts: 1},
		walRecord{Type: recRetry, JobID: j1, Attempts: 1},
		walRecord{Type: recDeliver, JobID: j2, Attempts: 1},
		walRecord{Type: recAdopt, JobID: j1, TopicID: 2, Attempts: 5, At: tEnq.Add(time.Hour)},
		walRecord{Type: recRetry, JobID: j2, Attempts: 1})
	st, _, err := replayDir(filepath.Join(dir, "wal"))
	if err != nil {
		t.Fatal(err)
	}
	mail := st.topicJobs["mail"]
	if len(mail) != 1 || mail[0].ID != j2 {
		t.Fatalf("mail after adopt/retry: %+v", mail)
	}
	dlq := st.topicJobs["mail.dlq"]
	if len(dlq) != 1 || dlq[0].ID != j1 || dlq[0].DeadLetteredAttempts != 5 ||
		!dlq[0].DeadLetteredAt.Equal(tEnq.Add(time.Hour)) {
		t.Fatalf("adopted job wrong: %+v", dlq)
	}
}

func TestReplayTruncatesTornTail(t *testing.T) {
	dir := logFor(t, false, topicRec(), enqRec(j1, 5), enqRec(j2, 5))
	segs, err := listSegments(filepath.Join(dir, "wal"))
	if err != nil {
		t.Fatal(err)
	}
	last := segs[len(segs)-1].path
	raw, _ := os.ReadFile(last)
	if err := os.WriteFile(last, raw[:len(raw)-7], 0o644); err != nil { // tear j2 mid-frame
		t.Fatal(err)
	}
	st, rep, err := replayDir(filepath.Join(dir, "wal"))
	if err != nil {
		t.Fatal(err)
	}
	if !rep.TornTail || rep.BytesTruncated == 0 {
		t.Errorf("torn tail not reported: %+v", rep)
	}
	if jobs := st.topicJobs["mail"]; len(jobs) != 1 || jobs[0].ID != j1 {
		t.Fatalf("state after truncation: %+v", jobs)
	}
	// The file itself must have been truncated so the next writer appends
	// after a valid record, not after garbage.
	again, rep2, err := replayDir(filepath.Join(dir, "wal"))
	if err != nil {
		t.Fatal(err)
	}
	if rep2.TornTail {
		t.Error("second replay still sees a torn tail")
	}
	if jobs := again.topicJobs["mail"]; len(jobs) != 1 {
		t.Fatalf("second replay state: %+v", jobs)
	}
}

func TestReplayRefusesCorruptionBeforeLastSegment(t *testing.T) {
	dir := t.TempDir()
	w, err := openWAL(dir, walOptions{policy: SyncAlways, segmentBytes: 128})
	if err != nil {
		t.Fatal(err)
	}
	if err := <-w.append(topicRec(), true); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 8; i++ { // force several segments
		if err := <-w.append(enqRec(newID(), 5), true); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.close(); err != nil {
		t.Fatal(err)
	}
	segs, _ := listSegments(filepath.Join(dir, "wal"))
	if len(segs) < 3 {
		t.Fatalf("need ≥3 segments, got %d", len(segs))
	}
	victim := segs[0].path
	raw, _ := os.ReadFile(victim)
	raw[segmentHeaderSize+5] ^= 0xff // corrupt record bytes in a non-tail segment
	if err := os.WriteFile(victim, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := replayDir(filepath.Join(dir, "wal")); err == nil {
		t.Fatal("mid-log corruption accepted; recovery must refuse to guess")
	}
}
