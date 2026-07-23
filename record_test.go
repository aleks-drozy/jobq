package jobq

import (
	"bytes"
	"errors"
	"testing"
	"time"
)

func sampleEnqueue() walRecord {
	return walRecord{
		Type:        recEnqueue,
		JobID:       "0123456789abcdef0123456789abcdef",
		TopicID:     3,
		MaxAttempts: 5,
		At:          time.Date(2026, 7, 22, 21, 0, 0, 123456789, time.UTC),
		Delay:       90 * time.Second,
		Payload:     []byte("hello, wal"),
	}
}

func TestRecordRoundTripsAllTypes(t *testing.T) {
	records := []walRecord{
		sampleEnqueue(),
		{Type: recTopic, TopicID: 3, TopicName: "emails"},
		{Type: recDeliver, JobID: sampleEnqueue().JobID, Attempts: 2},
		{Type: recRetry, JobID: sampleEnqueue().JobID, Attempts: 3},
		{Type: recAck, JobID: sampleEnqueue().JobID},
		{Type: recAdopt, JobID: sampleEnqueue().JobID, TopicID: 9,
			Attempts: 5, At: time.Date(2026, 7, 22, 22, 0, 0, 0, time.UTC)},
		{Type: recDrop, JobID: sampleEnqueue().JobID},
		{Type: recCleanShut},
	}
	for _, rec := range records {
		var buf bytes.Buffer
		if err := writeRecord(&buf, rec); err != nil {
			t.Fatalf("write %v: %v", rec.Type, err)
		}
		got, n, err := readRecord(buf.Bytes())
		if err != nil {
			t.Fatalf("read %v: %v", rec.Type, err)
		}
		if n != buf.Len() {
			t.Errorf("%v: consumed %d of %d bytes", rec.Type, n, buf.Len())
		}
		if !recordsEqual(rec, got) {
			t.Errorf("%v round trip:\n in: %+v\nout: %+v", rec.Type, rec, got)
		}
	}
}

func recordsEqual(a, b walRecord) bool {
	return a.Type == b.Type && a.JobID == b.JobID && a.TopicID == b.TopicID &&
		a.TopicName == b.TopicName && a.MaxAttempts == b.MaxAttempts &&
		a.Attempts == b.Attempts && a.At.Equal(b.At) && a.Delay == b.Delay &&
		bytes.Equal(a.Payload, b.Payload)
}

func TestZeroTimeRoundTripsToZero(t *testing.T) {
	// time.Time{}.UnixNano() does not round-trip to IsZero(); the sec+nsec
	// encoding must. ADOPT and ENQUEUE both carry times that can be zero.
	rec := walRecord{Type: recAdopt, JobID: sampleEnqueue().JobID, TopicID: 1}
	var buf bytes.Buffer
	if err := writeRecord(&buf, rec); err != nil {
		t.Fatal(err)
	}
	got, _, err := readRecord(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if !got.At.IsZero() {
		t.Errorf("zero time decoded as %v", got.At)
	}
}

func TestCorruptLengthIsDetectedNotFollowed(t *testing.T) {
	// The CRC covers the length prefix. Two cases, one contract each:
	//
	// A length corrupted DOWNWARD still leaves a complete (smaller) frame in
	// the buffer, so the CRC must catch it: errBadFrame, never a "valid"
	// short record.
	var buf bytes.Buffer
	if err := writeRecord(&buf, sampleEnqueue()); err != nil {
		t.Fatal(err)
	}
	down := append([]byte(nil), buf.Bytes()...)
	down[0] = 0x08 // shrink the length to 8 (frame still fits in the buffer)
	if _, _, err := readRecord(down); !errors.Is(err, errBadFrame) {
		t.Fatalf("length corrupted downward -> %v, want errBadFrame", err)
	}

	// A length corrupted UPWARD claims bytes that do not exist; no reader
	// can CRC bytes it does not have, so locally this is indistinguishable
	// from a torn tail. The reader reports errShortFrame and RECOVERY
	// resolves it by position: truncate at the tail, refuse mid-log.
	up := append([]byte(nil), buf.Bytes()...)
	up[1] ^= 0x40
	if _, _, err := readRecord(up); !errors.Is(err, errShortFrame) {
		t.Fatalf("length corrupted upward -> %v, want errShortFrame", err)
	}
	// Give the upward-corrupted frame enough trailing bytes and the CRC
	// must catch it after all.
	upPadded := append(append([]byte(nil), up...), bytes.Repeat([]byte{0xAA}, 1<<15)...)
	if _, _, err := readRecord(upPadded); !errors.Is(err, errBadFrame) {
		t.Fatalf("length corrupted upward with bytes available -> %v, want errBadFrame", err)
	}
}

func TestCorruptBodyIsDetected(t *testing.T) {
	var buf bytes.Buffer
	if err := writeRecord(&buf, sampleEnqueue()); err != nil {
		t.Fatal(err)
	}
	raw := buf.Bytes()
	raw[len(raw)-3] ^= 0x01
	if _, _, err := readRecord(raw); !errors.Is(err, errBadFrame) {
		t.Fatalf("corrupt body -> %v, want errBadFrame", err)
	}
}

func TestTruncatedFrameReportsShortNotCorrupt(t *testing.T) {
	// A torn tail (crash mid-write) must be distinguishable from corruption:
	// short frames are errShortFrame so recovery truncates instead of
	// refusing to start.
	var buf bytes.Buffer
	if err := writeRecord(&buf, sampleEnqueue()); err != nil {
		t.Fatal(err)
	}
	raw := buf.Bytes()
	for cut := 1; cut < len(raw); cut++ {
		if _, _, err := readRecord(raw[:cut]); !errors.Is(err, errShortFrame) {
			t.Fatalf("cut at %d/%d -> %v, want errShortFrame", cut, len(raw), err)
		}
	}
}

func FuzzReadRecordNeverPanics(f *testing.F) {
	var buf bytes.Buffer
	_ = writeRecord(&buf, sampleEnqueue())
	f.Add(buf.Bytes())
	f.Add([]byte{})
	f.Add(bytes.Repeat([]byte{0xff}, 64))
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _, _ = readRecord(data) // must never panic or over-read
	})
}
