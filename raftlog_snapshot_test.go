package jobq

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aleks-drozy/jobq/raft"
)

// copyDir copies the files of src into a fresh directory, lock file
// excluded, and returns it. It is how these tests keep a picture of a log
// directory to put pieces of back later.
func copyDir(t *testing.T, src string) string {
	t.Helper()
	dst := t.TempDir()
	err := filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(src, path)
		if info.IsDir() {
			return os.MkdirAll(filepath.Join(dst, rel), 0o700)
		}
		if info.Name() == "jobq.lock" {
			return nil
		}
		in, err := os.Open(path)
		if err != nil {
			return err
		}
		defer in.Close()
		out, err := os.Create(filepath.Join(dst, rel))
		if err != nil {
			return err
		}
		defer out.Close()
		_, err = io.Copy(out, in)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return dst
}

func segmentNames(t *testing.T, dir string) []string {
	t.Helper()
	segs, err := listSegments(walSubdir(dir))
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, s := range segs {
		names = append(names, filepath.Base(s.path))
	}
	return names
}

// loadAll opens dir, loads it and closes it again.
func loadAll(t *testing.T, dir string) (raft.HardState, raft.Snapshot, []raft.Entry) {
	t.Helper()
	l, _ := mustOpenRaftLog(t, dir)
	hard, snap, entries, err := l.Load()
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	return hard, snap, entries
}

func wantEntries(t *testing.T, got []raft.Entry, from, to uint64) {
	t.Helper()
	if to < from {
		if len(got) != 0 {
			t.Fatalf("got %d entries, want none", len(got))
		}
		return
	}
	if uint64(len(got)) != to-from+1 || got[0].Index != from || got[len(got)-1].Index != to {
		first, last := uint64(0), uint64(0)
		if len(got) > 0 {
			first, last = got[0].Index, got[len(got)-1].Index
		}
		t.Fatalf("got %d entries [%d, %d], want [%d, %d]", len(got), first, last, from, to)
	}
	for i, e := range got {
		if e.Index != from+uint64(i) {
			t.Fatalf("entry %d has index %d", i, e.Index)
		}
	}
}

// compacted builds a log of entries 1..20, compacts it at 15 keeping
// 13..20, appends 21 and closes it. It returns the directory, a copy of
// the directory from just before the compaction, and the names of the
// segments the compaction wrote.
func compacted(t *testing.T) (dir, before string, written []string) {
	t.Helper()
	dir = t.TempDir()
	l := mustOpenRaftLogOpts(t, dir, WithSegmentSize(96))
	if err := l.SaveHardState(raft.HardState{Term: 3, VotedFor: 2}); err != nil {
		t.Fatal(err)
	}
	all := raftEntries(1, 20, 3)
	for _, e := range all { // one at a time, so the log spans many segments
		if err := l.Append([]raft.Entry{e}); err != nil {
			t.Fatal(err)
		}
		if err := l.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	before = copyDir(t, dir)
	old := map[string]bool{}
	for _, name := range segmentNames(t, dir) {
		old[name] = true
	}
	if len(old) < 4 {
		_ = l.Close()
		t.Fatalf("the log spans only %d segments; the test needs several", len(old))
	}

	if err := l.SaveSnapshot(raft.Snapshot{Index: 15, Term: 3, Data: []byte("state@15")}, all[12:]); err != nil {
		t.Fatal(err)
	}
	for _, name := range segmentNames(t, dir) {
		if old[name] {
			t.Fatalf("segment %s survived the compaction", name)
		}
		written = append(written, name)
	}
	if err := l.Append(raftEntries(21, 21, 3)); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	return dir, before, written
}

func mustOpenRaftLogOpts(t *testing.T, dir string, opts ...Option) *RaftLog {
	t.Helper()
	l, _, err := OpenRaftLog(dir, opts...)
	if err != nil {
		t.Fatalf("OpenRaftLog: %v", err)
	}
	return l
}

func TestRaftLogCompactionSurvivesReopen(t *testing.T) {
	dir, _, _ := compacted(t)

	l, rep := mustOpenRaftLog(t, dir)
	defer l.Close()
	if rep.SnapshotIndex != 15 || rep.Entries != 9 {
		t.Fatalf("report %+v, want a snapshot at 15 and 9 entries", rep)
	}
	hard, snap, entries, err := l.Load()
	if err != nil {
		t.Fatal(err)
	}
	if hard.Term != 3 || hard.VotedFor != 2 {
		t.Fatalf("term and vote did not survive the compaction: %+v", hard)
	}
	if snap.Index != 15 || snap.Term != 3 || string(snap.Data) != "state@15" {
		t.Fatalf("snapshot %+v", snap)
	}
	wantEntries(t, entries, 13, 21)
	if !bytes.Equal(entries[0].Data, []byte("e13")) {
		t.Fatalf("entry 13 came back as %q", entries[0].Data)
	}
	// The log carries on from where it was.
	if err := l.Append(raftEntries(22, 22, 3)); err != nil {
		t.Fatal(err)
	}
	if err := l.TruncateFrom(15); err == nil {
		t.Fatal("truncating into the snapshot succeeded")
	}
}

func TestRaftLogCompactionGivesDiskBack(t *testing.T) {
	dir := t.TempDir()
	l := mustOpenRaftLogOpts(t, dir, WithSegmentSize(4096))
	defer l.Close()
	payload := bytes.Repeat([]byte("x"), 512)
	var all []raft.Entry
	for i := uint64(1); i <= 400; i++ {
		e := raft.Entry{Index: i, Term: 1, Data: payload}
		all = append(all, e)
		if err := l.Append([]raft.Entry{e}); err != nil {
			t.Fatal(err)
		}
		if i%10 == 0 {
			if err := l.Sync(); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := l.Sync(); err != nil {
		t.Fatal(err)
	}
	before, _ := l.DiskUsage()
	if err := l.SaveSnapshot(raft.Snapshot{Index: 400, Term: 1, Data: []byte("small")}, all[390:]); err != nil {
		t.Fatal(err)
	}
	after, snapBytes := l.DiskUsage()
	if before < 200_000 {
		t.Fatalf("the log took only %d bytes before compaction", before)
	}
	if after > before/10 {
		t.Fatalf("log took %d bytes before compaction and still takes %d", before, after)
	}
	if snapBytes != snapshotHeaderSize+5 {
		t.Fatalf("snapshot takes %d bytes", snapBytes)
	}
}

// A follower that is sent a snapshot drops its whole log.
func TestRaftLogInstalledSnapshotReplacesTheLog(t *testing.T) {
	dir := t.TempDir()
	l, _ := mustOpenRaftLog(t, dir)
	if err := l.Append(raftEntries(1, 10, 1)); err != nil {
		t.Fatal(err)
	}
	if err := l.SaveSnapshot(raft.Snapshot{Index: 50, Term: 4, Data: []byte("state@50")}, nil); err != nil {
		t.Fatal(err)
	}
	if err := l.Append(raftEntries(11, 11, 4)); err == nil {
		t.Fatal("append at the old position succeeded after the log was replaced")
	}
	_ = l.Close()

	l, _ = mustOpenRaftLog(t, dir)
	_, snap, entries, _ := l.Load()
	if snap.Index != 50 || string(snap.Data) != "state@50" || len(entries) != 0 {
		t.Fatalf("snapshot %+v with %d entries", snap, len(entries))
	}
	if err := l.Append(raftEntries(51, 52, 4)); err != nil {
		t.Fatalf("append after the snapshot: %v", err)
	}
	_ = l.Close()
	_, _, entries = loadAll(t, dir)
	wantEntries(t, entries, 51, 52)
}

// The three places a crash can land during a compaction, rebuilt from a
// copy of the directory taken just before it.

// After the swap is on disk but before any old segment is deleted.
func TestRaftLogCrashBeforeOldSegmentsAreDeleted(t *testing.T) {
	dir, before, _ := compacted(t)
	for _, name := range segmentNames(t, before) {
		copyFile(t, filepath.Join(walSubdir(before), name), filepath.Join(walSubdir(dir), name))
	}
	_, snap, entries := loadAll(t, dir)
	if snap.Index != 15 {
		t.Fatalf("snapshot at %d, want 15", snap.Index)
	}
	wantEntries(t, entries, 13, 21)
}

// Part way through deleting them. Deletion runs newest first, so what a
// crash leaves is the oldest ones.
func TestRaftLogCrashPartWayThroughDeletingOldSegments(t *testing.T) {
	names := func(dir string) []string { return segmentNames(t, dir) }
	_, probe, _ := compacted(t)
	total := len(names(probe))
	for left := 1; left < total; left++ {
		dir, before, _ := compacted(t)
		for _, name := range names(before)[:left] {
			copyFile(t, filepath.Join(walSubdir(before), name), filepath.Join(walSubdir(dir), name))
		}
		_, snap, entries := loadAll(t, dir)
		if snap.Index != 15 {
			t.Fatalf("with the oldest %d segments left behind: snapshot at %d, want 15", left, snap.Index)
		}
		wantEntries(t, entries, 13, 21)
	}
}

// Before the swap record made it to disk: the old log is still the log,
// whole, and the snapshot file written ahead of it is thrown away.
func TestRaftLogCrashBeforeTheSwapKeepsTheOldLog(t *testing.T) {
	// One big segment for the old log and one for the compaction, so the
	// cut can be placed exactly.
	dir := t.TempDir()
	l, _ := mustOpenRaftLog(t, dir)
	if err := l.SaveHardState(raft.HardState{Term: 3, VotedFor: 2}); err != nil {
		t.Fatal(err)
	}
	all := raftEntries(1, 20, 3)
	if err := l.Append(all); err != nil {
		t.Fatal(err)
	}
	if err := l.Sync(); err != nil {
		t.Fatal(err)
	}
	before := copyDir(t, dir)
	if err := l.SaveSnapshot(raft.Snapshot{Index: 15, Term: 3, Data: []byte("state@15")}, all[12:]); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	wal := walSubdir(dir)
	written := segmentNames(t, dir)
	if len(written) != 1 {
		t.Fatalf("the compaction left %d segments, want 1", len(written))
	}
	for _, name := range segmentNames(t, before) {
		copyFile(t, filepath.Join(walSubdir(before), name), filepath.Join(wal, name))
	}
	// The compaction's segment holds a begin record, eight kept entries,
	// the swap, and the term and vote. Cut it off part way through the
	// fourth kept entry: three are staged and the swap never happened.
	begin := int64(len(encodeFrame(walRecord{Type: recRaftBegin})))
	keep := int64(len(encodeFrame(walRecord{Type: recRaftKeep, Index: 13, Term: 3, Payload: []byte("e13")})))
	if err := os.Truncate(filepath.Join(wal, written[0]), segmentHeaderSize+begin+3*keep+keep/2); err != nil {
		t.Fatal(err)
	}

	l, rep := mustOpenRaftLog(t, dir)
	if rep.SnapshotIndex != 0 || !rep.TornTail {
		t.Fatalf("a compaction that never finished: %+v", rep)
	}
	hard, snap, entries, err := l.Load()
	if err != nil {
		t.Fatal(err)
	}
	if snap.Index != 0 || hard.Term != 3 {
		t.Fatalf("snapshot %+v, hard state %+v", snap, hard)
	}
	wantEntries(t, entries, 1, 20)
	left, _ := os.ReadDir(snapSubdir(dir))
	if len(left) != 0 {
		t.Fatalf("%d orphaned snapshot files were left in place", len(left))
	}

	// The three staged entries are still on disk. A later compaction must
	// not pick them up, even if it too is interrupted before it can
	// delete the segments that hold them.
	mid := copyDir(t, dir)
	if err := l.SaveSnapshot(raft.Snapshot{Index: 20, Term: 3, Data: []byte("state@20")}, entries[18:]); err != nil {
		t.Fatal(err)
	}
	_ = l.Close()
	// Put back only what the second compaction deleted. The segment it
	// wrote into already existed, empty, when the copy was taken.
	restored := 0
	for _, name := range segmentNames(t, mid) {
		if _, err := os.Stat(filepath.Join(wal, name)); os.IsNotExist(err) {
			copyFile(t, filepath.Join(walSubdir(mid), name), filepath.Join(wal, name))
			restored++
		}
	}
	if restored != 2 {
		t.Fatalf("put back %d segments, want the old log and the abandoned compaction", restored)
	}
	_, snap, entries = loadAll(t, dir)
	if snap.Index != 20 {
		t.Fatalf("snapshot at %d, want 20", snap.Index)
	}
	wantEntries(t, entries, 19, 20)
}

func copyFile(t *testing.T, src, dst string) {
	t.Helper()
	data, err := os.ReadFile(src)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dst, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

// A log that starts after index 1 is useless without its snapshot. A node
// in that state must refuse to start: starting empty would let it vote as
// though it had never held the entries it acknowledged.
func TestRaftLogRefusesToOpenWithoutItsSnapshot(t *testing.T) {
	for _, damage := range []string{"missing", "flipped byte", "cut short"} {
		t.Run(damage, func(t *testing.T) {
			dir, _, _ := compacted(t)
			path := filepath.Join(snapSubdir(dir), snapshotFileName(15, 3))
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			switch damage {
			case "missing":
				err = os.Remove(path)
			case "flipped byte":
				raw[len(raw)-1] ^= 0xff
				err = os.WriteFile(path, raw, 0o600)
			case "cut short":
				err = os.WriteFile(path, raw[:len(raw)-2], 0o600)
			}
			if err != nil {
				t.Fatal(err)
			}
			l, _, err := OpenRaftLog(dir)
			if err == nil {
				_ = l.Close()
				t.Fatal("opened a log whose snapshot is unusable")
			}
			if !strings.Contains(err.Error(), "cannot start") {
				t.Fatalf("error does not say what it means for the node: %v", err)
			}
			// And it let go of the directory.
			if _, statErr := os.Stat(filepath.Join(dir, "jobq.lock")); !os.IsNotExist(statErr) {
				t.Fatal("the failed open left the directory locked")
			}
		})
	}
}

// A process that opens a log and dies before writing a record leaves a
// segment holding only its header. The next start must not trip over it.
func TestSegmentWriterReopensAnEmptySegment(t *testing.T) {
	dir := t.TempDir()
	for i := 0; i < 3; i++ {
		w, err := openSegmentWriter(dir, 1<<20)
		if err != nil {
			t.Fatalf("open %d: %v", i+1, err)
		}
		if i == 2 {
			if err := w.write(encodeFrame(walRecord{Type: recCleanShut})); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.closeFile(); err != nil {
			t.Fatal(err)
		}
	}
	segs, err := listSegments(dir)
	if err != nil || len(segs) != 1 {
		t.Fatalf("%d segments (%v), want the one reused", len(segs), err)
	}
	if rep, err := scanWAL(dir, func(walRecord) error { return nil }); err != nil || rep.records != 1 {
		t.Fatalf("scan found %d records (%v), want 1", rep.records, err)
	}
}

// The whole path: a node compacts, restarts from the snapshot, and carries
// on from the entries after it.
func TestRaftLogNodeRestartsFromItsSnapshot(t *testing.T) {
	dir := t.TempDir()
	start := func() (*raft.Node, *RaftLog) {
		l, _ := mustOpenRaftLog(t, dir)
		n, err := raft.New(raft.Config{ID: 1, Peers: []raft.NodeID{1}, ElectionTicks: 3, HeartbeatTicks: 1, Storage: l})
		if err != nil {
			t.Fatal(err)
		}
		return n, l
	}
	lead := func(n *raft.Node) {
		for i := 0; i < 10 && n.Status().State != raft.Leader; i++ {
			if err := n.Tick(); err != nil {
				t.Fatal(err)
			}
		}
		if n.Status().State != raft.Leader {
			t.Fatal("single node never elected itself")
		}
	}

	n, l := start()
	lead(n)
	for i := 0; i < 49; i++ {
		if _, _, err := n.Propose([]byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Sync(); err != nil {
		t.Fatal(err)
	}
	if got := len(n.Ready().Committed); got != 50 {
		t.Fatalf("applied %d entries, want 50", got)
	}
	if err := n.Compact(40, []byte("state@40"), 5); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	n, l = start()
	defer l.Close()
	rd := n.Ready()
	if rd.Snapshot == nil || rd.Snapshot.Index != 40 || string(rd.Snapshot.Data) != "state@40" {
		t.Fatalf("restart handed out snapshot %+v", rd.Snapshot)
	}
	if st := n.Status(); st.SnapshotIndex != 40 || st.FirstIndex != 36 || st.LastIndex != 50 {
		t.Fatalf("after restart: %+v", st)
	}
	lead(n)
	if err := l.Sync(); err != nil {
		t.Fatal(err)
	}
	rd = n.Ready()
	if len(rd.Committed) != 11 || rd.Committed[0].Index != 41 || rd.Committed[10].Index != 51 {
		t.Fatalf("after restart applied %d entries, want 41..51", len(rd.Committed))
	}
}

// A process killed between creating a segment file and syncing its header
// leaves an empty or short file. It must be treated as a torn tail, not as
// corruption: the cluster crash harness found a node that would not start
// again after exactly this.
func TestHalfCreatedSegmentIsATornTail(t *testing.T) {
	for _, size := range []int{0, 7, segmentHeaderSize} {
		dir := t.TempDir()
		w, err := openSegmentWriter(dir, 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		if err := w.write(encodeFrame(walRecord{Type: recCleanShut})); err != nil {
			t.Fatal(err)
		}
		if err := w.closeFile(); err != nil {
			t.Fatal(err)
		}
		// Where the next segment would have gone, with size bytes of it
		// written. A full header's worth of zeroes fails its checksum.
		torn := filepath.Join(dir, fmt.Sprintf("%016x.seg", w.offset))
		if err := os.WriteFile(torn, make([]byte, size), 0o600); err != nil {
			t.Fatal(err)
		}

		segs, err := listSegments(dir)
		if err != nil || len(segs) != 1 {
			t.Fatalf("%d-byte file: %d segments, %v; want the one good segment", size, len(segs), err)
		}
		if _, err := os.Stat(torn); !os.IsNotExist(err) {
			t.Fatalf("%d-byte file was left in place", size)
		}
		// And the next writer can make that segment for real.
		w, err = openSegmentWriter(dir, 1<<20)
		if err != nil {
			t.Fatalf("%d-byte file: reopening: %v", size, err)
		}
		_ = w.closeFile()
	}
}

// Anywhere but the tail, a file without a header is not a torn write.
func TestHeaderlessSegmentInTheMiddleIsCorruption(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("%016x.seg", 0)), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	later := &segmentWriter{dir: dir, maxBytes: 1 << 20, offset: 100, seq: 1}
	if err := later.rotate(); err != nil {
		t.Fatal(err)
	}
	_ = later.closeFile()
	if _, err := listSegments(dir); err == nil {
		t.Fatal("an empty segment file before a good one was accepted")
	}
}
