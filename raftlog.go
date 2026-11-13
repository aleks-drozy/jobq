package jobq

// A Raft log stored on the same segmented, CRC-framed write-ahead log the
// single-node queue uses. A replicated node keeps its queue state in
// memory and rebuilds it from a snapshot plus the committed log entries
// after it, so the Raft log and its snapshot are the only things that node
// writes to disk.
//
// The log is append-only, so nothing is ever rewritten in place: a
// truncation and a change of term or vote are records like any other, and
// replay applies them in order. That keeps the one property recovery
// depends on, that a torn write can only ever be at the tail.
//
// Compaction keeps that property too. The snapshot goes into its own file
// first. Then the entries worth keeping are written again at the start of
// a fresh segment, followed by one record that swaps them in for the whole
// log. Only once that is on disk are the older segments deleted, newest
// first, so a crash at any point leaves either the old log or the new one,
// and what remains of a half-finished delete is still a prefix that the
// swap record supersedes.

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"

	"github.com/aleks-drozy/jobq/raft"
)

// RaftLogReport says what opening a Raft log found on disk.
type RaftLogReport struct {
	Segments       int
	Records        int
	TornTail       bool
	BytesTruncated int64
	Entries        int    // log entries that survive replay
	Term           uint64 // the stored current term
	// SnapshotIndex is the log index the stored snapshot covers, or zero.
	SnapshotIndex uint64
	SnapshotBytes int
}

// RaftLog is a raft.Storage backed by a directory on disk.
//
// It implements raft.Syncer: writes are handed to the group committer and
// are durable only once Sync returns. One Sync covers every write since
// the last one, which is how a batch of proposals shares a single fsync.
// Whoever drives the node must call Sync before acting on Ready.
type RaftLog struct {
	dir  string
	w    *wal
	hard raft.HardState

	snapIndex uint64
	snapTerm  uint64
	last      uint64 // index of the last entry, or snapIndex if none

	// Replay state, handed to Load once and then dropped.
	loaded   []raft.Entry
	snapData []byte
	staged   []raft.Entry // entries of a compaction not yet swapped in
	staging  bool
	loadOK   bool

	tail waiter // resolves when the newest write is durable; nil if clean
	err  error  // sticky
}

var (
	_ raft.Storage = (*RaftLog)(nil)
	_ raft.Syncer  = (*RaftLog)(nil)
)

// OpenRaftLog creates or recovers a Raft log rooted at dir. It takes the
// same directory lock as Open, so a directory has one owner at a time, and
// it refuses a directory that holds a single-node queue's log.
//
// WithSync, WithSyncEvery and WithSegmentSize apply. Anything weaker than
// SyncAlways breaks Raft's safety argument, which assumes a vote or an
// acknowledged entry survives a crash; use it only to measure.
func OpenRaftLog(dir string, opts ...Option) (*RaftLog, RaftLogReport, error) {
	var cfg Queue
	for _, opt := range opts {
		opt(&cfg)
	}
	if err := ensureWALDir(dir); err != nil {
		return nil, RaftLogReport{}, err
	}
	lock, err := acquireDirLock(dir)
	if err != nil {
		return nil, RaftLogReport{}, err
	}

	l := &RaftLog{dir: dir, loadOK: true}
	scan, err := scanWAL(walSubdir(dir), l.replay)
	l.staged, l.staging = nil, false // a compaction that never finished
	if err == nil && l.snapIndex > 0 {
		l.snapData, err = readSnapshotFile(dir, l.snapIndex, l.snapTerm)
	}
	rep := RaftLogReport{
		Segments:       scan.segments,
		Records:        scan.records,
		TornTail:       scan.torn,
		BytesTruncated: scan.truncated,
		Entries:        len(l.loaded),
		Term:           l.hard.Term,
		SnapshotIndex:  l.snapIndex,
		SnapshotBytes:  len(l.snapData),
	}
	if err != nil {
		_ = releaseDirLock(lock)
		return nil, rep, err
	}
	w, err := openWALWithLock(dir, cfg.wopt, lock)
	if err != nil {
		_ = releaseDirLock(lock)
		return nil, rep, err
	}
	l.w = w
	// Snapshot files a crash left behind: ones that were written but never
	// swapped in, and ones that were swapped out but never deleted.
	l.removeOtherSnapshots()
	return l, rep, nil
}

// replay applies one record from disk to the in-memory picture of the log.
func (l *RaftLog) replay(rec walRecord) error {
	if rec.Type != recRaftKeep && rec.Type != recRaftSnap {
		// Anything else means the compaction being staged, if there was
		// one, was abandoned: the process died before swapping it in.
		l.staged, l.staging = nil, false
	}
	switch rec.Type {
	case recRaftEntry:
		if want := l.last + 1; rec.Index != want {
			return fmt.Errorf("raft log entry has index %d, want %d", rec.Index, want)
		}
		l.loaded = append(l.loaded, raft.Entry{
			Index: rec.Index, Term: rec.Term, Kind: raft.EntryKind(rec.Kind), Data: rec.Payload,
		})
		l.last = rec.Index
	case recRaftTruncate:
		if rec.Index <= l.snapIndex || rec.Index > l.last {
			return fmt.Errorf("raft log truncates from %d but covers (%d, %d]", rec.Index, l.snapIndex, l.last)
		}
		l.loaded = l.loaded[:rec.Index-l.loaded[0].Index]
		l.last = rec.Index - 1
	case recRaftHard:
		l.hard = raft.HardState{Term: rec.Term, VotedFor: raft.NodeID(rec.Voted)}
	case recRaftBegin:
		l.staging = true
	case recRaftKeep:
		if !l.staging {
			return errors.New("raft log keeps an entry outside a compaction")
		}
		if n := len(l.staged); n > 0 && rec.Index != l.staged[n-1].Index+1 {
			return fmt.Errorf("raft log keeps index %d after %d", rec.Index, l.staged[n-1].Index)
		}
		l.staged = append(l.staged, raft.Entry{
			Index: rec.Index, Term: rec.Term, Kind: raft.EntryKind(rec.Kind), Data: rec.Payload,
		})
	case recRaftSnap:
		if !l.staging {
			return errors.New("raft log swaps in a compaction that was never begun")
		}
		if err := checkRaftTail(rec.Index, l.staged); err != nil {
			return err
		}
		l.snapIndex, l.snapTerm = rec.Index, rec.Term
		l.loaded, l.staged, l.staging = l.staged, nil, false
		l.last = rec.Index
		if n := len(l.loaded); n > 0 {
			l.last = l.loaded[n-1].Index
		}
	case recCleanShut:
		// Where it sits carries no meaning here: nothing is inferred from
		// how the last process ended.
	default:
		return errors.New("jobq: directory holds a single-node queue's log, not a replicated node's")
	}
	return nil
}

// checkRaftTail is the rule for the entries kept beside a snapshot: none,
// or a contiguous run that starts no later than one past the snapshot and
// reaches at least as far as it.
func checkRaftTail(snapIndex uint64, tail []raft.Entry) error {
	if len(tail) == 0 {
		return nil
	}
	for i, e := range tail {
		if e.Index != tail[0].Index+uint64(i) {
			return fmt.Errorf("raft log is not contiguous at index %d", e.Index)
		}
	}
	first, last := tail[0].Index, tail[len(tail)-1].Index
	if first == 0 || first > snapIndex+1 || last < snapIndex {
		return fmt.Errorf("raft log [%d, %d] does not meet its snapshot at %d", first, last, snapIndex)
	}
	return nil
}

// Load implements raft.Storage. It may be called once, before any write:
// the replayed entries and the snapshot are handed over, not kept, so they
// are held in memory once (by the node) rather than twice.
func (l *RaftLog) Load() (raft.HardState, raft.Snapshot, []raft.Entry, error) {
	if !l.loadOK {
		return raft.HardState{}, raft.Snapshot{}, nil, errors.New("jobq: RaftLog.Load called twice or after a write")
	}
	l.loadOK = false
	snap := raft.Snapshot{Index: l.snapIndex, Term: l.snapTerm, Data: l.snapData}
	entries := l.loaded
	l.loaded, l.snapData = nil, nil
	return l.hard, snap, entries, nil
}

// SaveHardState implements raft.Storage. Durable after the next Sync.
func (l *RaftLog) SaveHardState(h raft.HardState) error {
	if l.err != nil {
		return l.err
	}
	l.write(walRecord{Type: recRaftHard, Term: h.Term, Voted: uint64(h.VotedFor)})
	l.hard = h
	return nil
}

// Append implements raft.Storage. Durable after the next Sync.
func (l *RaftLog) Append(entries []raft.Entry) error {
	if l.err != nil {
		return l.err
	}
	for _, e := range entries {
		if e.Index != l.last+1 {
			return l.fail(fmt.Errorf("jobq: raft log append at index %d, want %d", e.Index, l.last+1))
		}
		l.write(walRecord{Type: recRaftEntry, Index: e.Index, Term: e.Term, Kind: uint8(e.Kind), Payload: e.Data})
		l.last++
	}
	return nil
}

// TruncateFrom implements raft.Storage. Durable after the next Sync.
func (l *RaftLog) TruncateFrom(index uint64) error {
	if l.err != nil {
		return l.err
	}
	if index <= l.snapIndex || index > l.last {
		return l.fail(fmt.Errorf("jobq: raft log truncate from %d, log covers (%d, %d]", index, l.snapIndex, l.last))
	}
	l.write(walRecord{Type: recRaftTruncate, Index: index})
	l.last = index - 1
	return nil
}

// SaveSnapshot implements raft.Storage: snap and tail replace everything
// stored, and the disk space the old log took is given back. Unlike the
// other writes it is durable when it returns, whatever the sync policy,
// because it ends by deleting the only other copy.
//
// It blocks for as long as writing and syncing the snapshot takes.
func (l *RaftLog) SaveSnapshot(snap raft.Snapshot, tail []raft.Entry) error {
	if l.err != nil {
		return l.err
	}
	if err := checkRaftTail(snap.Index, tail); err != nil {
		return l.fail(fmt.Errorf("jobq: %w", err))
	}
	if snap.Index < l.snapIndex {
		return l.fail(fmt.Errorf("jobq: snapshot at %d is older than the stored one at %d", snap.Index, l.snapIndex))
	}
	if err := l.Sync(); err != nil {
		return err
	}
	// 1. The snapshot, in a file of its own, durable before anything
	//    refers to it.
	if err := writeSnapshotFile(l.dir, snap); err != nil {
		return l.fail(fmt.Errorf("jobq: writing snapshot: %w", err))
	}
	// 2. A segment boundary, so that what follows shares no file with the
	//    log it replaces.
	base, err := l.w.cut()
	if err != nil {
		return l.fail(err)
	}
	// 3. The kept entries, staged; the swap; and the term and vote again,
	//    since their last record is about to be deleted with the old log.
	l.write(walRecord{Type: recRaftBegin})
	for _, e := range tail {
		l.write(walRecord{Type: recRaftKeep, Index: e.Index, Term: e.Term, Kind: uint8(e.Kind), Payload: e.Data})
	}
	l.write(walRecord{Type: recRaftSnap, Index: snap.Index, Term: snap.Term})
	l.write(walRecord{Type: recRaftHard, Term: l.hard.Term, Voted: uint64(l.hard.VotedFor)})
	if err := l.Sync(); err != nil {
		return err
	}
	// Under a weaker sync policy that Sync did not reach the disk. A second
	// boundary does: it syncs the segment just written.
	if l.w.opts.policy != SyncAlways {
		if _, err := l.w.cut(); err != nil {
			return l.fail(err)
		}
	}
	l.snapIndex, l.snapTerm = snap.Index, snap.Term
	l.last = snap.Index
	if n := len(tail); n > 0 {
		l.last = tail[n-1].Index
	}
	// 4. Only now is the old log redundant. Failing to delete it costs
	//    disk space, not correctness, and the next compaction tries again.
	l.removeSegmentsBefore(base)
	l.removeOtherSnapshots()
	return nil
}

// removeSegmentsBefore deletes every segment that starts before base,
// newest first, stopping at the first that will not go. Newest first is
// what makes a half-finished delete safe: what is left is a prefix of the
// old log, which replays cleanly and is then replaced by the swap record.
// Deleting oldest first could leave a middle with no beginning.
func (l *RaftLog) removeSegmentsBefore(base uint64) {
	segs, err := listSegments(walSubdir(l.dir))
	if err != nil {
		return
	}
	for i := len(segs) - 1; i >= 0; i-- {
		if segs[i].baseOffset >= base {
			continue
		}
		if err := os.Remove(segs[i].path); err != nil {
			return
		}
	}
}

func (l *RaftLog) removeOtherSnapshots() {
	entries, err := os.ReadDir(snapSubdir(l.dir))
	if err != nil {
		return
	}
	keep := ""
	if l.snapIndex > 0 {
		keep = snapshotFileName(l.snapIndex, l.snapTerm)
	}
	for _, e := range entries {
		if !e.IsDir() && e.Name() != keep {
			_ = os.Remove(filepath.Join(snapSubdir(l.dir), e.Name()))
		}
	}
}

// DiskUsage reports how many bytes the log's segments and its snapshot
// take on disk. Unlike the rest of RaftLog it may be called from any
// goroutine: it reads the directory, not the log's state.
func (l *RaftLog) DiskUsage() (logBytes, snapshotBytes int64) {
	return dirSize(walSubdir(l.dir)), dirSize(snapSubdir(l.dir))
}

func dirSize(dir string) int64 {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}
	var total int64
	for _, e := range entries {
		if info, err := e.Info(); err == nil && !e.IsDir() {
			total += info.Size()
		}
	}
	return total
}

// write hands one record to the committer and remembers only the newest
// waiter: the fsync that covers the newest record covers all before it.
func (l *RaftLog) write(rec walRecord) {
	l.loadOK = false
	l.tail = l.w.append(rec, true)
}

// Sync implements raft.Syncer: it returns once everything written so far
// is on disk. A failure is permanent; the log refuses all further use.
func (l *RaftLog) Sync() error {
	if l.err != nil {
		return l.err
	}
	if l.tail == nil {
		return nil
	}
	err := <-l.tail
	l.tail = nil
	if err != nil {
		return l.fail(err)
	}
	return nil
}

func (l *RaftLog) fail(err error) error {
	if l.err == nil {
		l.err = err
	}
	return l.err
}

// Close flushes and closes the log and releases the directory.
func (l *RaftLog) Close() error {
	syncErr := l.Sync()
	if l.err == nil {
		l.err = ErrClosed
	}
	return errors.Join(syncErr, l.w.close())
}

// Snapshot files: snap/<index>-<term>.snap, one at a time. A 40-byte
// header (magic, index, term, length, CRC of the data, CRC of the header)
// and then the data. The file is written under a temporary name, synced
// and renamed, so a file with the final name is complete or, if the rename
// itself was torn, fails its checksum.

const (
	snapshotMagic      = "JOBQSNP\x01"
	snapshotHeaderSize = 40
)

func snapSubdir(dir string) string { return filepath.Join(dir, "snap") }

func snapshotFileName(index, term uint64) string {
	return fmt.Sprintf("%016x-%016x.snap", index, term)
}

func writeSnapshotFile(dir string, snap raft.Snapshot) error {
	sub := snapSubdir(dir)
	if err := os.MkdirAll(sub, 0o700); err != nil {
		return err
	}
	var hdr [snapshotHeaderSize]byte
	copy(hdr[0:8], snapshotMagic)
	binary.LittleEndian.PutUint64(hdr[8:16], snap.Index)
	binary.LittleEndian.PutUint64(hdr[16:24], snap.Term)
	binary.LittleEndian.PutUint64(hdr[24:32], uint64(len(snap.Data)))
	binary.LittleEndian.PutUint32(hdr[32:36], crc32.Checksum(snap.Data, castagnoli))
	binary.LittleEndian.PutUint32(hdr[36:40], crc32.Checksum(hdr[0:36], castagnoli))

	final := filepath.Join(sub, snapshotFileName(snap.Index, snap.Term))
	tmp := final + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	_, err = f.Write(hdr[:])
	if err == nil {
		_, err = f.Write(snap.Data)
	}
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(tmp, final)
	}
	if err != nil {
		_ = os.Remove(tmp)
		return err
	}
	// Make the rename itself durable where the platform allows it. Windows
	// cannot sync a directory; there the checksum is the only defence, and
	// a snapshot lost this way stops the node at its next start rather
	// than letting it run on a log with no beginning.
	if d, err := os.Open(sub); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}

func readSnapshotFile(dir string, index, term uint64) ([]byte, error) {
	path := filepath.Join(snapSubdir(dir), snapshotFileName(index, term))
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("jobq: the log starts after index %d but its snapshot cannot be read, so this node cannot start: %w", index, err)
	}
	bad := func(why string) ([]byte, error) {
		return nil, fmt.Errorf("jobq: snapshot %s is damaged (%s); the log before index %d is gone, so this node cannot start", path, why, index)
	}
	if len(raw) < snapshotHeaderSize || string(raw[0:8]) != snapshotMagic {
		return bad("bad header")
	}
	if crc32.Checksum(raw[0:36], castagnoli) != binary.LittleEndian.Uint32(raw[36:40]) {
		return bad("header checksum")
	}
	if binary.LittleEndian.Uint64(raw[8:16]) != index || binary.LittleEndian.Uint64(raw[16:24]) != term {
		return bad("it is for a different index")
	}
	data := raw[snapshotHeaderSize:]
	if uint64(len(data)) != binary.LittleEndian.Uint64(raw[24:32]) {
		return bad("wrong length")
	}
	if crc32.Checksum(data, castagnoli) != binary.LittleEndian.Uint32(raw[32:36]) {
		return bad("data checksum")
	}
	return data, nil
}
