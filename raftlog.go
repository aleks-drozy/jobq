package jobq

// A Raft log stored on the same segmented, CRC-framed write-ahead log the
// single-node queue uses. A replicated node keeps its queue state in
// memory and rebuilds it by replaying committed log entries, so the Raft
// log is the only thing that node writes to disk.
//
// The log is append-only, so nothing is ever rewritten in place: a
// truncation and a change of term or vote are records like any other, and
// replay applies them in order. That keeps the one property recovery
// depends on, that a torn write can only ever be at the tail.

import (
	"errors"
	"fmt"

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
}

// RaftLog is a raft.Storage backed by a directory on disk.
//
// It implements raft.Syncer: writes are handed to the group committer and
// are durable only once Sync returns. One Sync covers every write since
// the last one, which is how a batch of proposals shares a single fsync.
// Whoever drives the node must call Sync before acting on Ready.
type RaftLog struct {
	w      *wal
	hard   raft.HardState
	loaded []raft.Entry // replayed entries, handed to Load once
	loadOK bool
	last   uint64 // index of the last entry
	tail   waiter // resolves when the newest write is durable; nil if clean
	err    error  // sticky
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

	l := &RaftLog{loadOK: true}
	scan, err := scanWAL(walSubdir(dir), l.replay)
	rep := RaftLogReport{
		Segments:       scan.segments,
		Records:        scan.records,
		TornTail:       scan.torn,
		BytesTruncated: scan.truncated,
		Entries:        len(l.loaded),
		Term:           l.hard.Term,
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
	l.last = uint64(len(l.loaded))
	return l, rep, nil
}

// replay applies one record from disk to the in-memory picture of the log.
func (l *RaftLog) replay(rec walRecord) error {
	switch rec.Type {
	case recRaftEntry:
		if want := uint64(len(l.loaded)) + 1; rec.Index != want {
			return fmt.Errorf("raft log entry has index %d, want %d", rec.Index, want)
		}
		l.loaded = append(l.loaded, raft.Entry{
			Index: rec.Index, Term: rec.Term, Kind: raft.EntryKind(rec.Kind), Data: rec.Payload,
		})
	case recRaftTruncate:
		if rec.Index == 0 || rec.Index > uint64(len(l.loaded)) {
			return fmt.Errorf("raft log truncates from %d but holds %d entries", rec.Index, len(l.loaded))
		}
		l.loaded = l.loaded[:rec.Index-1]
	case recRaftHard:
		l.hard = raft.HardState{Term: rec.Term, VotedFor: raft.NodeID(rec.Voted)}
	case recCleanShut:
		// Where it sits carries no meaning here: nothing is inferred from
		// how the last process ended.
	default:
		return errors.New("jobq: directory holds a single-node queue's log, not a replicated node's")
	}
	return nil
}

// Load implements raft.Storage. It may be called once, before any write:
// the replayed entries are handed over, not kept, so the log is held in
// memory once (by the node) rather than twice.
func (l *RaftLog) Load() (raft.HardState, []raft.Entry, error) {
	if !l.loadOK {
		return raft.HardState{}, nil, errors.New("jobq: RaftLog.Load called twice or after a write")
	}
	l.loadOK = false
	entries := l.loaded
	l.loaded = nil
	return l.hard, entries, nil
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
	if index == 0 || index > l.last {
		return l.fail(fmt.Errorf("jobq: raft log truncate from %d, log has %d entries", index, l.last))
	}
	l.write(walRecord{Type: recRaftTruncate, Index: index})
	l.last = index - 1
	return nil
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
