package raft

import "fmt"

// HardState is the part of a node's state that must survive a crash: the
// latest term it has seen and who it voted for in that term. Losing either
// lets a node vote twice in one term, which is how two leaders get elected.
type HardState struct {
	Term     uint64
	VotedFor NodeID
}

// Snapshot stands in for a prefix of the log: Data is the state machine
// after applying every entry up to and including Index, and Term is the
// term of the entry at Index. The zero Snapshot means "no snapshot": the
// log starts at index 1. Data is shared, not copied: treat it as read-only.
type Snapshot struct {
	Index uint64
	Term  uint64
	Data  []byte
}

// Storage is the durable half of a node. Every method must be durable
// before it returns: the node sends messages that promise the write
// happened (a granted vote, an acknowledged append) as soon as the call
// comes back.
//
// A Storage is owned by exactly one Node and is not called concurrently.
//
// A Storage that also implements Syncer loosens "before it returns" to
// "before Sync returns"; see Syncer for what that asks of the driver.
type Storage interface {
	// Load returns the state to restart from: the snapshot, if there is
	// one, and the log entries kept alongside it. The entries must be
	// contiguous and must not leave a gap after the snapshot: the first is
	// at most one past the snapshot's index.
	Load() (HardState, Snapshot, []Entry, error)
	SaveHardState(HardState) error
	// Append adds entries after the current last entry.
	Append([]Entry) error
	// TruncateFrom removes the entry at index and everything after it.
	TruncateFrom(index uint64) error
	// SaveSnapshot replaces the stored snapshot and the whole stored log
	// with snap and tail. tail is every entry the node still keeps: empty,
	// or a contiguous run that starts no later than one past snap.Index
	// and reaches at least snap.Index. The term and vote are untouched.
	//
	// The replacement must be atomic across a crash: afterwards Load
	// returns either what it returned before or snap and tail.
	SaveSnapshot(snap Snapshot, tail []Entry) error
}

// Syncer is implemented by a Storage that buffers its writes so that many
// of them can share one fsync. The node never performs I/O and cannot tell
// the difference, so the durability promise moves to whoever drives it:
// after any Tick, Step, Propose or Compact, the driver must call Sync and
// see it succeed before it sends a message or applies an entry from Ready.
// A crash before Sync then loses only writes nobody outside the process
// ever heard about.
type Syncer interface {
	Sync() error
}

// MemoryStorage is a Storage that lives in memory. It survives a Node being
// discarded and rebuilt, which is exactly what a crash looks like to the
// algorithm, so it is what the simulator restarts nodes from.
type MemoryStorage struct {
	hard    HardState
	snap    Snapshot
	entries []Entry // contiguous; entries[0].Index <= snap.Index+1
}

// NewMemoryStorage returns an empty MemoryStorage.
func NewMemoryStorage() *MemoryStorage { return &MemoryStorage{} }

func (s *MemoryStorage) Load() (HardState, Snapshot, []Entry, error) {
	return s.hard, s.snap, append([]Entry(nil), s.entries...), nil
}

func (s *MemoryStorage) SaveHardState(h HardState) error {
	s.hard = h
	return nil
}

func (s *MemoryStorage) lastIndex() uint64 {
	if n := len(s.entries); n > 0 {
		return s.entries[n-1].Index
	}
	return s.snap.Index
}

func (s *MemoryStorage) Append(entries []Entry) error {
	for _, e := range entries {
		if want := s.lastIndex() + 1; e.Index != want {
			return fmt.Errorf("raft: append index %d, want %d", e.Index, want)
		}
		s.entries = append(s.entries, e)
	}
	return nil
}

func (s *MemoryStorage) TruncateFrom(index uint64) error {
	if index <= s.snap.Index || index > s.lastIndex() {
		return fmt.Errorf("raft: truncate from %d, log covers (%d, %d]", index, s.snap.Index, s.lastIndex())
	}
	s.entries = s.entries[:index-s.entries[0].Index]
	return nil
}

func (s *MemoryStorage) SaveSnapshot(snap Snapshot, tail []Entry) error {
	if err := checkTail(snap, tail); err != nil {
		return err
	}
	s.snap = snap
	s.entries = append([]Entry(nil), tail...)
	return nil
}

// checkTail enforces what Storage.SaveSnapshot and Load say about the
// entries kept beside a snapshot.
func checkTail(snap Snapshot, tail []Entry) error {
	if len(tail) == 0 {
		return nil
	}
	for i, e := range tail {
		if e.Index != tail[0].Index+uint64(i) {
			return fmt.Errorf("raft: log is not contiguous at index %d", e.Index)
		}
	}
	first, last := tail[0].Index, tail[len(tail)-1].Index
	if first == 0 || first > snap.Index+1 || last < snap.Index {
		return fmt.Errorf("raft: log [%d, %d] does not meet snapshot at %d", first, last, snap.Index)
	}
	return nil
}
