package raft

import "fmt"

// HardState is the part of a node's state that must survive a crash: the
// latest term it has seen and who it voted for in that term. Losing either
// lets a node vote twice in one term, which is how two leaders get elected.
type HardState struct {
	Term     uint64
	VotedFor NodeID
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
	// Load returns the state to restart from. The log must be contiguous
	// and start at index 1.
	Load() (HardState, []Entry, error)
	SaveHardState(HardState) error
	// Append adds entries after the current last entry.
	Append([]Entry) error
	// TruncateFrom removes the entry at index and everything after it.
	TruncateFrom(index uint64) error
}

// Syncer is implemented by a Storage that buffers its writes so that many
// of them can share one fsync. The node never performs I/O and cannot tell
// the difference, so the durability promise moves to whoever drives it:
// after any Tick, Step or Propose, the driver must call Sync and see it
// succeed before it sends a message or applies an entry from Ready. A
// crash before Sync then loses only writes nobody outside the process ever
// heard about.
type Syncer interface {
	Sync() error
}

// MemoryStorage is a Storage that lives in memory. It survives a Node being
// discarded and rebuilt, which is exactly what a crash looks like to the
// algorithm, so it is what the simulator restarts nodes from.
type MemoryStorage struct {
	hard    HardState
	entries []Entry
}

// NewMemoryStorage returns an empty MemoryStorage.
func NewMemoryStorage() *MemoryStorage { return &MemoryStorage{} }

func (s *MemoryStorage) Load() (HardState, []Entry, error) {
	return s.hard, append([]Entry(nil), s.entries...), nil
}

func (s *MemoryStorage) SaveHardState(h HardState) error {
	s.hard = h
	return nil
}

func (s *MemoryStorage) Append(entries []Entry) error {
	for _, e := range entries {
		if want := uint64(len(s.entries)) + 1; e.Index != want {
			return fmt.Errorf("raft: append index %d, want %d", e.Index, want)
		}
		s.entries = append(s.entries, e)
	}
	return nil
}

func (s *MemoryStorage) TruncateFrom(index uint64) error {
	if index == 0 || index > uint64(len(s.entries)) {
		return fmt.Errorf("raft: truncate from %d, log has %d entries", index, len(s.entries))
	}
	s.entries = s.entries[:index-1]
	return nil
}
