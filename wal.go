package jobq

// The write-ahead log: one per Queue, shared by every topic actor, flushed
// by a single committer goroutine doing group commit. Actors encode records
// and hand them over under a short mutex; while one batch is inside fsync,
// everything that arrives accumulates into the next batch, so the fsync
// cost divides across concurrent producers instead of multiplying.
//
// Only durability-gated appends (enqueues under SyncAlways) wait for the
// fsync that covers their record; everything else returns as soon as the
// record is in the batch. A write or sync failure fails the WAL closed:
// every waiter gets the error and all subsequent appends refuse. A queue
// that cannot persist must say so, not limp.

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// SyncPolicy controls when the WAL calls fsync.
type SyncPolicy int

const (
	// SyncAlways gates every acknowledged enqueue on the group fsync that
	// covers its record. A crash loses nothing that was acknowledged.
	SyncAlways SyncPolicy = iota
	// SyncInterval fsyncs on a timer (default 5ms). A crash loses at most
	// that much acknowledged work — stated plainly, chosen knowingly.
	SyncInterval
	// SyncNever leaves flushing to the OS page cache. A crash loses an
	// unbounded suffix of acknowledged work. For benchmarks and for callers
	// who genuinely do not care.
	SyncNever
)

const defaultSyncInterval = 5 * time.Millisecond

type walOptions struct {
	policy       SyncPolicy
	interval     time.Duration
	segmentBytes int64
}

type waiter chan error

type wal struct {
	dir  string
	opts walOptions
	lock *os.File // held for the process lifetime; see acquireDirLock

	mu      sync.Mutex
	pending []byte   // encoded frames awaiting write
	waiters []waiter // released after the covering fsync (SyncAlways)
	failed  error    // sticky first error; nil while healthy
	closed  bool
	kick    chan struct{} // wakes the committer; capacity 1

	seg *segmentWriter

	committerDone chan struct{}
}

func walSubdir(dir string) string { return filepath.Join(dir, "wal") }

func ensureWALDir(dir string) error { return os.MkdirAll(walSubdir(dir), 0o755) }

func openWAL(dir string, opts walOptions) (*wal, error) {
	if err := ensureWALDir(dir); err != nil {
		return nil, err
	}
	lock, err := acquireDirLock(dir)
	if err != nil {
		return nil, err
	}
	w, err := openWALWithLock(dir, opts, lock)
	if err != nil {
		_ = releaseDirLock(lock)
		return nil, err
	}
	return w, nil
}

// openWALWithLock builds the WAL for a directory whose lock the caller
// already holds (Open replays before the writer starts; both need the lock).
func openWALWithLock(dir string, opts walOptions, lock *os.File) (*wal, error) {
	if opts.interval <= 0 {
		opts.interval = defaultSyncInterval
	}
	if opts.segmentBytes <= 0 {
		opts.segmentBytes = defaultSegmentBytes
	}
	seg, err := openSegmentWriter(walSubdir(dir), opts.segmentBytes)
	if err != nil {
		return nil, err
	}
	w := &wal{
		dir:           dir,
		opts:          opts,
		lock:          lock,
		kick:          make(chan struct{}, 1),
		seg:           seg,
		committerDone: make(chan struct{}),
	}
	go w.committer()
	return w, nil
}

// append hands one record to the WAL. When gated is true and the policy is
// SyncAlways, the returned channel yields after the record is fsynced;
// otherwise it yields as soon as the record is batched. The channel always
// yields exactly once.
func (w *wal) append(rec walRecord, gated bool) waiter {
	done := make(waiter, 1)
	frame := encodeFrame(rec)

	w.mu.Lock()
	switch {
	case w.failed != nil:
		err := w.failed
		w.mu.Unlock()
		done <- err
		return done
	case w.closed:
		w.mu.Unlock()
		done <- ErrClosed
		return done
	}
	w.pending = append(w.pending, frame...)
	if gated && w.opts.policy == SyncAlways {
		w.waiters = append(w.waiters, done)
	} else {
		done <- nil
	}
	w.mu.Unlock()

	select {
	case w.kick <- struct{}{}:
	default: // committer already signalled
	}
	return done
}

// committer is the only goroutine that touches the segment files.
func (w *wal) committer() {
	defer close(w.committerDone)
	var ticker *time.Ticker
	var tick <-chan time.Time
	if w.opts.policy == SyncInterval {
		ticker = time.NewTicker(w.opts.interval)
		tick = ticker.C
		defer ticker.Stop()
	}
	dirty := false // bytes written since the last fsync

	for {
		select {
		case <-w.kick:
			batch, waiters, closing := w.takeBatch()
			if len(batch) > 0 {
				err := w.seg.write(batch)
				if err == nil && w.opts.policy == SyncAlways {
					err = w.seg.sync()
				} else if err == nil {
					dirty = true
				}
				if err != nil {
					w.fail(err, waiters)
					continue
				}
			}
			for _, done := range waiters {
				done <- nil
			}
			if closing {
				return
			}
		case <-tick:
			if dirty {
				if err := w.seg.sync(); err != nil {
					w.fail(err, nil)
					continue
				}
				dirty = false
			}
		}
	}
}

// takeBatch swaps out everything pending. closing reports that close() has
// been requested and this batch is the last.
func (w *wal) takeBatch() (batch []byte, waiters []waiter, closing bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	batch, w.pending = w.pending, nil
	waiters, w.waiters = w.waiters, nil
	return batch, waiters, w.closed
}

// fail marks the WAL broken and releases every waiter with the error.
func (w *wal) fail(err error, waiters []waiter) {
	w.mu.Lock()
	if w.failed == nil {
		w.failed = fmt.Errorf("jobq: wal failed: %w", err)
	}
	stored := w.failed
	extra := w.waiters
	w.waiters = nil
	w.mu.Unlock()
	for _, done := range waiters {
		done <- stored
	}
	for _, done := range extra {
		done <- stored
	}
}

// close writes CLEANSHUT, syncs, and releases the directory lock. Safe to
// call once; the Queue guarantees that.
func (w *wal) close() error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		<-w.committerDone
		return w.failed
	}
	w.closed = true
	w.pending = append(w.pending, encodeFrame(walRecord{Type: recCleanShut})...)
	w.mu.Unlock()

	select {
	case w.kick <- struct{}{}:
	default:
	}
	<-w.committerDone

	var errs []error
	if w.failed == nil {
		if err := w.seg.sync(); err != nil {
			errs = append(errs, err)
		}
	}
	if err := w.seg.closeFile(); err != nil {
		errs = append(errs, err)
	}
	if err := releaseDirLock(w.lock); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// encodeFrame is writeRecord into memory; record encoding cannot fail for
// records the engine itself produces, so an error here is a programmer
// mistake and panics loudly rather than corrupting the log by omission.
func encodeFrame(rec walRecord) []byte {
	var buf frameBuffer
	if err := writeRecord(&buf, rec); err != nil {
		panic("jobq: unencodable wal record: " + err.Error())
	}
	return buf
}

type frameBuffer []byte

func (b *frameBuffer) Write(p []byte) (int, error) {
	*b = append(*b, p...)
	return len(p), nil
}

// acquireDirLock takes single-process ownership of dir via O_CREATE|O_EXCL
// (flock does not exist on Windows). The file holds the PID for operators.
// A crash leaves the file behind; openWAL treats a lock file whose process
// is provably gone the same as no lock — but proving "gone" portably is
// v2 territory, so for now a stale lock is removed only by hand, and the
// error says exactly that.
func acquireDirLock(dir string) (*os.File, error) {
	path := filepath.Join(dir, "jobq.lock")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		if os.IsExist(err) {
			return nil, fmt.Errorf("jobq: %s is locked by another process "+
				"(if that process crashed, remove the file by hand): %w", path, err)
		}
		return nil, err
	}
	fmt.Fprintf(f, "%d\n", os.Getpid())
	return f, nil
}

func releaseDirLock(f *os.File) error {
	path := f.Name()
	if err := f.Close(); err != nil {
		return err
	}
	return os.Remove(path)
}
