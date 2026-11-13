package cluster

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aleks-drozy/jobq"
	"github.com/aleks-drozy/jobq/raft"
)

// DedupWindow is how long a replicated queue remembers an enqueue's
// request ID, measured on the queue's own clock (the leaders' stamps). An
// enqueue that arrives again inside the window adds nothing and returns
// the first one's job ID.
//
// It is a constant because every replica must forget an ID at the same
// log entry. A client's retries span seconds; the window only has to
// outlast them.
const DedupWindow = 5 * time.Minute

// dedupMax bounds the table whatever the enqueue rate. Past it the oldest
// IDs are forgotten early, which weakens the window; it does not break
// agreement, since every replica drops the same ones.
const dedupMax = 1 << 20

// machine is the replicated state machine: an ordinary in-memory
// jobq.Queue, driven only by committed log entries.
//
// Replicas stay identical because nothing the queue does while applying an
// entry depends on the replica it runs on:
//
//   - its clock is the timestamp the leader wrote into the entry, never
//     the local wall clock, and never moves backwards;
//   - its job and lease IDs come from the entry's position in the log,
//     not from crypto/rand;
//   - reads (Stats, Peek) observe nothing, so a replica that is asked
//     questions does not drift from one that is not;
//   - entries are applied one at a time, each to completion.
//
// A snapshot is all of it at one log index, and a machine loaded from one
// carries on exactly as the machine that took it would have.
type machine struct {
	// q is swapped whole when a snapshot is loaded, while readers on other
	// goroutines may be using it.
	q atomic.Pointer[jobq.Queue]

	clock atomic.Int64 // unix nanoseconds of the newest applied command

	idMu    sync.Mutex
	index   uint64 // entry being applied
	term    uint64
	counter uint32 // IDs minted so far for that entry

	// Recent request IDs. Touched only by the applying goroutine.
	seen  map[string]string // request ID -> job ID
	order []dedupEntry      // the same, oldest first

	applied atomic.Uint64
	// deduped counts enqueues answered from the table. Unlike everything
	// above it is not replicated state: it restarts from zero.
	deduped atomic.Uint64
}

type dedupEntry struct {
	requestID string
	jobID     string
	at        int64 // the queue's clock when it was recorded
}

// result is what one applied command produced. Which fields are set
// depends on the operation.
type result struct {
	id       string
	job      *jobq.Job
	lease    jobq.Lease
	deadline time.Time
	err      error
}

func newMachine() *machine {
	m := &machine{seen: map[string]string{}}
	m.q.Store(jobq.New(m.options()...))
	return m
}

func (m *machine) options() []jobq.Option {
	return []jobq.Option{jobq.WithClock(m.now), jobq.WithIDSource(m.nextID), jobq.WithPassiveReads()}
}

func (m *machine) queue() *jobq.Queue { return m.q.Load() }

func (m *machine) now() time.Time { return time.Unix(0, m.clock.Load()).UTC() }

// nextID derives an ID from the log position: index, term, and how many
// IDs this entry has already minted. A log index is applied once, so the
// result is unique, and every replica computes the same one.
func (m *machine) nextID() string {
	m.idMu.Lock()
	defer m.idMu.Unlock()
	id := fmt.Sprintf("%016x%08x%08x", m.index, uint32(m.term), m.counter)
	m.counter++
	return id
}

// apply runs one committed entry. It is called from one goroutine, in log
// order, with no gaps.
func (m *machine) apply(e raft.Entry) result {
	m.idMu.Lock()
	m.index, m.term, m.counter = e.Index, e.Term, 0
	m.idMu.Unlock()
	defer m.applied.Store(e.Index)
	q := m.queue()

	if e.Kind == raft.EntryNoop {
		// A no-op marks a new leader's term. Every lease ends here, the
		// same rule a single node applies on restart. Consumers already
		// handle a lease timing out, so this adds no new behaviour for
		// them, and it means lease deadlines never have to be compared
		// across two leaders' clocks.
		q.ExpireLeases()
		return result{}
	}

	c, err := decodeCommand(e.Data)
	if err != nil {
		// Every replica reaches the same verdict on the same bytes, so
		// skipping is as deterministic as applying.
		return result{err: fmt.Errorf("cluster: entry %d: %w", e.Index, err)}
	}
	if at := c.At.UnixNano(); at > m.clock.Load() {
		m.clock.Store(at)
	}

	switch c.Op {
	case opEnqueue, opEnqueueID:
		m.forgetOld()
		if c.RequestID != "" {
			if id, ok := m.seen[c.RequestID]; ok {
				// This enqueue has already happened. Say so with the
				// same answer the first one got.
				m.deduped.Add(1)
				return result{id: id}
			}
		}
		var opts []jobq.EnqueueOption
		if c.Delay > 0 {
			opts = append(opts, jobq.WithDelay(c.Delay))
		}
		if c.MaxAttempts > 0 {
			opts = append(opts, jobq.WithMaxAttempts(c.MaxAttempts))
		}
		id, err := q.Enqueue(c.Topic, c.Payload, opts...)
		if err == nil && c.RequestID != "" {
			m.seen[c.RequestID] = id
			m.order = append(m.order, dedupEntry{requestID: c.RequestID, jobID: id, at: m.clock.Load()})
		}
		return result{id: id, err: err}
	case opDequeue:
		job, lease, err := q.Dequeue(c.Topic, c.Duration)
		return result{job: job, lease: lease, err: err}
	case opAck:
		return result{err: q.Ack(jobq.Lease{ID: c.LeaseID, Topic: c.Topic})}
	case opNack:
		return result{err: q.Nack(jobq.Lease{ID: c.LeaseID, Topic: c.Topic})}
	case opExtend:
		deadline, err := q.Extend(jobq.Lease{ID: c.LeaseID, Topic: c.Topic}, c.Duration)
		return result{deadline: deadline, err: err}
	}
	return result{err: errBadCommand}
}

// forgetOld drops request IDs older than the window, and the oldest beyond
// the size bound. It runs at every enqueue, on the queue's clock, so every
// replica forgets the same IDs at the same entry.
func (m *machine) forgetOld() {
	horizon := m.clock.Load() - int64(DedupWindow)
	drop := 0
	for drop < len(m.order) && (m.order[drop].at < horizon || len(m.order)-drop >= dedupMax) {
		delete(m.seen, m.order[drop].requestID)
		drop++
	}
	if drop > 0 {
		m.order = append(m.order[:0:0], m.order[drop:]...)
	}
}

// snapshot encodes the machine as it stands. It must be called from the
// applying goroutine, between entries.
func (m *machine) snapshot() []byte {
	return state{clock: m.clock.Load(), dedup: m.order, queue: m.queue().State()}.encode()
}

// restore replaces the machine with the one a snapshot describes, taken at
// log index. Readers holding the old queue see it closed.
// checkSnapshot reports whether restore could load data, without loading
// it.
func checkSnapshot(data []byte) error {
	st, err := decodeState(data)
	if err != nil {
		return err
	}
	return st.queue.Validate()
}

func (m *machine) restore(data []byte, index uint64) error {
	st, err := decodeState(data)
	if err != nil {
		return err
	}
	q, err := jobq.NewFromState(st.queue, m.options()...)
	if err != nil {
		return fmt.Errorf("cluster: snapshot at index %d: %w", index, err)
	}
	old := m.q.Swap(q)
	m.clock.Store(st.clock)
	m.order = st.dedup
	m.seen = make(map[string]string, len(st.dedup))
	for _, d := range st.dedup {
		m.seen[d.requestID] = d.jobID
	}
	m.applied.Store(index)
	return old.Close()
}
