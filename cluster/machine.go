package cluster

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aleks-drozy/jobq"
	"github.com/aleks-drozy/jobq/raft"
)

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
type machine struct {
	q *jobq.Queue

	clock atomic.Int64 // unix nanoseconds of the newest applied command

	idMu    sync.Mutex
	index   uint64 // entry being applied
	term    uint64
	counter uint32 // IDs minted so far for that entry

	applied atomic.Uint64
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
	m := &machine{}
	m.q = jobq.New(jobq.WithClock(m.now), jobq.WithIDSource(m.nextID), jobq.WithPassiveReads())
	return m
}

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

	if e.Kind == raft.EntryNoop {
		// A no-op marks a new leader's term. Every lease ends here, the
		// same rule a single node applies on restart. Consumers already
		// handle a lease timing out, so this adds no new behaviour for
		// them, and it means lease deadlines never have to be compared
		// across two leaders' clocks.
		m.q.ExpireLeases()
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
	case opEnqueue:
		var opts []jobq.EnqueueOption
		if c.Delay > 0 {
			opts = append(opts, jobq.WithDelay(c.Delay))
		}
		if c.MaxAttempts > 0 {
			opts = append(opts, jobq.WithMaxAttempts(c.MaxAttempts))
		}
		id, err := m.q.Enqueue(c.Topic, c.Payload, opts...)
		return result{id: id, err: err}
	case opDequeue:
		job, lease, err := m.q.Dequeue(c.Topic, c.Duration)
		return result{job: job, lease: lease, err: err}
	case opAck:
		return result{err: m.q.Ack(jobq.Lease{ID: c.LeaseID, Topic: c.Topic})}
	case opNack:
		return result{err: m.q.Nack(jobq.Lease{ID: c.LeaseID, Topic: c.Topic})}
	case opExtend:
		deadline, err := m.q.Extend(jobq.Lease{ID: c.LeaseID, Topic: c.Topic}, c.Duration)
		return result{deadline: deadline, err: err}
	}
	return result{err: errBadCommand}
}
