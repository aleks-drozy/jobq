package jobq

import (
	"fmt"
	"strings"
	"time"
)

// QueueState is everything a Queue holds, in a form that does not depend
// on how the queue got there: topics in name order, ready jobs in delivery
// order, leases in expiry order. Two queues that would behave identically
// from here on have equal states.
//
// It exists for replication. A replicated node snapshots its queue with
// State so that it can throw away the log that built it, and a node that
// starts from a snapshot rebuilds the queue with NewFromState.
type QueueState struct {
	Topics []TopicState
}

// TopicState is one topic's jobs and counters.
type TopicState struct {
	Name string
	// Ready is the ready list in the order Dequeue would serve it, delayed
	// jobs included.
	Ready []*Job
	// Leased is every outstanding lease, soonest deadline first.
	Leased []LeasedJob
	// Stats carries the cumulative counters. Its Ready and InFlight are
	// redundant with the two lists and are ignored by NewFromState.
	Stats Stats
}

// LeasedJob is a job out on a lease.
type LeasedJob struct {
	Job      *Job
	LeaseID  string
	Deadline time.Time
}

// State copies out everything the queue holds. Unlike Stats and Peek it
// never observes lease expiry, whatever the queue's options: a lease past
// its deadline is reported as a lease.
//
// Each topic is copied atomically, but the topics are visited one after
// another. The result is a consistent picture of the whole queue only if
// nothing changes it meanwhile, which is the caller's business; a
// replicated node calls it between applying one log entry and the next.
func (q *Queue) State() QueueState {
	q.mu.RLock()
	names := make([]string, 0, len(q.topics))
	for name := range q.topics {
		names = append(names, name)
	}
	sortStrings(names)
	tps := make([]*topic, 0, len(names))
	for _, name := range names {
		tps = append(tps, q.topics[name])
	}
	q.mu.RUnlock()

	st := QueueState{Topics: make([]TopicState, 0, len(tps))}
	for _, tp := range tps {
		if ts, ok := tp.state(); ok {
			st.Topics = append(st.Topics, ts)
		}
	}
	return st
}

// NewFromState creates an in-memory Queue holding exactly st. Leases come
// back live, with their IDs and deadlines, so a consumer holding one can
// still settle it.
func NewFromState(st QueueState, opts ...Option) (*Queue, error) {
	if err := st.Validate(); err != nil {
		return nil, err
	}
	q := New(opts...)
	q.mu.Lock()
	defer q.mu.Unlock()
	for i := range st.Topics {
		if _, err := q.startTopicLocked(st.Topics[i].Name, nil, &st.Topics[i]); err != nil {
			return nil, closeAfter(q, err)
		}
	}
	return q, nil
}

// Validate reports whether st is something NewFromState can build a queue
// from. It starts nothing, so it is the check to make on a state that
// came from somewhere else before committing to it.
func (st QueueState) Validate() error {
	topics := map[string]bool{}
	for i := range st.Topics {
		ts := &st.Topics[i]
		if strings.TrimSpace(ts.Name) == "" {
			return ErrEmptyTopic
		}
		if topics[ts.Name] {
			return fmt.Errorf("jobq: state lists topic %q twice", ts.Name)
		}
		topics[ts.Name] = true
		for _, job := range ts.Ready {
			if job == nil {
				return fmt.Errorf("jobq: state for topic %q has a missing job", ts.Name)
			}
		}
		seen := map[string]bool{}
		for _, l := range ts.Leased {
			if l.Job == nil || l.LeaseID == "" || seen[l.LeaseID] {
				return fmt.Errorf("jobq: state for topic %q has a bad lease", ts.Name)
			}
			seen[l.LeaseID] = true
		}
	}
	return nil
}

// closeAfter stops the topics a failed NewFromState already started. The
// caller holds q.mu.
func closeAfter(q *Queue, err error) error {
	q.closed = true
	for _, tp := range q.topics {
		tp.close()
	}
	return err
}
