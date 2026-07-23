package jobq

// Replay: rebuild queue state from the WAL alone.
//
// Restart semantics (spec): leases are not persisted; a restart is the
// simultaneous expiry of every outstanding lease. A DELIVER with no later
// settle therefore returns to ready with its attempt count — or, if its
// budget is exhausted, dead-letters during replay. That inference is NOT
// re-logged: replay is deterministic, so every future recovery reaches the
// same conclusion from the same records.
//
// Torn tails: a short or corrupt frame in the LAST segment is a torn write;
// the file is truncated at the last good record, counted and reported. The
// same bytes anywhere earlier are real corruption, and recovery refuses to
// guess (fail loudly with the offset) — on one node there is no quorum to
// repair from.

import (
	"fmt"
	"os"
	"time"
)

// RecoveryReport says what replay found and did. It is returned by Open so
// an operator can log it; nothing here is hidden.
type RecoveryReport struct {
	Segments         int
	RecordsApplied   int
	CleanShutdown    bool
	TornTail         bool
	BytesTruncated   int64
	JobsRestored     int // jobs returned to ready lists (all topics)
	JobsDeadLettered int // exhausted in-flight jobs moved to DLQs during replay
}

// replayState is the reconstructed world: per-topic FIFO job lists, in log
// order, ready to seed live topics, plus the topic-ID table the log used.
type replayState struct {
	topicJobs map[string][]*Job // topic name -> jobs in FIFO order
	topics    map[uint32]string
}

type replayJob struct {
	job      *Job
	topic    string
	inflight bool
	seq      int // FIFO position; reassigned on every requeue
}

func replayDir(walDir string) (*replayState, RecoveryReport, error) {
	var rep RecoveryReport
	segs, err := listSegments(walDir)
	if err != nil {
		return nil, rep, err
	}
	rep.Segments = len(segs)

	topics := map[uint32]string{}
	jobs := map[string]*replayJob{}
	seq := 0
	var lastType recType

	for i, seg := range segs {
		raw, err := os.ReadFile(seg.path)
		if err != nil {
			return nil, rep, err
		}
		buf := raw[segmentHeaderSize:]
		off := int64(segmentHeaderSize)
		for len(buf) > 0 {
			rec, n, err := readRecord(buf)
			if err != nil {
				last := i == len(segs)-1
				if !last {
					return nil, rep, fmt.Errorf(
						"jobq: corruption in non-tail segment %s at offset %d: %w",
						seg.path, off, err)
				}
				// Torn tail: truncate the file at the last good record so
				// the next writer resumes after valid bytes.
				rep.TornTail = true
				rep.BytesTruncated = int64(len(buf))
				if terr := os.Truncate(seg.path, off); terr != nil {
					return nil, rep, fmt.Errorf("jobq: truncating torn tail: %w", terr)
				}
				buf = nil
				continue
			}
			applyRecord(rec, topics, jobs, &seq)
			lastType = rec.Type
			rep.RecordsApplied++
			buf = buf[n:]
			off += int64(n)
		}
	}
	rep.CleanShutdown = lastType == recCleanShut

	// Settle the crash-time in-flight set: expired, per the spec.
	st := &replayState{topicJobs: map[string][]*Job{}, topics: topics}
	ordered := make([]*replayJob, 0, len(jobs))
	for _, rj := range jobs {
		ordered = append(ordered, rj)
	}
	// FIFO by seq (map iteration is random).
	sortReplayJobs(ordered)
	for _, rj := range ordered {
		if rj.inflight {
			if rj.job.Attempts >= rj.job.MaxAttempts {
				dlqName := DeadLetterTopic(rj.topic)
				j := rj.job
				j.DeadLetteredAttempts = j.Attempts
				j.Attempts, j.Attempt = 0, 0
				j.MaxAttempts = DefaultMaxAttempts
				j.DeadLetteredAt = time.Time{} // unknown crash time; zero is honest
				j.Topic = dlqName
				st.topicJobs[dlqName] = append(st.topicJobs[dlqName], j)
				rep.JobsDeadLettered++
				continue
			}
			rj.inflight = false // lease expired by restart
		}
		rj.job.Topic = rj.topic
		st.topicJobs[rj.topic] = append(st.topicJobs[rj.topic], rj.job)
		rep.JobsRestored++
	}
	return st, rep, nil
}

func applyRecord(rec walRecord, topics map[uint32]string, jobs map[string]*replayJob, seq *int) {
	switch rec.Type {
	case recTopic:
		topics[rec.TopicID] = rec.TopicName
	case recEnqueue:
		*seq++
		job := &Job{
			ID:          rec.JobID,
			Payload:     rec.Payload,
			MaxAttempts: rec.MaxAttempts,
			EnqueuedAt:  rec.At,
		}
		if rec.Delay > 0 {
			job.NotBefore = rec.At.Add(rec.Delay)
		}
		jobs[rec.JobID] = &replayJob{job: job, topic: topics[rec.TopicID], seq: *seq}
	case recDeliver:
		if rj, ok := jobs[rec.JobID]; ok {
			rj.job.Attempts = rec.Attempts
			rj.job.Attempt = rec.Attempts
			rj.inflight = true
		}
	case recRetry:
		if rj, ok := jobs[rec.JobID]; ok {
			*seq++
			rj.job.Attempts = rec.Attempts
			rj.inflight = false
			rj.seq = *seq // requeued at the back, like live Nack
		}
	case recAck, recDrop:
		delete(jobs, rec.JobID)
	case recAdopt:
		if rj, ok := jobs[rec.JobID]; ok {
			*seq++
			j := rj.job
			// The record carries the attempts-at-dead-lettering; the job's
			// live counter may be stale relative to what was logged.
			j.DeadLetteredAttempts = rec.Attempts
			j.Attempts, j.Attempt = 0, 0
			j.MaxAttempts = DefaultMaxAttempts
			j.DeadLetteredAt = rec.At
			rj.topic = topics[rec.TopicID]
			rj.inflight = false
			rj.seq = *seq
		}
	case recCleanShut:
		// Position is meaningful (must be last); handled by the caller via
		// lastType. No state change.
	}
}

// topicNames returns name→id as seen in the log, so a reopened queue keeps
// assigning fresh topic IDs above the existing ones.
func topicIDsFromLog(topics map[uint32]string) (map[string]uint32, uint32) {
	byName := make(map[string]uint32, len(topics))
	var max uint32
	for id, name := range topics {
		byName[name] = id
		if id > max {
			max = id
		}
	}
	return byName, max
}

func sortReplayJobs(rjs []*replayJob) {
	// Insertion sort keeps the dependency surface zero; replay is not a hot
	// path and n is the live job count, not the log length.
	for i := 1; i < len(rjs); i++ {
		for j := i; j > 0 && rjs[j].seq < rjs[j-1].seq; j-- {
			rjs[j], rjs[j-1] = rjs[j-1], rjs[j]
		}
	}
}
