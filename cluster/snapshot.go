package cluster

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"time"

	"github.com/aleks-drozy/jobq"
)

// A snapshot is the whole replicated state machine at one log index: the
// queue's clock, the table of recent request IDs, and every topic's jobs,
// leases and counters. A node that loads one is indistinguishable from a
// node that applied the log up to that index, which is the only property a
// snapshot has to have.
//
// Layout, little-endian, where str and bytes are a uvarint length followed
// by the bytes, and time is i64 seconds then u32 nanoseconds:
//
//	"JQS1"
//	i64      clock, unix nanoseconds
//	uvarint  request IDs, oldest first, each: str id, str job id, i64 at
//	uvarint  topics, in name order, each:
//	    str      name
//	    uvarint  enqueued, acked, dead-lettered, dropped (cumulative)
//	    uvarint  ready jobs, in delivery order, each a job
//	    uvarint  leases, soonest deadline first, each: job, str lease id,
//	             time deadline
//	u32      CRC-32C of everything before it
//
// The checksum is there because a snapshot crosses the network between
// nodes, and a node that installs one gives up its log for it.
//
// and a job is:
//
//	str id, bytes payload, uvarint attempt, attempts, max attempts,
//	time enqueued, not before, dead-lettered, uvarint dead-lettered attempts

const snapshotMagic = "JQS1"

var errBadSnapshot = errors.New("cluster: malformed snapshot")

// state is what a snapshot holds.
type state struct {
	clock int64
	dedup []dedupEntry
	queue jobq.QueueState
}

func (s state) encode() []byte {
	b := append(make([]byte, 0, 4096), snapshotMagic...)
	b = binary.LittleEndian.AppendUint64(b, uint64(s.clock))
	b = binary.AppendUvarint(b, uint64(len(s.dedup)))
	for _, d := range s.dedup {
		b = appendBytes(b, []byte(d.requestID))
		b = appendBytes(b, []byte(d.jobID))
		b = binary.LittleEndian.AppendUint64(b, uint64(d.at))
	}
	b = binary.AppendUvarint(b, uint64(len(s.queue.Topics)))
	for _, t := range s.queue.Topics {
		b = appendBytes(b, []byte(t.Name))
		for _, v := range []int{t.Stats.Enqueued, t.Stats.Acked, t.Stats.DeadLetters, t.Stats.Dropped} {
			b = binary.AppendUvarint(b, uint64(nonNegative(int64(v))))
		}
		b = binary.AppendUvarint(b, uint64(len(t.Ready)))
		for _, job := range t.Ready {
			b = appendJob(b, job)
		}
		b = binary.AppendUvarint(b, uint64(len(t.Leased)))
		for _, l := range t.Leased {
			b = appendJob(b, l.Job)
			b = appendBytes(b, []byte(l.LeaseID))
			b = appendTime(b, l.Deadline)
		}
	}
	return binary.LittleEndian.AppendUint32(b, crc32.Checksum(b, castagnoli))
}

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

func appendJob(b []byte, job *jobq.Job) []byte {
	b = appendBytes(b, []byte(job.ID))
	b = appendBytes(b, job.Payload)
	for _, v := range []int{job.Attempt, job.Attempts, job.MaxAttempts} {
		b = binary.AppendUvarint(b, uint64(nonNegative(int64(v))))
	}
	b = appendTime(b, job.EnqueuedAt)
	b = appendTime(b, job.NotBefore)
	b = appendTime(b, job.DeadLetteredAt)
	return binary.AppendUvarint(b, uint64(nonNegative(int64(job.DeadLetteredAttempts))))
}

// appendTime writes seconds and nanoseconds separately: unlike UnixNano,
// that pair survives the zero time, which several job fields use to mean
// "not set".
func appendTime(b []byte, t time.Time) []byte {
	b = binary.LittleEndian.AppendUint64(b, uint64(t.Unix()))
	return binary.LittleEndian.AppendUint32(b, uint32(t.Nanosecond()))
}

func (r *reader) time() time.Time {
	sec := int64(r.u64())
	nsec := r.u32()
	if nsec >= 1e9 {
		r.bad = true
		return time.Time{}
	}
	return time.Unix(sec, int64(nsec)).UTC()
}

// count reads a list length and refuses one the remaining bytes could not
// hold at min bytes an item, so a hostile length allocates nothing.
func (r *reader) count(min int) int {
	n := r.uvarint()
	if r.bad || n > uint64(len(r.buf)/min) {
		r.bad = true
		return 0
	}
	return int(n)
}

func (r *reader) int() int {
	v := r.uvarint()
	if v > 1<<31 {
		r.bad = true
		return 0
	}
	return int(v)
}

func (r *reader) job(topic string) *jobq.Job {
	job := &jobq.Job{Topic: topic}
	job.ID = string(r.bytes())
	job.Payload = r.bytes()
	job.Attempt, job.Attempts, job.MaxAttempts = r.int(), r.int(), r.int()
	job.EnqueuedAt, job.NotBefore, job.DeadLetteredAt = r.time(), r.time(), r.time()
	job.DeadLetteredAttempts = r.int()
	return job
}

func decodeState(b []byte) (state, error) {
	if len(b) < len(snapshotMagic)+4 || string(b[:len(snapshotMagic)]) != snapshotMagic {
		return state{}, errBadSnapshot
	}
	body, sum := b[:len(b)-4], binary.LittleEndian.Uint32(b[len(b)-4:])
	if crc32.Checksum(body, castagnoli) != sum {
		return state{}, errBadSnapshot
	}
	r := reader{buf: body[len(snapshotMagic):]}
	var s state
	s.clock = int64(r.u64())
	for i, n := 0, r.count(10); i < n && !r.bad; i++ {
		s.dedup = append(s.dedup, dedupEntry{
			requestID: string(r.bytes()), jobID: string(r.bytes()), at: int64(r.u64()),
		})
	}
	for i, n := 0, r.count(7); i < n && !r.bad; i++ {
		t := jobq.TopicState{Name: string(r.bytes())}
		t.Stats.Enqueued, t.Stats.Acked, t.Stats.DeadLetters, t.Stats.Dropped = r.int(), r.int(), r.int(), r.int()
		for j, m := 0, r.count(42); j < m && !r.bad; j++ {
			t.Ready = append(t.Ready, r.job(t.Name))
		}
		for j, m := 0, r.count(55); j < m && !r.bad; j++ {
			l := jobq.LeasedJob{Job: r.job(t.Name)}
			l.LeaseID = string(r.bytes())
			l.Deadline = r.time()
			t.Leased = append(t.Leased, l)
		}
		t.Stats.Ready, t.Stats.InFlight = len(t.Ready), len(t.Leased)
		s.queue.Topics = append(s.queue.Topics, t)
	}
	if r.bad || len(r.buf) != 0 {
		return state{}, errBadSnapshot
	}
	return s, nil
}
