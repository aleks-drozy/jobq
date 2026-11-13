package cluster

import (
	"encoding/binary"
	"errors"
	"time"
)

// op names the five operations that change a queue. Every one of them goes
// through the Raft log, Dequeue included: handing out a lease changes
// state, and a deposed leader that could do it on its own would hand out
// jobs the rest of the cluster knows nothing about.
type op uint8

const (
	opEnqueue op = iota + 1
	opDequeue
	opAck
	opNack
	opExtend
)

// command is one log entry's worth of work. At is filled in by the leader
// at the moment it proposes; it is the only clock a replica reads while
// applying, which is what keeps replicas identical.
type command struct {
	At    time.Time
	Op    op
	Topic string

	Payload     []byte        // enqueue
	Delay       time.Duration // enqueue
	MaxAttempts int           // enqueue
	Duration    time.Duration // dequeue: visibility; extend: extension
	LeaseID     string        // ack, nack, extend
}

var errBadCommand = errors.New("cluster: malformed command")

// encode lays a command out as:
//
//	i64  At, unix nanoseconds
//	u8   op
//	str  topic
//	...  fields of that op, in the order below
//
// where str and bytes are a uvarint length followed by the bytes.
func (c command) encode() []byte {
	b := make([]byte, 0, 32+len(c.Topic)+len(c.Payload)+len(c.LeaseID))
	b = binary.LittleEndian.AppendUint64(b, uint64(c.At.UnixNano()))
	b = append(b, byte(c.Op))
	b = appendBytes(b, []byte(c.Topic))
	switch c.Op {
	case opEnqueue:
		b = binary.AppendUvarint(b, uint64(nonNegative(int64(c.Delay))))
		b = binary.AppendUvarint(b, uint64(nonNegative(int64(c.MaxAttempts))))
		b = appendBytes(b, c.Payload)
	case opDequeue:
		b = binary.AppendUvarint(b, uint64(nonNegative(int64(c.Duration))))
	case opAck, opNack:
		b = appendBytes(b, []byte(c.LeaseID))
	case opExtend:
		b = appendBytes(b, []byte(c.LeaseID))
		b = binary.AppendUvarint(b, uint64(nonNegative(int64(c.Duration))))
	}
	return b
}

func decodeCommand(b []byte) (command, error) {
	d := reader{buf: b}
	c := command{At: time.Unix(0, int64(d.u64())).UTC(), Op: op(d.u8())}
	c.Topic = string(d.bytes())
	switch c.Op {
	case opEnqueue:
		c.Delay = time.Duration(d.uvarint())
		c.MaxAttempts = int(d.uvarint())
		c.Payload = d.bytes()
	case opDequeue:
		c.Duration = time.Duration(d.uvarint())
	case opAck, opNack:
		c.LeaseID = string(d.bytes())
	case opExtend:
		c.LeaseID = string(d.bytes())
		c.Duration = time.Duration(d.uvarint())
	default:
		return command{}, errBadCommand
	}
	if d.bad || len(d.buf) != 0 {
		return command{}, errBadCommand
	}
	return c, nil
}

func nonNegative(v int64) int64 {
	if v < 0 {
		return 0
	}
	return v
}

func appendBytes(b, p []byte) []byte {
	b = binary.AppendUvarint(b, uint64(len(p)))
	return append(b, p...)
}

// reader decodes the little formats in this package. It never panics on
// short or hostile input: the first problem sets bad and every later read
// returns zero, so callers check once at the end.
type reader struct {
	buf []byte
	bad bool
}

func (r *reader) take(n uint64) []byte {
	if r.bad || n > uint64(len(r.buf)) {
		r.bad = true
		return nil
	}
	out := r.buf[:n]
	r.buf = r.buf[n:]
	return out
}

func (r *reader) u8() uint8 {
	if b := r.take(1); b != nil {
		return b[0]
	}
	return 0
}

func (r *reader) u32() uint32 {
	if b := r.take(4); b != nil {
		return binary.LittleEndian.Uint32(b)
	}
	return 0
}

func (r *reader) u64() uint64 {
	if b := r.take(8); b != nil {
		return binary.LittleEndian.Uint64(b)
	}
	return 0
}

func (r *reader) uvarint() uint64 {
	if r.bad {
		return 0
	}
	v, n := binary.Uvarint(r.buf)
	if n <= 0 {
		r.bad = true
		return 0
	}
	r.buf = r.buf[n:]
	return v
}

// bytes returns a copy, so a decoded value never aliases a network buffer
// or a log entry.
func (r *reader) bytes() []byte {
	b := r.take(r.uvarint())
	if len(b) == 0 {
		return nil
	}
	return append([]byte(nil), b...)
}
