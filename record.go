package jobq

// WAL record encoding. Frame layout, little-endian:
//
//	u32 len   — length of (type byte + body)
//	u32 crc   — crc32c over (len ‖ type ‖ body)
//	u8  type
//	body
//
// The CRC covers the length prefix so a corrupted length is detected rather
// than used to skip into garbage. Castagnoli, not IEEE: hash/crc32 has a
// hardware path for it on amd64/arm64. Timestamps travel as i64 seconds +
// u32 nanoseconds because time.Time{}.UnixNano() does not round-trip to a
// zero time; (Unix, Nanosecond) does.

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"hash/crc32"
	"time"
)

type recType uint8

const (
	recTopic     recType = 0x01 // topicID ↔ name, written once per topic
	recEnqueue   recType = 0x02 // full job incl. payload — payload written exactly once
	recDeliver   recType = 0x03 // job leased; attempts after increment
	recAck       recType = 0x04 // terminal: completed
	recRetry     recType = 0x05 // back to ready (nack or observed expiry); attempts
	recAdopt     recType = 0x06 // moved into a DLQ topic
	recDrop      recType = 0x07 // terminal: destroyed inside a DLQ (visible via Stats.Dropped)
	recCleanShut recType = 0x08 // written and synced by Close
)

var (
	errBadFrame   = errors.New("jobq: wal frame failed crc check")
	errShortFrame = errors.New("jobq: wal frame truncated")
)

const (
	frameHeaderSize = 9 // u32 len + u32 crc + u8 type
	jobIDWireSize   = 16
	// maxFrameLen bounds a single record; anything larger fails the CRC
	// anyway (the length is covered), this just caps allocation on decode.
	maxFrameLen = 64 << 20
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// walRecord is the union of every record's fields; Type selects which are
// meaningful. One struct keeps encode/decode/table-tests trivially symmetric.
type walRecord struct {
	Type        recType
	JobID       string // 32-char hex as produced by newID
	TopicID     uint32
	TopicName   string // recTopic only
	MaxAttempts int    // recEnqueue
	Attempts    int    // recDeliver, recRetry; DeadLetteredAttempts on recAdopt
	At          time.Time
	Delay       time.Duration // recEnqueue
	Payload     []byte        // recEnqueue
}

func writeRecord(w io.Writer, rec walRecord) error {
	body, err := encodeBody(rec)
	if err != nil {
		return err
	}
	frame := make([]byte, frameHeaderSize+len(body))
	binary.LittleEndian.PutUint32(frame[0:4], uint32(1+len(body)))
	frame[8] = byte(rec.Type)
	copy(frame[frameHeaderSize:], body)
	crc := crc32.Update(0, castagnoli, frame[0:4])
	crc = crc32.Update(crc, castagnoli, frame[8:])
	binary.LittleEndian.PutUint32(frame[4:8], crc)
	_, err = w.Write(frame)
	return err
}

// readRecord decodes one frame from the head of buf, returning the record
// and the number of bytes consumed. errShortFrame means buf ends mid-frame
// (a torn tail); errBadFrame means the bytes are corrupt.
func readRecord(buf []byte) (walRecord, int, error) {
	if len(buf) < frameHeaderSize {
		return walRecord{}, 0, errShortFrame
	}
	length := binary.LittleEndian.Uint32(buf[0:4])
	if length == 0 || length > maxFrameLen {
		return walRecord{}, 0, errBadFrame
	}
	total := frameHeaderSize + int(length) - 1
	if len(buf) < total {
		// Cannot verify the CRC without the whole frame; but if the frame is
		// self-consistent garbage we still must not report "short" forever —
		// recovery treats short-at-tail as torn and truncates.
		return walRecord{}, 0, errShortFrame
	}
	want := binary.LittleEndian.Uint32(buf[4:8])
	crc := crc32.Update(0, castagnoli, buf[0:4])
	crc = crc32.Update(crc, castagnoli, buf[8:total])
	if crc != want {
		return walRecord{}, 0, errBadFrame
	}
	rec, err := decodeBody(recType(buf[8]), buf[frameHeaderSize:total])
	if err != nil {
		return walRecord{}, 0, err
	}
	return rec, total, nil
}

func encodeBody(rec walRecord) ([]byte, error) {
	var b []byte
	switch rec.Type {
	case recTopic:
		b = binary.LittleEndian.AppendUint32(b, rec.TopicID)
		b = appendString(b, rec.TopicName)
	case recEnqueue:
		var err error
		if b, err = appendJobID(b, rec.JobID); err != nil {
			return nil, err
		}
		b = binary.LittleEndian.AppendUint32(b, rec.TopicID)
		b = binary.AppendUvarint(b, uint64(rec.MaxAttempts))
		b = appendTime(b, rec.At)
		b = binary.AppendUvarint(b, uint64(rec.Delay))
		b = binary.LittleEndian.AppendUint32(b, uint32(len(rec.Payload)))
		b = append(b, rec.Payload...)
	case recDeliver, recRetry:
		var err error
		if b, err = appendJobID(b, rec.JobID); err != nil {
			return nil, err
		}
		b = binary.AppendUvarint(b, uint64(rec.Attempts))
	case recAck, recDrop:
		var err error
		if b, err = appendJobID(b, rec.JobID); err != nil {
			return nil, err
		}
	case recAdopt:
		var err error
		if b, err = appendJobID(b, rec.JobID); err != nil {
			return nil, err
		}
		b = binary.LittleEndian.AppendUint32(b, rec.TopicID)
		b = binary.AppendUvarint(b, uint64(rec.Attempts))
		b = appendTime(b, rec.At)
	case recCleanShut:
		// empty body
	default:
		return nil, fmt.Errorf("jobq: unknown record type %#x", rec.Type)
	}
	return b, nil
}

func decodeBody(t recType, body []byte) (walRecord, error) {
	rec := walRecord{Type: t}
	d := &decoder{buf: body}
	switch t {
	case recTopic:
		rec.TopicID = d.u32()
		rec.TopicName = d.str()
	case recEnqueue:
		rec.JobID = d.jobID()
		rec.TopicID = d.u32()
		rec.MaxAttempts = int(d.uvarint())
		rec.At = d.time()
		rec.Delay = time.Duration(d.uvarint())
		rec.Payload = d.bytes(int(d.u32()))
	case recDeliver, recRetry:
		rec.JobID = d.jobID()
		rec.Attempts = int(d.uvarint())
	case recAck, recDrop:
		rec.JobID = d.jobID()
	case recAdopt:
		rec.JobID = d.jobID()
		rec.TopicID = d.u32()
		rec.Attempts = int(d.uvarint())
		rec.At = d.time()
	case recCleanShut:
	default:
		return rec, errBadFrame
	}
	if d.err != nil || len(d.buf) != 0 {
		return rec, errBadFrame
	}
	return rec, nil
}

func appendString(b []byte, s string) []byte {
	b = binary.AppendUvarint(b, uint64(len(s)))
	return append(b, s...)
}

func appendJobID(b []byte, id string) ([]byte, error) {
	raw, err := decodeHexID(id)
	if err != nil {
		return nil, err
	}
	return append(b, raw[:]...), nil
}

// appendTime encodes t as (unix seconds, nanoseconds); the zero time is a
// sentinel (0, ^u32(0)) chosen because a real time never has that nsec.
func appendTime(b []byte, t time.Time) []byte {
	if t.IsZero() {
		b = binary.LittleEndian.AppendUint64(b, 0)
		return binary.LittleEndian.AppendUint32(b, ^uint32(0))
	}
	b = binary.LittleEndian.AppendUint64(b, uint64(t.Unix()))
	return binary.LittleEndian.AppendUint32(b, uint32(t.Nanosecond()))
}

type decoder struct {
	buf []byte
	err error
}

func (d *decoder) take(n int) []byte {
	if d.err != nil || n < 0 || len(d.buf) < n {
		d.err = errBadFrame
		return nil
	}
	out := d.buf[:n]
	d.buf = d.buf[n:]
	return out
}

func (d *decoder) u32() uint32 {
	b := d.take(4)
	if b == nil {
		return 0
	}
	return binary.LittleEndian.Uint32(b)
}

func (d *decoder) uvarint() uint64 {
	v, n := binary.Uvarint(d.buf)
	if n <= 0 {
		d.err = errBadFrame
		return 0
	}
	d.buf = d.buf[n:]
	return v
}

func (d *decoder) str() string {
	n := d.uvarint()
	if n > uint64(len(d.buf)) {
		d.err = errBadFrame
		return ""
	}
	return string(d.take(int(n)))
}

func (d *decoder) bytes(n int) []byte {
	b := d.take(n)
	if b == nil {
		return nil
	}
	return append([]byte(nil), b...)
}

func (d *decoder) jobID() string {
	b := d.take(jobIDWireSize)
	if b == nil {
		return ""
	}
	return encodeHexID(b)
}

func (d *decoder) time() time.Time {
	b := d.take(12)
	if b == nil {
		return time.Time{}
	}
	sec := binary.LittleEndian.Uint64(b[0:8])
	nsec := binary.LittleEndian.Uint32(b[8:12])
	if sec == 0 && nsec == ^uint32(0) {
		return time.Time{}
	}
	return time.Unix(int64(sec), int64(nsec)).UTC()
}

func decodeHexID(id string) ([jobIDWireSize]byte, error) {
	var raw [jobIDWireSize]byte
	if len(id) != jobIDWireSize*2 {
		return raw, fmt.Errorf("jobq: job id %q is not %d hex chars", id, jobIDWireSize*2)
	}
	for i := 0; i < jobIDWireSize; i++ {
		hi, lo := hexVal(id[2*i]), hexVal(id[2*i+1])
		if hi < 0 || lo < 0 {
			return raw, fmt.Errorf("jobq: job id %q is not hex", id)
		}
		raw[i] = byte(hi<<4 | lo)
	}
	return raw, nil
}

func encodeHexID(raw []byte) string {
	const digits = "0123456789abcdef"
	out := make([]byte, len(raw)*2)
	for i, b := range raw {
		out[2*i] = digits[b>>4]
		out[2*i+1] = digits[b&0x0f]
	}
	return string(out)
}

func hexVal(c byte) int {
	switch {
	case c >= '0' && c <= '9':
		return int(c - '0')
	case c >= 'a' && c <= 'f':
		return int(c-'a') + 10
	default:
		return -1
	}
}
