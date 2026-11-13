package jobq

// Segment files: wal/%016x.seg, named by the byte offset of their first
// record in the whole-log ordering. Each starts with a 32-byte synced
// header. The header, not the file name, is the source of truth — Windows
// cannot fsync a directory (File.Sync on a directory handle returns
// "Access is denied"), so recovery trusts only file contents it can CRC.

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"time"
)

const (
	segmentMagic        = "JOBQWAL\x01"
	segmentHeaderSize   = 32
	defaultSegmentBytes = 32 << 20
)

type segmentInfo struct {
	path       string
	baseOffset uint64
	seq        uint32
}

// listSegments returns the valid segments in walDir in log order, verifying
// every header. A file with a corrupt header is an error: segment headers
// are written and synced before any record, so a bad one is real corruption,
// not a torn write. The one exception is the newest file when it holds no
// more than a header: see below.
func listSegments(walDir string) ([]segmentInfo, error) {
	entries, err := os.ReadDir(walDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var names []string
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".seg" {
			names = append(names, e.Name())
		}
	}
	// Names are the base offset in fixed-width hex, so they sort in log
	// order without a header to read.
	sort.Strings(names)
	var segs []segmentInfo
	for i, name := range names {
		path := filepath.Join(walDir, name)
		info, err := readSegmentHeader(path)
		if err != nil {
			// A process killed between creating a segment and syncing its
			// header leaves a file with no usable header and, necessarily,
			// no records. That can only be the newest file. It is a torn
			// write like any other at the tail, and is removed so the next
			// writer can create the segment again under the same name.
			if fi, serr := os.Stat(path); i == len(names)-1 && serr == nil && fi.Size() <= segmentHeaderSize {
				if rerr := os.Remove(path); rerr != nil {
					return nil, fmt.Errorf("jobq: removing half-created segment %s: %w", path, rerr)
				}
				continue
			}
			return nil, fmt.Errorf("jobq: segment %s: %w", path, err)
		}
		segs = append(segs, info)
	}
	sort.Slice(segs, func(i, j int) bool { return segs[i].baseOffset < segs[j].baseOffset })
	return segs, nil
}

func readSegmentHeader(path string) (segmentInfo, error) {
	f, err := os.Open(path)
	if err != nil {
		return segmentInfo{}, err
	}
	defer f.Close()
	var hdr [segmentHeaderSize]byte
	if _, err := f.Read(hdr[:]); err != nil {
		return segmentInfo{}, fmt.Errorf("header unreadable: %w", err)
	}
	if string(hdr[0:8]) != segmentMagic {
		return segmentInfo{}, fmt.Errorf("bad magic")
	}
	want := binary.LittleEndian.Uint32(hdr[28:32])
	if crc32.Checksum(hdr[0:28], castagnoli) != want {
		return segmentInfo{}, fmt.Errorf("header crc mismatch")
	}
	return segmentInfo{
		path:       path,
		baseOffset: binary.LittleEndian.Uint64(hdr[8:16]),
		seq:        binary.LittleEndian.Uint32(hdr[16:20]),
	}, nil
}

// segmentWriter appends frames to the current segment, rotating when the
// size cap is passed. Only the committer goroutine touches it.
type segmentWriter struct {
	dir      string
	maxBytes int64

	file    *os.File
	written int64 // bytes in the current segment, header included
	offset  uint64
	seq     uint32
}

func openSegmentWriter(walDir string, maxBytes int64) (*segmentWriter, error) {
	segs, err := listSegments(walDir)
	if err != nil {
		return nil, err
	}
	w := &segmentWriter{dir: walDir, maxBytes: maxBytes}
	if n := len(segs); n > 0 {
		last := segs[n-1]
		fi, err := os.Stat(last.path)
		if err != nil {
			return nil, err
		}
		if fi.Size() == segmentHeaderSize {
			// The last process opened this segment and died before
			// writing a record. A fresh segment would start at the same
			// offset and so take the same name; write into this one.
			f, err := os.OpenFile(last.path, os.O_WRONLY|os.O_APPEND, 0o600)
			if err != nil {
				return nil, err
			}
			w.file = f
			w.written = segmentHeaderSize
			w.offset = last.baseOffset
			w.seq = last.seq + 1
			return w, nil
		}
		// Resume the global offset after the existing log; the last segment
		// is left untouched (recovery already dealt with its tail) and a
		// fresh segment starts at the next offset.
		w.offset = last.baseOffset + uint64(fi.Size()) - segmentHeaderSize
		w.seq = last.seq + 1
	}
	if err := w.rotate(); err != nil {
		return nil, err
	}
	return w, nil
}

// cut syncs the current segment and starts a new one, unless the current
// one is still empty, in which case it already is the boundary. It returns
// the base offset of the segment the next write goes to.
func (w *segmentWriter) cut() (uint64, error) {
	if w.written == segmentHeaderSize {
		return w.offset, nil
	}
	if err := w.rotate(); err != nil {
		return 0, err
	}
	return w.offset, nil
}

func (w *segmentWriter) rotate() error {
	if w.file != nil {
		if err := w.file.Sync(); err != nil {
			return err
		}
		if err := w.file.Close(); err != nil {
			return err
		}
	}
	path := filepath.Join(w.dir, fmt.Sprintf("%016x.seg", w.offset))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	var hdr [segmentHeaderSize]byte
	copy(hdr[0:8], segmentMagic)
	binary.LittleEndian.PutUint64(hdr[8:16], w.offset)
	binary.LittleEndian.PutUint32(hdr[16:20], w.seq)
	binary.LittleEndian.PutUint64(hdr[20:28], uint64(time.Now().UnixNano()))
	binary.LittleEndian.PutUint32(hdr[28:32], crc32.Checksum(hdr[0:28], castagnoli))
	if _, err := f.Write(hdr[:]); err != nil {
		f.Close()
		return err
	}
	// The header is synced immediately: a segment either exists with a
	// verifiable header or is garbage recovery can ignore; never in between.
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	w.file = f
	w.written = segmentHeaderSize
	w.seq++
	return nil
}

func (w *segmentWriter) write(batch []byte) error {
	if w.written >= w.maxBytes {
		if err := w.rotate(); err != nil {
			return err
		}
	}
	n, err := w.file.Write(batch)
	w.written += int64(n)
	w.offset += uint64(n)
	return err
}

func (w *segmentWriter) sync() error { return w.file.Sync() }

func (w *segmentWriter) closeFile() error { return w.file.Close() }
