package cluster

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/aleks-drozy/jobq/raft"
)

// Raft messages between nodes travel as HTTP POSTs of a small binary
// format. One request carries a batch: whatever queued up for that peer
// while the previous request was in flight.
//
// The peer endpoint has no authentication. Anyone who can reach it can
// feed the node Raft messages, so it must only ever be reachable by the
// other nodes of the cluster.

// PeerPath is where a node accepts messages from its peers.
const PeerPath = "/raft/v1/messages"

const (
	peerQueueSize   = 1024
	peerBatchMax    = 64
	peerSendTimeout = 2 * time.Second
	// maxPeerBody bounds one batch on the receiving side.
	maxPeerBody = 256 << 20
)

// HTTPTransport sends Raft messages to peers over HTTP. Each peer has its
// own bounded queue and sender, so one dead or slow peer delays nobody
// else; when a queue is full new messages for that peer are dropped.
type HTTPTransport struct {
	client *http.Client
	queues map[raft.NodeID]chan raft.Message
	stop   chan struct{}
	wg     sync.WaitGroup
	once   sync.Once
}

// NewHTTPTransport starts a sender for every peer in peers except self.
// Each value is the peer's base URL, for example "http://10.0.1.12:9090".
func NewHTTPTransport(self raft.NodeID, peers map[raft.NodeID]string) *HTTPTransport {
	t := &HTTPTransport{
		client: &http.Client{Timeout: peerSendTimeout},
		queues: map[raft.NodeID]chan raft.Message{},
		stop:   make(chan struct{}),
	}
	for id, base := range peers {
		if id == self {
			continue
		}
		q := make(chan raft.Message, peerQueueSize)
		t.queues[id] = q
		t.wg.Add(1)
		go t.sender(strings.TrimSuffix(base, "/")+PeerPath, q)
	}
	return t
}

// Send implements Transport.
func (t *HTTPTransport) Send(m raft.Message) {
	q, ok := t.queues[m.To]
	if !ok {
		return
	}
	select {
	case q <- m:
	default:
	}
}

// Close stops the senders. Messages still queued are discarded.
func (t *HTTPTransport) Close() {
	t.once.Do(func() { close(t.stop) })
	t.wg.Wait()
}

func (t *HTTPTransport) sender(url string, q chan raft.Message) {
	defer t.wg.Done()
	for {
		var first raft.Message
		select {
		case <-t.stop:
			return
		case first = <-q:
		}
		body := appendFrame(nil, first)
	fill:
		for i := 1; i < peerBatchMax; i++ {
			select {
			case m := <-q:
				body = appendFrame(body, m)
			default:
				break fill
			}
		}
		// A failed send is not retried: Raft resends what still matters
		// on its next heartbeat, and by then this batch may be stale.
		resp, err := t.client.Post(url, "application/octet-stream", bytes.NewReader(body))
		if err != nil {
			continue
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
}

// PeerHandler serves PeerPath for n: it decodes a batch and steps the node
// with each message addressed to it.
func PeerHandler(n *Node) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST "+PeerPath, func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxPeerBody))
		if err != nil {
			http.Error(w, "unreadable body", http.StatusBadRequest)
			return
		}
		msgs, err := decodeFrames(body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		for _, m := range msgs {
			if m.To == n.cfg.ID {
				n.Step(m)
			}
		}
		w.WriteHeader(http.StatusNoContent)
	})
	return mux
}

var errBadMessage = errors.New("cluster: malformed peer message")

// appendFrame appends one length-prefixed message to b.
func appendFrame(b []byte, m raft.Message) []byte {
	at := len(b)
	b = append(b, 0, 0, 0, 0)
	b = appendMessage(b, m)
	binary.LittleEndian.PutUint32(b[at:], uint32(len(b)-at-4))
	return b
}

func decodeFrames(b []byte) ([]raft.Message, error) {
	var out []raft.Message
	r := reader{buf: b}
	for len(r.buf) > 0 {
		frame := r.take(uint64(r.u32()))
		if r.bad {
			return nil, errBadMessage
		}
		m, err := decodeMessage(frame)
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, nil
}

func appendMessage(b []byte, m raft.Message) []byte {
	b = append(b, byte(m.Type))
	for _, v := range []uint64{uint64(m.From), uint64(m.To), m.Term, m.LogIndex, m.LogTerm, m.Commit, m.Hint} {
		b = binary.LittleEndian.AppendUint64(b, v)
	}
	reject := byte(0)
	if m.Reject {
		reject = 1
	}
	b = append(b, reject)
	b = binary.AppendUvarint(b, uint64(len(m.Entries)))
	for _, e := range m.Entries {
		b = binary.LittleEndian.AppendUint64(b, e.Index)
		b = binary.LittleEndian.AppendUint64(b, e.Term)
		b = append(b, byte(e.Kind))
		b = appendBytes(b, e.Data)
	}
	return b
}

func decodeMessage(b []byte) (raft.Message, error) {
	r := reader{buf: b}
	m := raft.Message{Type: raft.MessageType(r.u8())}
	m.From = raft.NodeID(r.u64())
	m.To = raft.NodeID(r.u64())
	m.Term = r.u64()
	m.LogIndex = r.u64()
	m.LogTerm = r.u64()
	m.Commit = r.u64()
	m.Hint = r.u64()
	m.Reject = r.u8() == 1
	count := r.uvarint()
	// Each entry is at least 18 bytes, so a count the remaining bytes
	// cannot hold is a lie, and is refused before anything is allocated.
	if r.bad || count > uint64(len(r.buf))/18 {
		return raft.Message{}, errBadMessage
	}
	for i := uint64(0); i < count; i++ {
		e := raft.Entry{Index: r.u64(), Term: r.u64(), Kind: raft.EntryKind(r.u8())}
		e.Data = r.bytes()
		m.Entries = append(m.Entries, e)
	}
	if r.bad || len(r.buf) != 0 || m.Type < raft.MsgVote || m.Type > raft.MsgAppendResp {
		return raft.Message{}, errBadMessage
	}
	return m, nil
}
