// Package jobqhttp is an HTTP surface over a queue, and the Client that
// talks to it. The queue is a local *jobq.Queue or one node of a replicated
// cluster (*cluster.Node); the API is the same either way. Server and
// client share this package so the wire types in wire.go are declared
// exactly once.
package jobqhttp

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aleks-drozy/jobq"
	"github.com/aleks-drozy/jobq/cluster"
)

// Backend is what the handler serves: the five lifecycle calls plus the
// read-only ones. *jobq.Queue and *cluster.Node both satisfy it.
type Backend interface {
	jobq.Client
	Stats(topic string) jobq.Stats
	Topics() []string
	Peek(topic string, limit int, includeInflight bool) (ready, inflight []*jobq.Job, err error)
}

var (
	_ Backend = (*jobq.Queue)(nil)
	_ Backend = (*cluster.Node)(nil)
)

// clusterBackend is the extra a replicated node offers; a plain queue does
// not implement it, and GET /v1/cluster says so.
type clusterBackend interface {
	Status() cluster.Status
	LeaderAddr() string
	Metrics() cluster.Metrics
}

const (
	defaultMaxPayload    = 1 << 20 // 1 MiB
	defaultMaxWait       = 30 * time.Second
	defaultMaxVisibility = 24 * time.Hour

	defaultPeekLimit = 10
	maxPeekLimit     = 100
)

type handlerOptions struct {
	maxPayload    int64
	maxWait       time.Duration
	maxVisibility time.Duration
	logger        *log.Logger

	haveRecovery bool
	recovery     jobq.RecoveryReport
	dir          string
	sync         string
	startedAt    time.Time
	diskUsage    func() (logBytes, snapshotBytes int64)
}

// HandlerOption configures NewHandler.
type HandlerOption func(*handlerOptions)

// WithMaxPayload caps the enqueue body size (default 1 MiB). Enforced with
// http.MaxBytesReader, so an oversize body is rejected while streaming, not
// after being buffered in full.
func WithMaxPayload(n int64) HandlerOption {
	return func(o *handlerOptions) { o.maxPayload = n }
}

// WithMaxWait caps how long a dequeue's wait_ms may block the connection
// (default 30s), regardless of what the caller asks for.
func WithMaxWait(d time.Duration) HandlerOption {
	return func(o *handlerOptions) { o.maxWait = d }
}

// WithMaxVisibility caps the visibility/extend duration a caller may request
// (default 24h).
func WithMaxVisibility(d time.Duration) HandlerOption {
	return func(o *handlerOptions) { o.maxVisibility = d }
}

// WithLogger sets the logger used for server-side diagnostics (default
// discards output).
func WithLogger(l *log.Logger) HandlerOption {
	return func(o *handlerOptions) { o.logger = l }
}

// WithRecovery attaches the RecoveryReport captured once at process start,
// served by GET /v1/recovery, and marks the queue as durable for GET
// /v1/health. Omit it for an in-memory queue (jobq.New): health then
// reports durable:false and /v1/recovery reports a zero report with an
// empty dir.
func WithRecovery(report jobq.RecoveryReport, dir, sync string) HandlerOption {
	return func(o *handlerOptions) {
		o.haveRecovery = true
		o.recovery = report
		o.dir = dir
		o.sync = sync
	}
}

// WithDiskUsage lets GET /metrics report how much disk a replicated node's
// log and snapshot take; pass (*jobq.RaftLog).DiskUsage.
func WithDiskUsage(f func() (logBytes, snapshotBytes int64)) HandlerOption {
	return func(o *handlerOptions) { o.diskUsage = f }
}

type handler struct {
	q    Backend
	opts handlerOptions
}

// NewHandler builds the HTTP surface for q. See the package doc and
// docs/specs/2026-07-30-jobq-p3-http.md for the route table and error
// envelope shape.
func NewHandler(q Backend, opts ...HandlerOption) http.Handler {
	o := handlerOptions{
		maxPayload:    defaultMaxPayload,
		maxWait:       defaultMaxWait,
		maxVisibility: defaultMaxVisibility,
		logger:        log.New(discardWriter{}, "", 0),
		startedAt:     time.Now(),
	}
	for _, opt := range opts {
		opt(&o)
	}
	h := &handler{q: q, opts: o}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/topics/{topic}/jobs", h.handleEnqueue)
	mux.HandleFunc("POST /v1/topics/{topic}/dequeue", h.handleDequeue)
	mux.HandleFunc("POST /v1/topics/{topic}/leases/{leaseID}/ack", h.handleAck)
	mux.HandleFunc("POST /v1/topics/{topic}/leases/{leaseID}/nack", h.handleNack)
	mux.HandleFunc("POST /v1/topics/{topic}/leases/{leaseID}/extend", h.handleExtend)
	mux.HandleFunc("GET /v1/topics/{topic}/jobs", h.handlePeek)
	mux.HandleFunc("GET /v1/topics", h.handleTopics)
	mux.HandleFunc("GET /v1/topics/{topic}/stats", h.handleTopicStats)
	mux.HandleFunc("GET /v1/stats", h.handleStats)
	mux.HandleFunc("GET /v1/health", h.handleHealth)
	mux.HandleFunc("GET /v1/recovery", h.handleRecovery)
	mux.HandleFunc("GET /v1/cluster", h.handleCluster)
	mux.HandleFunc("GET /metrics", h.handleMetrics)
	return mux
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

// --- helpers -----------------------------------------------------------

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErrorEnvelope(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, errorEnvelope{Error: errorBody{Code: code, Message: message}})
}

// writeEngineError maps a *Queue error (an engine sentinel or something
// unexpected) to the response. Callers must special-case jobq.ErrNoJob
// themselves — it is 204, not an error.
func writeEngineError(w http.ResponseWriter, err error) {
	status, code := statusAndCodeFor(err)
	body := errorBody{Code: code, Message: err.Error()}
	// A node that is not the leader says who is, when it knows, so a
	// client can go straight there instead of trying every address.
	var nl *jobq.NotLeaderError
	if errors.As(err, &nl) {
		body.Leader = nl.Leader
	}
	writeJSON(w, status, errorEnvelope{Error: body})
}

// handleCluster implements GET /v1/cluster: this node's view of the
// cluster. It is answered from the node's own state without consulting
// anyone, so a node cut off from the others reports what it last knew.
func (h *handler) handleCluster(w http.ResponseWriter, r *http.Request) {
	cb, ok := h.q.(clusterBackend)
	if !ok {
		writeErrorEnvelope(w, http.StatusNotFound, "not_clustered", "this server is a single node, not part of a cluster")
		return
	}
	st := cb.Status()
	writeJSON(w, http.StatusOK, ClusterStatus{
		ID:            uint64(st.ID),
		State:         st.State.String(),
		Term:          st.Term,
		Leader:        uint64(st.Leader),
		LeaderAddr:    cb.LeaderAddr(),
		Commit:        st.Commit,
		Applied:       st.Applied,
		LastIndex:     st.LastIndex,
		SnapshotIndex: st.SnapshotIndex,
		FirstIndex:    st.FirstIndex,
	})
}

// decodeJSON reads a JSON body with unknown fields rejected, under
// maxPayload. A *http.MaxBytesError is reported to the caller so it can
// answer 413 instead of the generic 400.
func decodeJSON(w http.ResponseWriter, r *http.Request, maxPayload int64, v any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxPayload)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return err
	}
	return nil
}

func isMaxBytesError(err error) bool {
	var mbe *http.MaxBytesError
	return errors.As(err, &mbe)
}

func (h *handler) handleTopics(w http.ResponseWriter, r *http.Request) {
	names := h.q.Topics()
	sort.Strings(names)
	writeJSON(w, http.StatusOK, topicsResponseDTO{Topics: names})
}

func (h *handler) handleTopicStats(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	writeJSON(w, http.StatusOK, h.q.Stats(topic))
}

// handlePeek implements GET /v1/topics/{topic}/jobs — read-only inspection.
// It never leases, mutates, or logs anything (see Queue.Peek's doc for why
// that matters for a DLQ). limit defaults to 10 and is capped at 100;
// fetching one extra job internally is how Truncated is computed without
// widening jobq.Queue.Peek's own signature.
func (h *handler) handlePeek(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")

	limit := defaultPeekLimit
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			writeErrorEnvelope(w, http.StatusBadRequest, "bad_request", "invalid limit")
			return
		}
		limit = n
	}
	if limit > maxPeekLimit {
		limit = maxPeekLimit
	}

	includeInflight := false
	if v := r.URL.Query().Get("include_inflight"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			writeErrorEnvelope(w, http.StatusBadRequest, "bad_request", "invalid include_inflight")
			return
		}
		includeInflight = b
	}

	ready, inflight, err := h.q.Peek(topic, limit+1, includeInflight)
	if err != nil {
		writeEngineError(w, err)
		return
	}
	truncated := false
	if len(ready) > limit {
		truncated = true
		ready = ready[:limit]
	}
	if len(inflight) > limit {
		truncated = true
		inflight = inflight[:limit]
	}

	resp := peekResponseDTO{Ready: make([]jobDTO, len(ready)), Truncated: truncated}
	for i, j := range ready {
		resp.Ready[i] = jobToDTO(j)
	}
	if includeInflight {
		resp.Inflight = make([]jobDTO, len(inflight))
		for i, j := range inflight {
			resp.Inflight[i] = jobToDTO(j)
		}
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h *handler) handleStats(w http.ResponseWriter, r *http.Request) {
	names := h.q.Topics()
	sort.Strings(names)
	out := make(map[string]jobq.Stats, len(names))
	for _, name := range names {
		out[name] = h.q.Stats(name)
	}
	writeJSON(w, http.StatusOK, statsResponseDTO{Topics: out})
}

func (h *handler) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, healthResponseDTO{
		Status:   "ok",
		Durable:  h.opts.haveRecovery,
		UptimeMs: time.Since(h.opts.startedAt).Milliseconds(),
	})
}

func (h *handler) handleRecovery(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, recoveryResponseDTO{
		Report:    h.opts.recovery,
		StartedAt: h.opts.startedAt,
		Dir:       h.opts.dir,
		Sync:      h.opts.sync,
	})
}

func (h *handler) handleAck(w http.ResponseWriter, r *http.Request) {
	lease := jobq.Lease{ID: r.PathValue("leaseID"), Topic: r.PathValue("topic")}
	if err := h.q.Ack(lease); err != nil {
		writeEngineError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *handler) handleNack(w http.ResponseWriter, r *http.Request) {
	lease := jobq.Lease{ID: r.PathValue("leaseID"), Topic: r.PathValue("topic")}
	if err := h.q.Nack(lease); err != nil {
		writeEngineError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *handler) handleExtend(w http.ResponseWriter, r *http.Request) {
	var req extendRequestDTO
	if err := decodeJSON(w, r, h.opts.maxPayload, &req); err != nil {
		if isMaxBytesError(err) {
			writeErrorEnvelope(w, http.StatusRequestEntityTooLarge, "payload_too_large", err.Error())
			return
		}
		writeErrorEnvelope(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	d := time.Duration(req.ByMs) * time.Millisecond
	if h.opts.maxVisibility > 0 && d > h.opts.maxVisibility {
		d = h.opts.maxVisibility
	}
	lease := jobq.Lease{ID: r.PathValue("leaseID"), Topic: r.PathValue("topic")}
	deadline, err := h.q.Extend(lease, d)
	if err != nil {
		writeEngineError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, extendResponseDTO{Deadline: deadline})
}

func (h *handler) handleEnqueue(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	ct := r.Header.Get("Content-Type")
	mediaType := ct
	if i := strings.IndexByte(ct, ';'); i >= 0 {
		mediaType = ct[:i]
	}
	mediaType = strings.TrimSpace(mediaType)

	var payload []byte
	var delay time.Duration
	var maxAttempts int

	if strings.EqualFold(mediaType, "application/json") {
		var req enqueueJSONRequest
		if err := decodeJSON(w, r, h.opts.maxPayload, &req); err != nil {
			if isMaxBytesError(err) {
				writeErrorEnvelope(w, http.StatusRequestEntityTooLarge, "payload_too_large", err.Error())
				return
			}
			writeErrorEnvelope(w, http.StatusBadRequest, "bad_request", err.Error())
			return
		}
		payload = req.Payload
		delay = time.Duration(req.DelayMs) * time.Millisecond
		maxAttempts = req.MaxAttempts
	} else {
		r.Body = http.MaxBytesReader(w, r.Body, h.opts.maxPayload)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			if isMaxBytesError(err) {
				writeErrorEnvelope(w, http.StatusRequestEntityTooLarge, "payload_too_large", err.Error())
				return
			}
			writeErrorEnvelope(w, http.StatusBadRequest, "bad_request", err.Error())
			return
		}
		payload = body
		if v := r.URL.Query().Get("delay_ms"); v != "" {
			ms, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				writeErrorEnvelope(w, http.StatusBadRequest, "bad_request", "invalid delay_ms")
				return
			}
			delay = time.Duration(ms) * time.Millisecond
		}
		if v := r.URL.Query().Get("max_attempts"); v != "" {
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				writeErrorEnvelope(w, http.StatusBadRequest, "bad_request", "invalid max_attempts")
				return
			}
			maxAttempts = int(n)
		}
	}

	var opts []jobq.EnqueueOption
	if delay > 0 {
		opts = append(opts, jobq.WithDelay(delay))
	}
	if maxAttempts > 0 {
		opts = append(opts, jobq.WithMaxAttempts(maxAttempts))
	}
	// A request ID travels in a header so that it works for both body
	// forms. A replicated node uses it to make a retried enqueue add one
	// job; a single node ignores it, but refuses the same malformed ones.
	if key := r.Header.Get(RequestIDHeader); key != "" {
		if len(key) > jobq.MaxRequestIDLen {
			writeEngineError(w, jobq.ErrRequestIDTooLong)
			return
		}
		opts = append(opts, jobq.WithRequestID(key))
	}
	id, err := h.q.Enqueue(topic, payload, opts...)
	if err != nil {
		writeEngineError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, enqueueResponseDTO{ID: id})
}
