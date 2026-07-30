package jobqhttp

import (
	"errors"
	"net/http"
	"time"

	"github.com/aleks-drozy/jobq"
)

const defaultVisibility = 30 * time.Second

// longPollTick is how often handleDequeue retries Queue.Dequeue while
// waiting for a job to become ready. This is polling, not a notification
// path — say so in any docs that describe it, so nobody mistakes the
// bounded wait for a push mechanism.
const longPollTick = 25 * time.Millisecond

// handleDequeue implements POST /v1/topics/{topic}/dequeue. wait_ms bounds
// a retry loop over the ordinary, non-blocking Queue.Dequeue; it touches no
// engine internals and adds no new delivery semantics.
func (h *handler) handleDequeue(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")

	var req dequeueRequestDTO
	if r.ContentLength != 0 {
		if err := decodeJSON(w, r, h.opts.maxPayload, &req); err != nil {
			if isMaxBytesError(err) {
				writeErrorEnvelope(w, http.StatusRequestEntityTooLarge, "payload_too_large", err.Error())
				return
			}
			writeErrorEnvelope(w, http.StatusBadRequest, "bad_request", err.Error())
			return
		}
	}

	visibility := defaultVisibility
	if req.VisibilityMs > 0 {
		visibility = time.Duration(req.VisibilityMs) * time.Millisecond
	}
	if h.opts.maxVisibility > 0 && visibility > h.opts.maxVisibility {
		visibility = h.opts.maxVisibility
	}

	wait := time.Duration(req.WaitMs) * time.Millisecond
	if wait < 0 {
		wait = 0
	}
	if wait > h.opts.maxWait {
		wait = h.opts.maxWait
	}

	job, lease, err := h.q.Dequeue(topic, visibility)
	if err == nil {
		writeDequeueOK(w, job, lease)
		return
	}
	if !errors.Is(err, jobq.ErrNoJob) {
		writeEngineError(w, err)
		return
	}
	if wait <= 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	deadline := time.Now().Add(wait)
	ticker := time.NewTicker(longPollTick)
	defer ticker.Stop()
	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			w.WriteHeader(http.StatusNoContent)
			return
		case now := <-ticker.C:
			if !now.Before(deadline) {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			job, lease, err := h.q.Dequeue(topic, visibility)
			if err == nil {
				writeDequeueOK(w, job, lease)
				return
			}
			if !errors.Is(err, jobq.ErrNoJob) {
				writeEngineError(w, err)
				return
			}
		}
	}
}

func writeDequeueOK(w http.ResponseWriter, job *jobq.Job, lease jobq.Lease) {
	jd := jobToDTO(job)
	ld := leaseToDTO(lease)
	writeJSON(w, http.StatusOK, dequeueResponseDTO{Job: &jd, Lease: &ld})
}
