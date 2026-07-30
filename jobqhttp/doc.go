// Package jobqhttp is the HTTP surface for jobq: NewHandler wraps a
// *jobq.Queue as an http.Handler, and Client talks to that handler (or any
// server implementing the same wire format) as a jobq.Client.
//
// # Route table and error envelope
//
// See docs/specs/2026-07-30-jobq-p3-http.md for the full route table,
// status/code mapping, and enqueue content-type rules. In short: every
// error response is {"error":{"code":"...","message":"..."}}; jobq.ErrNoJob
// is not an error on the wire, it is 204 No Content.
//
// # Stats and Topics are not on the jobq.Client interface
//
// jobq.Queue.Stats and jobq.Queue.Topics cannot fail; over HTTP they can
// (timeout, connection refused, server error). Client exposes them as
// separate methods — TopicStats, Stats, Topics — that return an error,
// rather than widening the shared jobq.Client interface's local signature
// to lie about the in-process case.
//
// # An empty topic segment never reaches the handler
//
// "POST /v1/topics//jobs" (an empty {topic}) collapses to a double slash
// that Go's http.ServeMux does not route to any registered pattern — the
// request 404s from the mux itself, before jobq.ErrEmptyTopic could ever be
// produced, and the response is the mux's plain-text 404, not the JSON
// error envelope. Client.Enqueue checks for an empty topic itself and
// returns jobq.ErrEmptyTopic without a request, so Go callers see the same
// error as a local *jobq.Queue; a raw HTTP caller constructing that URL by
// hand will see the mux's 404 instead of a 400 empty_topic — a routing
// artifact worth knowing about, not a bug to work around with a second
// route just for it.
//
// # Long-poll is polling
//
// The dequeue endpoint's wait_ms parameter is a bounded retry loop over the
// ordinary Dequeue call (~25ms between attempts), not a notification path.
// A caller expecting push-style wakeup on enqueue will see up to one tick
// of extra latency instead — documented rather than left to be discovered.
package jobqhttp
