package jobq

import "time"

// Client is the lifecycle surface of a Queue: enqueue, dequeue, and settle a
// lease. It exists so a remote queue (jobqhttp.Client, talking to a jobqd
// over HTTP) and a local one (*Queue) are interchangeable to calling code —
// the round-trip tests in jobqhttp pin that the two behave identically.
//
// Stats and Topics are deliberately NOT part of this interface. Queue.Stats
// and Queue.Topics cannot fail; the HTTP versions can (a request can time
// out or the server can be unreachable). Widening the local signatures to
// return an error just to match would be a lie about the in-process case, so
// the asymmetry is documented instead of hidden.
type Client interface {
	// Enqueue adds a job to a topic and returns its ID.
	Enqueue(topic string, payload []byte, opts ...EnqueueOption) (string, error)
	// Dequeue leases the next ready job from a topic.
	Dequeue(topic string, visibility time.Duration) (*Job, Lease, error)
	// Ack marks a leased job complete.
	Ack(lease Lease) error
	// Nack returns a leased job for retry, consuming one attempt.
	Nack(lease Lease) error
	// Extend lengthens a lease and returns the effective deadline.
	Extend(lease Lease, d time.Duration) (time.Time, error)
}

// var _ Client = (*Queue)(nil) pins the compile-time guarantee that Queue's
// method set never drifts from Client's.
var _ Client = (*Queue)(nil)
