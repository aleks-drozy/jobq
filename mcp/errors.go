package mcp

import (
	"encoding/json"
	"errors"

	"github.com/aleks-drozy/jobq"
)

// toolError is what a tool handler returns for a validation problem or a
// mapped engine sentinel. Its Error() method is JSON, not prose: the SDK
// wraps a returned error in a CallToolResult with IsError=true and the
// error's message as the result's text content (see AddTool's doc), and
// this is how a model — or a test — gets a stable machine-readable `code`
// out of that text instead of parsing English.
type toolError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e *toolError) Error() string {
	b, err := json.Marshal(e)
	if err != nil {
		return e.Message // unreachable in practice: both fields are plain strings
	}
	return string(b)
}

// sentinelTable maps jobq's engine sentinels to the code and message a tool
// caller sees. Reuses the errors.Is discipline jobqhttp/errors.go
// established, so a wrapped sentinel still matches. jobq.ErrNoJob is
// deliberately absent: it is not an error on this surface, it's
// {"empty": true} (see the dequeue handler in tools.go).
var sentinelTable = []struct {
	err     error
	code    string
	message string
}{
	{
		jobq.ErrUnknownLease, "unknown_lease",
		"the lease is unknown, already settled, or expired and the job was redelivered — " +
			"the engine deliberately makes these indistinguishable; dequeue again rather than retrying this lease.",
	},
	{jobq.ErrEmptyTopic, "empty_topic", "topic name must not be empty."},
	{
		jobq.ErrReservedTopic, "reserved_topic",
		"topic names ending in .dlq are reserved for dead-letter queues; jobs arrive there only by exhausting their attempts.",
	},
	{
		jobq.ErrNonPositiveDuration, "non_positive_duration",
		"the requested duration must be positive; a zero or negative visibility/extension would issue an already-expired lease.",
	},
	{jobq.ErrClosed, "closed", "the queue is closed."},
}

// mapError translates err into a *toolError carrying a stable code, or nil
// if err is nil. Callers must special-case jobq.ErrNoJob before calling
// this — it is not an error on this surface. Unmapped errors (a bug, or a
// transport failure from an HTTP backend) become code "internal" rather
// than leaking arbitrary Go error text as if it were a contract.
func mapError(err error) error {
	if err == nil {
		return nil
	}
	// Already a *toolError (a validation error, or — in tests — a fault
	// injected by a Backend wrapper): pass it through rather than
	// re-wrapping it as code "internal".
	if te, ok := err.(*toolError); ok {
		return te
	}
	for _, e := range sentinelTable {
		if errors.Is(err, e.err) {
			return &toolError{Code: e.code, Message: e.message}
		}
	}
	return &toolError{Code: "internal", Message: err.Error()}
}
