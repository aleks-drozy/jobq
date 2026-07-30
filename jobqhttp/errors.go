package jobqhttp

// Sentinel <-> HTTP status/code mapping, in both directions. writeError
// (server) walks sentinelTable with errors.Is so a wrapped sentinel still
// matches; parseErrorEnvelope (client) looks the code up in codeToSentinel
// so errors.Is(err, jobq.ErrX) works identically against a remote Client and
// a local *Queue — that equivalence is what roundtrip_test.go pins.

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/aleks-drozy/jobq"
)

var sentinelTable = []struct {
	err    error
	status int
	code   string
}{
	{jobq.ErrUnknownLease, http.StatusNotFound, "unknown_lease"},
	{jobq.ErrEmptyTopic, http.StatusBadRequest, "empty_topic"},
	{jobq.ErrReservedTopic, http.StatusBadRequest, "reserved_topic"},
	{jobq.ErrNonPositiveDuration, http.StatusBadRequest, "non_positive_duration"},
	{jobq.ErrClosed, http.StatusServiceUnavailable, "closed"},
}

var codeToSentinel = func() map[string]error {
	m := make(map[string]error, len(sentinelTable))
	for _, e := range sentinelTable {
		m[e.code] = e.err
	}
	return m
}()

// statusAndCodeFor returns the HTTP status and machine code for err. Unknown
// errors (a bug, not an engine sentinel) map to 500/"internal" rather than
// leaking Go error text as an API contract.
func statusAndCodeFor(err error) (status int, code string) {
	for _, e := range sentinelTable {
		if errors.Is(err, e.err) {
			return e.status, e.code
		}
	}
	return http.StatusInternalServerError, "internal"
}

// apiError is what Client returns for a server-reported error whose code is
// not one of jobq's sentinels (e.g. "bad_request", "payload_too_large" —
// client-input problems that have no equivalent *Queue error to impersonate).
type apiError struct {
	Status  int
	Code    string
	Message string
}

func (e *apiError) Error() string {
	return fmt.Sprintf("jobqhttp: %s (%d): %s", e.Code, e.Status, e.Message)
}

// errorFromEnvelope reconstitutes the real jobq sentinel when the code names
// one, so a caller's errors.Is(err, jobq.ErrNoJob)-style check behaves the
// same for a remote Client as for a local *Queue. Any other code becomes an
// *apiError carrying the status and code for callers that want to branch on
// it directly.
func errorFromEnvelope(status int, env errorEnvelope) error {
	if sentinel, ok := codeToSentinel[env.Error.Code]; ok {
		return sentinel
	}
	return &apiError{Status: status, Code: env.Error.Code, Message: env.Error.Message}
}
