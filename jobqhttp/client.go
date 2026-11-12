package jobqhttp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aleks-drozy/jobq"
)

// Client talks to a jobqd (or anything implementing the same wire format)
// and implements jobq.Client, so callers can use a remote queue exactly like
// a local *jobq.Queue for the five lifecycle methods. roundtrip_test.go
// pins that the two behave identically, sentinel errors included.
//
// Against a cluster, give it every node's address. It sends each request
// to the node it believes leads and moves on when it is wrong: to the
// address a follower names, or to the next one in the list when a node
// cannot be reached. It keeps trying for the retry budget (default 10s),
// which is what carries a caller across a change of leader.
//
// Two things follow from retrying, and both are within at-least-once. A
// request that failed in transit may have been applied before the retry
// applies it again, so an enqueue can land twice. And an ack that was
// applied before its reply was lost comes back from the retry as
// jobq.ErrUnknownLease: the job is settled even though the call failed.
type Client struct {
	addrs []string
	httpc *http.Client
	retry time.Duration

	mu         sync.Mutex
	target     string // where the next request goes
	next       int    // index into addrs to rotate to
	redirected bool   // a node has answered not_leader: this is a cluster
}

// ClientOption configures a Client.
type ClientOption func(*Client)

// WithHTTPClient replaces the underlying http.Client (for a custom
// transport or TLS config). To change only the timeout, use WithTimeout,
// which keeps the connection pool NewClient sets up.
func WithHTTPClient(c *http.Client) ClientOption {
	return func(cl *Client) { cl.httpc = c }
}

// WithTimeout sets the per-request timeout (default 10s). It must exceed
// the longest DequeueWait the caller intends to make.
func WithTimeout(d time.Duration) ClientOption {
	return func(cl *Client) { cl.httpc.Timeout = d }
}

// newTransport is http.DefaultTransport with a connection pool sized for
// how a queue is used: many workers, one host. The default keeps two idle
// connections per host, so a pool of ten workers sharing a Client would
// open and close a connection on most requests. Under load that is slow,
// and on a busy machine it runs the operating system out of local ports.
func newTransport() *http.Transport {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConns = 256
	tr.MaxIdleConnsPerHost = 64
	return tr
}

// WithRetryBudget sets how long a request against a cluster keeps trying
// other nodes before giving up (default 10s). Zero means one try.
func WithRetryBudget(d time.Duration) ClientOption {
	return func(cl *Client) { cl.retry = d }
}

// NewClient builds a Client pointed at baseURL (e.g. "http://127.0.0.1:8080").
// For a cluster, baseURL is every node's address separated by commas. A
// leader hint is followed only to one of these addresses, so list them all:
// a client given one follower's address gets jobq.ErrNotLeader back.
func NewClient(baseURL string, opts ...ClientOption) *Client {
	c := &Client{httpc: &http.Client{Timeout: 10 * time.Second, Transport: newTransport()}, retry: 10 * time.Second}
	for _, a := range strings.Split(baseURL, ",") {
		if a = strings.TrimSuffix(strings.TrimSpace(a), "/"); a != "" {
			c.addrs = append(c.addrs, a)
		}
	}
	if len(c.addrs) == 0 {
		c.addrs = []string{""}
	}
	c.target = c.addrs[0]
	c.next = 1 % len(c.addrs)
	for _, opt := range opts {
		opt(c)
	}
	return c
}

var _ jobq.Client = (*Client)(nil)

// Enqueue implements jobq.Client. An empty topic is rejected client-side,
// before any request is sent: an empty {topic} path segment collapses to a
// double slash that the server's mux never routes to the enqueue handler
// (it 404s before jobq ever sees the request), so this check is what makes
// Enqueue("", ...) return jobq.ErrEmptyTopic here exactly as it does against
// a local *jobq.Queue — see roundtrip_test.go.
func (c *Client) Enqueue(topic string, payload []byte, opts ...jobq.EnqueueOption) (string, error) {
	if strings.TrimSpace(topic) == "" {
		return "", jobq.ErrEmptyTopic
	}
	var o jobq.EnqueueOptions
	for _, opt := range opts {
		opt(&o)
	}
	req := enqueueJSONRequest{Payload: payload, DelayMs: ms(o.Delay), MaxAttempts: o.MaxAttempts}
	var resp enqueueResponseDTO
	if err := c.doJSON(context.Background(), http.MethodPost, "/v1/topics/"+url.PathEscape(topic)+"/jobs", req, &resp, http.StatusCreated); err != nil {
		return "", err
	}
	return resp.ID, nil
}

// Dequeue implements jobq.Client: a single, non-blocking attempt — it never
// waits, matching *jobq.Queue.Dequeue exactly. Use DequeueWait for the
// HTTP-only bounded long-poll.
func (c *Client) Dequeue(topic string, visibility time.Duration) (*jobq.Job, jobq.Lease, error) {
	return c.dequeue(context.Background(), topic, visibility, 0)
}

// DequeueWait is an HTTP-only extension: it blocks up to wait for a job to
// become ready (bounded server-side long-poll), returning jobq.ErrNoJob if
// none arrives in time. Not part of jobq.Client because a local *jobq.Queue
// has no equivalent wait.
func (c *Client) DequeueWait(topic string, visibility, wait time.Duration) (*jobq.Job, jobq.Lease, error) {
	ctx := context.Background()
	if wait > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, wait+5*time.Second)
		defer cancel()
	}
	return c.dequeue(ctx, topic, visibility, wait)
}

func (c *Client) dequeue(ctx context.Context, topic string, visibility, wait time.Duration) (*jobq.Job, jobq.Lease, error) {
	req := dequeueRequestDTO{VisibilityMs: ms(visibility), WaitMs: ms(wait)}
	var resp dequeueResponseDTO
	err := c.doJSON(ctx, http.MethodPost, "/v1/topics/"+url.PathEscape(topic)+"/dequeue", req, &resp, http.StatusOK)
	if errors.Is(err, errNoContent) {
		return nil, jobq.Lease{}, jobq.ErrNoJob
	}
	if err != nil {
		return nil, jobq.Lease{}, err
	}
	if resp.Job == nil || resp.Lease == nil {
		return nil, jobq.Lease{}, jobq.ErrNoJob
	}
	return dtoToJob(*resp.Job), dtoToLease(*resp.Lease), nil
}

// Ack implements jobq.Client.
func (c *Client) Ack(lease jobq.Lease) error {
	return c.settle(lease, "ack")
}

// Nack implements jobq.Client.
func (c *Client) Nack(lease jobq.Lease) error {
	return c.settle(lease, "nack")
}

func (c *Client) settle(lease jobq.Lease, verb string) error {
	path := "/v1/topics/" + url.PathEscape(lease.Topic) + "/leases/" + url.PathEscape(lease.ID) + "/" + verb
	return c.doJSON(context.Background(), http.MethodPost, path, nil, nil, http.StatusNoContent)
}

// Extend implements jobq.Client.
func (c *Client) Extend(lease jobq.Lease, d time.Duration) (time.Time, error) {
	path := "/v1/topics/" + url.PathEscape(lease.Topic) + "/leases/" + url.PathEscape(lease.ID) + "/extend"
	req := extendRequestDTO{ByMs: ms(d)}
	var resp extendResponseDTO
	if err := c.doJSON(context.Background(), http.MethodPost, path, req, &resp, http.StatusOK); err != nil {
		return time.Time{}, err
	}
	return resp.Deadline, nil
}

// Topics lists known topics. Not part of jobq.Client — see package doc.
func (c *Client) Topics() ([]string, error) {
	var resp topicsResponseDTO
	if err := c.doJSON(context.Background(), http.MethodGet, "/v1/topics", nil, &resp, http.StatusOK); err != nil {
		return nil, err
	}
	return resp.Topics, nil
}

// TopicStats fetches one topic's Stats. Not part of jobq.Client.
func (c *Client) TopicStats(topic string) (jobq.Stats, error) {
	var resp jobq.Stats
	err := c.doJSON(context.Background(), http.MethodGet, "/v1/topics/"+url.PathEscape(topic)+"/stats", nil, &resp, http.StatusOK)
	return resp, err
}

// Stats fetches every topic's Stats in one call. Not part of jobq.Client.
func (c *Client) Stats() (map[string]jobq.Stats, error) {
	var resp statsResponseDTO
	if err := c.doJSON(context.Background(), http.MethodGet, "/v1/stats", nil, &resp, http.StatusOK); err != nil {
		return nil, err
	}
	return resp.Topics, nil
}

// Peek fetches up to limit ready jobs (and optionally in-flight jobs) from
// topic without leasing, mutating, or consuming an attempt. Not part of
// jobq.Client — see jobq.Queue.Peek's doc for why this exists.
func (c *Client) Peek(topic string, limit int, includeInflight bool) (ready, inflight []*jobq.Job, truncated bool, err error) {
	q := url.Values{}
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}
	if includeInflight {
		q.Set("include_inflight", "true")
	}
	path := "/v1/topics/" + url.PathEscape(topic) + "/jobs"
	if enc := q.Encode(); enc != "" {
		path += "?" + enc
	}
	var resp peekResponseDTO
	if err := c.doJSON(context.Background(), http.MethodGet, path, nil, &resp, http.StatusOK); err != nil {
		return nil, nil, false, err
	}
	ready = make([]*jobq.Job, len(resp.Ready))
	for i, d := range resp.Ready {
		ready[i] = dtoToJob(d)
	}
	inflight = make([]*jobq.Job, len(resp.Inflight))
	for i, d := range resp.Inflight {
		inflight[i] = dtoToJob(d)
	}
	return ready, inflight, resp.Truncated, nil
}

// ClusterStatus fetches one node's view of its cluster. Against a server
// that is not part of a cluster it returns an error.
func (c *Client) ClusterStatus() (ClusterStatus, error) {
	var resp ClusterStatus
	err := c.doJSON(context.Background(), http.MethodGet, "/v1/cluster", nil, &resp, http.StatusOK)
	return resp, err
}

// Health fetches server health.
func (c *Client) Health() (status string, durable bool, uptime time.Duration, err error) {
	var resp healthResponseDTO
	if err := c.doJSON(context.Background(), http.MethodGet, "/v1/health", nil, &resp, http.StatusOK); err != nil {
		return "", false, 0, err
	}
	return resp.Status, resp.Durable, time.Duration(resp.UptimeMs) * time.Millisecond, nil
}

// Recovery fetches the process-start RecoveryReport. It is a snapshot from
// when the server started, not live state.
func (c *Client) Recovery() (report jobq.RecoveryReport, startedAt time.Time, dir, sync string, err error) {
	var resp recoveryResponseDTO
	if err := c.doJSON(context.Background(), http.MethodGet, "/v1/recovery", nil, &resp, http.StatusOK); err != nil {
		return jobq.RecoveryReport{}, time.Time{}, "", "", err
	}
	return resp.Report, resp.StartedAt, resp.Dir, resp.Sync, nil
}

// errNoContent is an internal sentinel doJSON returns for a 204 response;
// callers that treat 204 as meaningful (Dequeue) check for it, others
// (Ack/Nack) simply pass wantStatus=204 and never see this value.
var errNoContent = errors.New("jobqhttp: no content")

// doJSON sends reqBody (or no body if nil) as JSON and decodes the response
// into respBody (skipped if nil) on wantStatus. Any other status is parsed
// as an errorEnvelope and translated via errorFromEnvelope; a 204 when
// wantStatus is 200 is reported as errNoContent for Dequeue to interpret.
//
// Against a cluster it retries as described on Client. Against a single
// server it makes exactly one request, as it always has.
func (c *Client) doJSON(ctx context.Context, method, path string, reqBody, respBody any, wantStatus int) error {
	var body []byte
	if reqBody != nil {
		b, err := json.Marshal(reqBody)
		if err != nil {
			return fmt.Errorf("jobqhttp: encoding request: %w", err)
		}
		body = b
	}
	deadline := time.Now().Add(c.retry)
	for hops := 0; ; hops++ {
		base := c.currentTarget()
		err := c.once(ctx, base, method, path, body, respBody, wantStatus)
		pause, again := c.reroute(base, err)
		// A hint is followed at once, but not forever: two nodes each
		// naming the other during an election must not spin. Everything
		// else backs off, so a cluster that is down is not also flooded.
		if pause > 0 || hops >= 3 {
			pause = retryPause << min(hops, 3)
		}
		if !again || ctx.Err() != nil || !time.Now().Add(pause).Before(deadline) {
			return err
		}
		if pause > 0 {
			select {
			case <-ctx.Done():
				return err
			case <-time.After(pause):
			}
		}
	}
}

const retryPause = 25 * time.Millisecond

func (c *Client) currentTarget() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.target
}

// reroute decides, from the outcome of a request sent to base, whether to
// try again and where. It retries only what a different node could answer
// differently: a refusal by a node that is not the leader, a node that is
// shutting down, and a node that could not be reached at all.
func (c *Client) reroute(base string, err error) (pause time.Duration, again bool) {
	if err == nil {
		return 0, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	var nl *jobq.NotLeaderError
	var transport *transportError
	switch {
	case errors.As(err, &nl):
		c.redirected = true
		// The hint is whatever the answering server chose to say. It is
		// followed only to an address the caller configured, so no server
		// can send the caller's requests, payloads included, somewhere
		// the caller never named.
		if hint := strings.TrimSuffix(nl.Leader, "/"); hint != base && c.configured(hint) {
			if c.target == base {
				c.target = hint
			}
			return 0, true
		}
		if len(c.addrs) < 2 {
			return 0, false // one address, and it is not the leader's
		}
		// No leader known yet (an election is under way), or one named at
		// an address this client was not given. Look elsewhere after a
		// pause.
	case errors.Is(err, jobq.ErrClosed), errors.As(err, &transport):
		if len(c.addrs) < 2 && !c.redirected {
			return 0, false // a single server: nowhere else to go
		}
	default:
		return 0, false
	}
	if c.target == base { // unless another goroutine already moved on
		c.target = c.addrs[c.next]
		c.next = (c.next + 1) % len(c.addrs)
	}
	return retryPause, true
}

// configured reports whether addr is one of the addresses the client was
// built with. The caller holds c.mu.
func (c *Client) configured(addr string) bool {
	for _, a := range c.addrs {
		if a == addr && a != "" {
			return true
		}
	}
	return false
}

// transportError marks a request that got no HTTP response: the server
// could not be reached, or the connection died first.
type transportError struct{ err error }

func (e *transportError) Error() string { return "jobqhttp: request failed: " + e.err.Error() }
func (e *transportError) Unwrap() error { return e.err }

// once makes a single request to one server.
func (c *Client) once(ctx context.Context, base, method, path string, body []byte, respBody any, wantStatus int) error {
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, base+path, rd)
	if err != nil {
		return fmt.Errorf("jobqhttp: building request: %w", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.httpc.Do(req)
	if err != nil {
		return &transportError{err}
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		if wantStatus == http.StatusNoContent {
			return nil
		}
		return errNoContent
	}
	if resp.StatusCode != wantStatus {
		var env errorEnvelope
		if derr := json.NewDecoder(resp.Body).Decode(&env); derr != nil {
			return fmt.Errorf("jobqhttp: unexpected status %d (body unreadable: %w)", resp.StatusCode, derr)
		}
		return errorFromEnvelope(resp.StatusCode, env)
	}
	if respBody == nil {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(respBody); err != nil {
		return fmt.Errorf("jobqhttp: decoding response: %w", err)
	}
	return nil
}

func ms(d time.Duration) int64 { return d.Milliseconds() }
