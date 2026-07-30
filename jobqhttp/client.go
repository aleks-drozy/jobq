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
	"time"

	"github.com/aleks-drozy/jobq"
)

// Client talks to a jobqd (or anything implementing the same wire format)
// and implements jobq.Client, so callers can use a remote queue exactly like
// a local *jobq.Queue for the five lifecycle methods. roundtrip_test.go
// pins that the two behave identically, sentinel errors included.
type Client struct {
	baseURL string
	httpc   *http.Client
}

// ClientOption configures a Client.
type ClientOption func(*Client)

// WithHTTPClient replaces the underlying http.Client (for custom
// transports, timeouts, or TLS config).
func WithHTTPClient(c *http.Client) ClientOption {
	return func(cl *Client) { cl.httpc = c }
}

// NewClient builds a Client pointed at baseURL (e.g. "http://127.0.0.1:8080").
func NewClient(baseURL string, opts ...ClientOption) *Client {
	c := &Client{baseURL: strings.TrimSuffix(baseURL, "/"), httpc: &http.Client{Timeout: 10 * time.Second}}
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
func (c *Client) doJSON(ctx context.Context, method, path string, reqBody, respBody any, wantStatus int) error {
	var body io.Reader
	if reqBody != nil {
		b, err := json.Marshal(reqBody)
		if err != nil {
			return fmt.Errorf("jobqhttp: encoding request: %w", err)
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, body)
	if err != nil {
		return fmt.Errorf("jobqhttp: building request: %w", err)
	}
	if reqBody != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.httpc.Do(req)
	if err != nil {
		return fmt.Errorf("jobqhttp: request failed: %w", err)
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
