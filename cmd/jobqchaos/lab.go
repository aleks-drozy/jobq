package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aleks-drozy/jobq"
	"github.com/aleks-drozy/jobq/jobqhttp"
)

// node is one member of the cluster under test, seen from outside.
type node struct {
	id  uint64
	url string
	// direct talks to this node and no other, for asking it questions.
	direct *jobqhttp.Client
}

func newNode(id uint64, url string) *node {
	return &node{id: id, url: url,
		direct: jobqhttp.NewClient(url, jobqhttp.WithRetryBudget(0), jobqhttp.WithTimeout(time.Second))}
}

// injector does something to a node. It returns once the thing is done.
type injector interface {
	Do(verb string, id uint64) error
}

// hookInjector runs an executable as HOOK VERB NODE-ID.
type hookInjector struct {
	path    string
	timeout time.Duration
}

func (h hookInjector) Do(verb string, id uint64) error {
	ctx, cancel := context.WithTimeout(context.Background(), h.timeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, h.path, verb, strconv.FormatUint(id, 10)).CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s %s %d: %w: %s", h.path, verb, id, err, bytes.TrimSpace(out))
	}
	return nil
}

// lab is the cluster and the means of breaking it.
type lab struct {
	nodes []*node
	inj   injector
	out   io.Writer
}

func (l *lab) ids() []uint64 {
	ids := make([]uint64, len(l.nodes))
	for i, n := range l.nodes {
		ids[i] = n.id
	}
	return ids
}

func (l *lab) addrs() string {
	urls := make([]string, len(l.nodes))
	for i, n := range l.nodes {
		urls[i] = n.url
	}
	return strings.Join(urls, ",")
}

// look asks every node for its own view, at once. A node that does not
// answer within a second is simply missing from the result.
func (l *lab) look() map[uint64]jobqhttp.ClusterStatus {
	var (
		mu  sync.Mutex
		wg  sync.WaitGroup
		out = map[uint64]jobqhttp.ClusterStatus{}
	)
	for _, n := range l.nodes {
		wg.Add(1)
		go func(n *node) {
			defer wg.Done()
			st, err := n.direct.ClusterStatus()
			if err != nil {
				return
			}
			mu.Lock()
			out[n.id] = st
			mu.Unlock()
		}(n)
	}
	wg.Wait()
	return out
}

// agreed reports the leader, if exactly one node in sts says it leads and
// every node in sts follows it in its term.
func agreed(sts map[uint64]jobqhttp.ClusterStatus) (leader, term uint64, ok bool) {
	for id, st := range sts {
		if st.State != "leader" {
			continue
		}
		if leader != 0 {
			return 0, 0, false
		}
		leader, term = id, st.Term
	}
	if leader == 0 {
		return 0, 0, false
	}
	for _, st := range sts {
		if st.Leader != leader || st.Term != term {
			return 0, 0, false
		}
	}
	return leader, term, true
}

// catchUpSlack is how far behind the leader's commit index a follower may
// be and still count as caught up. Under load a healthy follower is always
// a few entries behind.
const catchUpSlack = 256

// waitHealthy waits until every node answers, they agree on a leader, and
// none is more than catchUpSlack entries behind it.
func (l *lab) waitHealthy(ctx context.Context, timeout time.Duration) (leader, term uint64, err error) {
	deadline := time.Now().Add(timeout)
	for {
		sts := l.look()
		if len(sts) == len(l.nodes) {
			if leader, term, ok := agreed(sts); ok {
				caught := true
				for _, st := range sts {
					if st.Applied+catchUpSlack < sts[leader].Commit {
						caught = false
					}
				}
				if caught {
					return leader, term, nil
				}
			}
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			return 0, 0, fmt.Errorf("cluster not whole after %s: %s", timeout, describe(sts))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// describe renders what each node said, for an error or a line of evidence.
func describe(sts map[uint64]jobqhttp.ClusterStatus) string {
	if len(sts) == 0 {
		return "no node answered"
	}
	ids := make([]uint64, 0, len(sts))
	for id := range sts {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	parts := make([]string, len(ids))
	for i, id := range ids {
		st := sts[id]
		parts[i] = fmt.Sprintf("node %d %s term %d (leader %d, applied %d)", id, st.State, st.Term, st.Leader, st.Applied)
	}
	return strings.Join(parts, "; ")
}

// watchNewLeader polls until some node says it leads in a term above term,
// and sends the time it was seen. It sends the zero time if stop closes
// first. The time is when the answer arrived, so it is late by up to one
// poll: an upper bound on when the election finished.
func (l *lab) watchNewLeader(term uint64, stop <-chan struct{}) <-chan time.Time {
	ch := make(chan time.Time, 1)
	go func() {
		for {
			select {
			case <-stop:
				ch <- time.Time{}
				return
			default:
			}
			for _, st := range l.look() {
				if st.State == "leader" && st.Term > term {
					ch <- time.Now()
					return
				}
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()
	return ch
}

// drain waits until every node reports the topic and its dead-letter topic
// empty, with nothing in flight, and all of them report the same counts,
// fifteen times running. It returns the last counts it read either way.
func (l *lab) drain(topic string, timeout time.Duration) (main, dead jobq.Stats, err error) {
	type counts struct{ main, dead jobq.Stats }
	read := func(n *node) (counts, error) {
		m, err := n.direct.TopicStats(topic)
		if err != nil {
			return counts{}, err
		}
		d, err := n.direct.TopicStats(jobq.DeadLetterTopic(topic))
		return counts{m, d}, err
	}
	var last counts
	deadline := time.Now().Add(timeout)
	for quiet := 0; quiet < 15; {
		if time.Now().After(deadline) {
			return last.main, last.dead, fmt.Errorf("queues never drained in %s; last counts %+v", timeout, last)
		}
		time.Sleep(20 * time.Millisecond)
		first, err := read(l.nodes[0])
		ok := err == nil && first.main.Ready == 0 && first.main.InFlight == 0 &&
			first.dead.Ready == 0 && first.dead.InFlight == 0
		for _, n := range l.nodes[1:] {
			other, err := read(n)
			ok = ok && err == nil && other == first
		}
		if err == nil {
			last = first
		}
		if ok {
			quiet++
		} else {
			quiet = 0
		}
	}
	return last.main, last.dead, nil
}
