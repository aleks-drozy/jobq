package main

// The harness run end to end against a real cluster: three replicated
// nodes in this process, each with its own directory and its own two
// listeners on loopback, the way jobqd runs them.
//
// This tests the harness, not the queue. The only faults it can inject
// here are the two that need nothing but a process to stop: a node is
// "killed" by closing its listeners and its node, which is abrupt but is
// not the power cut that SIGKILL is, and nothing here can cut a network.
// Cold kills are the crash harness's job (cmd/jobqd); partitions and a slow
// network need chaos/netns.sh and root.

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aleks-drozy/jobq"
	"github.com/aleks-drozy/jobq/cluster"
	"github.com/aleks-drozy/jobq/jobqhttp"
	"github.com/aleks-drozy/jobq/raft"
)

// procLab is three nodes in this process, and an injector over them.
type procLab struct {
	ids        []raft.NodeID
	dir        map[raft.NodeID]string
	clientAddr map[raft.NodeID]string
	peerAddr   map[raft.NodeID]string
	clientURL  map[raft.NodeID]string
	peerURL    map[raft.NodeID]string

	mu   sync.Mutex
	stop map[raft.NodeID]func()
	log  []string // every verb applied, in order
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

func newProcLab(t *testing.T) *procLab {
	t.Helper()
	p := &procLab{
		ids: []raft.NodeID{1, 2, 3},
		dir: map[raft.NodeID]string{}, clientAddr: map[raft.NodeID]string{}, peerAddr: map[raft.NodeID]string{},
		clientURL: map[raft.NodeID]string{}, peerURL: map[raft.NodeID]string{}, stop: map[raft.NodeID]func(){},
	}
	root := t.TempDir()
	for _, id := range p.ids {
		p.dir[id] = filepath.Join(root, fmt.Sprintf("node%d", id))
		p.clientAddr[id], p.peerAddr[id] = freeAddr(t), freeAddr(t)
		p.clientURL[id], p.peerURL[id] = "http://"+p.clientAddr[id], "http://"+p.peerAddr[id]
	}
	for _, id := range p.ids {
		if err := p.start(id); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		for _, id := range p.ids {
			p.kill(id)
		}
	})
	return p
}

// listen retries briefly: the port was this node's a moment ago, and the
// operating system may not have let go of it yet.
func listen(addr string) (net.Listener, error) {
	var err error
	for i := 0; i < 100; i++ {
		var ln net.Listener
		if ln, err = net.Listen("tcp", addr); err == nil {
			return ln, nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return nil, err
}

func (p *procLab) start(id raft.NodeID) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stop[id] != nil {
		return fmt.Errorf("node %d is already running", id)
	}
	// Whoever held this directory was stopped by kill below, and is gone.
	if err := os.Remove(filepath.Join(p.dir[id], "jobq.lock")); err != nil && !os.IsNotExist(err) {
		return err
	}
	rlog, _, err := jobq.OpenRaftLog(p.dir[id])
	if err != nil {
		return err
	}
	peerLn, err := listen(p.peerAddr[id])
	if err != nil {
		rlog.Close()
		return err
	}
	ln, err := listen(p.clientAddr[id])
	if err != nil {
		peerLn.Close()
		rlog.Close()
		return err
	}
	transport := cluster.NewHTTPTransport(id, p.peerURL)
	n, err := cluster.Start(cluster.Config{
		ID: id, Peers: p.ids, Storage: rlog, Transport: transport,
		ClientAddr: func(id raft.NodeID) string { return p.clientURL[id] },
	})
	if err != nil {
		ln.Close()
		peerLn.Close()
		transport.Close()
		rlog.Close()
		return err
	}
	peerSrv := &http.Server{Handler: cluster.PeerHandler(n)}
	srv := &http.Server{Handler: jobqhttp.NewHandler(n)}
	go peerSrv.Serve(peerLn)
	go srv.Serve(ln)
	p.stop[id] = func() {
		srv.Close()
		peerSrv.Close()
		n.Close()
		transport.Close()
		rlog.Close()
	}
	return nil
}

func (p *procLab) kill(id raft.NodeID) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if stop := p.stop[id]; stop != nil {
		stop()
		p.stop[id] = nil
	}
}

func (p *procLab) Do(verb string, id uint64) error {
	p.mu.Lock()
	p.log = append(p.log, fmt.Sprintf("%s %d", verb, id))
	p.mu.Unlock()
	switch verb {
	case "kill":
		p.kill(raft.NodeID(id))
		return nil
	case "start":
		return p.start(raft.NodeID(id))
	}
	return fmt.Errorf("verb %q needs a real network to break; this lab has none", verb)
}

func (p *procLab) lab(out *bytes.Buffer) *lab {
	l := &lab{inj: p, out: out}
	for _, id := range p.ids {
		l.nodes = append(l.nodes, newNode(uint64(id), p.clientURL[id]))
	}
	return l
}

func TestHarnessAgainstARealClusterAcrossNodeKills(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos harness end-to-end test skipped under -short")
	}
	p := newProcLab(t)
	var out bytes.Buffer
	l := p.lab(&out)
	c := config{runs: 2, hold: 1500 * time.Millisecond, settle: 500 * time.Millisecond, drain: time.Minute,
		producers: 2, consumers: 3, pace: 5 * time.Millisecond, seed: 1}
	fs, err := parseFaults("leader-kill,follower-kill")
	if err != nil {
		t.Fatal(err)
	}

	rep := runMatrix(context.Background(), l, fs, c)
	t.Logf("harness output:\n%s\n%s", out.String(), rep.markdown())
	if !rep.ok() {
		t.Fatal("the harness reported a failure against a healthy cluster")
	}

	for _, cell := range rep.Cells {
		if len(cell.Runs) != c.runs {
			t.Fatalf("%s: %d runs, want %d", cell.Fault, len(cell.Runs), c.runs)
		}
		if cell.Contract.Acknowledged < 50 {
			t.Errorf("%s: only %d enqueues were acknowledged: the run proves nothing", cell.Fault, cell.Contract.Acknowledged)
		}
		for _, r := range cell.Runs {
			if r.FirstWriteMs < 0 {
				t.Errorf("%s run %d: no write was acknowledged after the fault", cell.Fault, r.Run)
			}
			if r.LongestStallMs <= 0 || r.WholeAgainMs <= 0 {
				t.Errorf("%s run %d: stall %v ms, whole again %v ms: not measured", cell.Fault, r.Run, r.LongestStallMs, r.WholeAgainMs)
			}
		}
	}
	lk, fk := rep.Cells[0], rep.Cells[1]
	for _, r := range lk.Runs {
		// Killing the leader must cost a leader: a later term, led by
		// someone else, seen while the old one was down.
		if r.TermAfter <= r.TermBefore || r.NewLeaderMs < 0 {
			t.Errorf("leader-kill run %d: term %d -> %d, new leader after %v ms: no election was seen",
				r.Run, r.TermBefore, r.TermAfter, r.NewLeaderMs)
		}
		if len(r.Targets) != 1 || r.Targets[0] != r.LeaderBefore {
			t.Errorf("leader-kill run %d: hit %v, but node %d led", r.Run, r.Targets, r.LeaderBefore)
		}
	}
	for _, r := range fk.Runs {
		if len(r.Targets) != 1 || r.Targets[0] == r.LeaderBefore {
			t.Errorf("follower-kill run %d: hit %v, and node %d led", r.Run, r.Targets, r.LeaderBefore)
		}
	}
	// Every kill was followed by a start of the same node: nothing is left
	// broken behind the harness.
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.log) != 4*c.runs {
		t.Fatalf("injector saw %v, want %d verbs", p.log, 4*c.runs)
	}
	for i := 0; i < len(p.log); i += 2 {
		k, s := strings.Fields(p.log[i]), strings.Fields(p.log[i+1])
		if k[0] != "kill" || s[0] != "start" || k[1] != s[1] {
			t.Errorf("injector saw %q then %q: a kill must be undone by a start of the same node", p.log[i], p.log[i+1])
		}
	}
}

// brokenInjector reports success and does nothing, as a hook would whose
// commands silently failed to take effect.
type brokenInjector struct{}

func (brokenInjector) Do(string, uint64) error { return nil }

// A fault that is never applied must fail the run. Otherwise a hook that
// does nothing would produce a perfect report.
func TestHarnessFailsWhenTheFaultIsNotApplied(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos harness end-to-end test skipped under -short")
	}
	p := newProcLab(t)
	var out bytes.Buffer
	l := p.lab(&out)
	l.inj = brokenInjector{}
	c := config{runs: 1, hold: 500 * time.Millisecond, settle: 300 * time.Millisecond, drain: time.Minute,
		producers: 2, consumers: 3, pace: 5 * time.Millisecond, seed: 1}

	rep := runMatrix(context.Background(), l, faults, c)
	if rep.ok() {
		t.Fatalf("a hook that did nothing produced a passing report:\n%s", rep.markdown())
	}
	for _, cell := range rep.Cells {
		for _, r := range cell.Runs {
			if r.Seen {
				t.Errorf("%s: claimed the fault was seen (%s)", cell.Fault, r.Evidence)
			}
		}
		// The cluster was never touched, so the contract must still hold:
		// the failure is the unseen fault and nothing else.
		if len(cell.Contract.Violations) != 0 {
			t.Errorf("%s: %v", cell.Fault, cell.Contract.Violations)
		}
	}
	if md := rep.markdown(); !strings.Contains(md, "fault not seen") {
		t.Errorf("the report does not say why it failed:\n%s", md)
	}
}
