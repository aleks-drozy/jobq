package cluster

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aleks-drozy/jobq"
	"github.com/aleks-drozy/jobq/raft"
)

// The last-resort procedure in RUNBOOK.md: every node's disk is replaced
// with a copy of one node's, and the cluster is started again. It must
// come up agreeing on exactly what that node held.
//
// This is the only way back from losing a majority of disks, and it is
// only as good as the copy: whatever was committed after it was taken is
// gone. The test is that the procedure works, not that it is free.
func TestWholeClusterRestoredFromOneNodesDisk(t *testing.T) {
	ids := []raft.NodeID{1, 2, 3}
	root := t.TempDir()
	dir := func(id raft.NodeID) string { return filepath.Join(root, fmt.Sprintf("node%d", id)) }
	net := newTestNet()
	type running struct {
		n   *Node
		log *jobq.RaftLog
	}
	nodes := map[raft.NodeID]*running{}
	start := func(id raft.NodeID) {
		log, _, err := jobq.OpenRaftLog(dir(id))
		if err != nil {
			t.Fatal(err)
		}
		n, err := Start(Config{ID: id, Peers: ids, Storage: log, Transport: testLink{net},
			Tick: 5 * time.Millisecond, ElectionTicks: 10, HeartbeatTicks: 2,
			SnapshotEvery: 30, SnapshotKeep: 5})
		if err != nil {
			t.Fatal(err)
		}
		nodes[id] = &running{n, log}
		net.mu.Lock()
		net.nodes[id] = n
		net.mu.Unlock()
	}
	stopAll := func() {
		for id, r := range nodes {
			net.mu.Lock()
			delete(net.nodes, id)
			net.mu.Unlock()
			_ = r.n.Close()
			_ = r.log.Close()
		}
		nodes = map[raft.NodeID]*running{}
	}
	defer stopAll()
	leader := func() *Node {
		var lead *Node
		eventually(t, "a leader everyone has caught up with", func() bool {
			lead = nil
			for _, r := range nodes {
				if r.n.Status().State == raft.Leader {
					lead = r.n
				}
			}
			if lead == nil {
				return false
			}
			want := lead.Status()
			for _, r := range nodes {
				if st := r.n.Status(); st.Applied != want.LastIndex || st.Term != want.Term {
					return false
				}
			}
			return true
		})
		return lead
	}

	for _, id := range ids {
		start(id)
	}
	lead := leader()
	for i := 0; i < 100; i++ {
		if _, err := lead.Enqueue("work", []byte(fmt.Sprintf("job-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 40; i++ {
		_, lease, err := lead.Dequeue("work", time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		if err := lead.Ack(lease); err != nil {
			t.Fatal(err)
		}
	}
	once, err := lead.Enqueue("work", []byte("once"), jobq.WithRequestID("req-1"))
	if err != nil {
		t.Fatal(err)
	}
	leader()
	if lead.Status().SnapshotIndex == 0 {
		t.Fatal("the cluster never snapshotted; the copy would not include one")
	}
	want := lead.Stats("work")
	stopAll()

	// Nodes 2 and 3 lose everything and are given node 1's disk.
	for _, id := range ids[1:] {
		if err := os.RemoveAll(dir(id)); err != nil {
			t.Fatal(err)
		}
		copyTree(t, dir(1), dir(id))
	}
	for _, id := range ids {
		start(id)
	}
	lead = leader()
	for id, r := range nodes {
		if got := r.n.Stats("work"); got != want {
			t.Fatalf("node %d came back with %+v, want %+v", id, got, want)
		}
	}
	// It is a working cluster again, not just three matching disks: it
	// takes writes, and it still knows what it had already been asked.
	if again, err := lead.Enqueue("work", []byte("once"), jobq.WithRequestID("req-1")); err != nil || again != once {
		t.Fatalf("a retried enqueue after the restore: %s, %v; want the original %s", again, err, once)
	}
	if _, err := lead.Enqueue("work", []byte("new")); err != nil {
		t.Fatalf("enqueue after the restore: %v", err)
	}
	leader()
	for id, r := range nodes {
		if got := r.n.Stats("work").Enqueued; got != want.Enqueued+1 {
			t.Fatalf("node %d has %d jobs after one more enqueue, want %d", id, got, want.Enqueued+1)
		}
	}
}

func copyTree(t *testing.T, src, dst string) {
	t.Helper()
	err := filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(src, path)
		if info.IsDir() {
			return os.MkdirAll(filepath.Join(dst, rel), 0o700)
		}
		in, err := os.Open(path)
		if err != nil {
			return err
		}
		defer in.Close()
		out, err := os.Create(filepath.Join(dst, rel))
		if err != nil {
			return err
		}
		defer out.Close()
		_, err = io.Copy(out, in)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
}

// The other runbook procedure: one node's disk is lost and it comes back
// empty, under its old ID, while the other two are healthy and agree. It
// is brought up to date by snapshot and becomes a full member again.
//
// The runbook's precondition matters and is not checked by the software:
// the two survivors must hold the same log before the empty node starts.
// This test meets it by waiting for them to converge.
func TestNodeReplacedWithAnEmptyDiskRejoins(t *testing.T) {
	c := newTestCluster(t, 3).startAll()
	lead := c.leader()
	var lost raft.NodeID
	for _, id := range c.ids {
		if id != lead.cfg.ID {
			lost = id
		}
	}
	for i := 0; i < 60; i++ {
		mustEnqueue(t, c, "work", fmt.Sprintf("job-%d", i))
	}
	_, open, err := c.dequeue("work", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	c.stop(lost)
	for i := 60; i < 90; i++ {
		mustEnqueue(t, c, "work", fmt.Sprintf("job-%d", i))
	}
	c.converged() // the precondition: the survivors agree

	c.stores[lost] = raft.NewMemoryStorage()
	n := c.start(lost)
	c.converged()
	if n.Metrics().SnapshotsLoaded == 0 || n.Status().SnapshotIndex == 0 {
		t.Fatal("the empty node was not brought up to date by snapshot")
	}
	want := snapshot(c.leader())
	if got := snapshot(n); got != want {
		t.Fatalf("the replaced node differs from the leader.\n--- leader\n%s\n--- node %d\n%s", want, lost, got)
	}
	// It counts toward a majority again: with one of the others gone, the
	// cluster still takes writes, and the lease from before still settles.
	for _, id := range c.ids {
		if id != lost && id != c.leader().cfg.ID {
			c.stop(id)
			break
		}
	}
	mustEnqueue(t, c, "work", "after")
	if err := c.ack(open); err != nil {
		t.Fatalf("acking a lease issued before the node was replaced: %v", err)
	}
}
