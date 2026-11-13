package main

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aleks-drozy/jobq/cluster"
	"github.com/aleks-drozy/jobq/jobqhttp"
	"github.com/aleks-drozy/jobq/raft"
)

func TestCLICluster(t *testing.T) {
	n, err := cluster.Start(cluster.Config{ID: 1, Peers: []raft.NodeID{1}, Storage: raft.NewMemoryStorage(),
		Tick: 2 * time.Millisecond, ElectionTicks: 5, HeartbeatTicks: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	srv := httptest.NewServer(jobqhttp.NewHandler(n))
	defer srv.Close()
	for deadline := time.Now().Add(10 * time.Second); n.Status().State != raft.Leader; {
		if time.Now().After(deadline) {
			t.Fatal("the node never led")
		}
		time.Sleep(time.Millisecond)
	}
	dead := httptest.NewServer(nil)
	deadURL := dead.URL
	dead.Close()
	_, plain := newCLITestServer(t)

	out, _, code := runCLI(srv.URL, "cluster")
	if code != exitOK || !strings.Contains(out, "leader") || !strings.Contains(out, "SNAPSHOT") {
		t.Fatalf("one healthy node: exit %d\n%s", code, out)
	}

	// A node that is down is listed as such and does not make a cluster
	// with a leader unhealthy.
	out, _, code = runCLI(srv.URL+","+deadURL, "cluster")
	if code != exitOK || !strings.Contains(out, "unreachable") {
		t.Fatalf("a healthy node and a dead one: exit %d\n%s", code, out)
	}

	// Nobody answering is not healthy, and neither is a server that is
	// not part of a cluster at all.
	if out, _, code := runCLI(deadURL, "cluster"); code != exitError {
		t.Fatalf("only a dead node: exit %d\n%s", code, out)
	}
	if out, _, code := runCLI(plain, "cluster"); code != exitError || !strings.Contains(out, "not_clustered") {
		t.Fatalf("a single-node server: exit %d\n%s", code, out)
	}

	out, _, code = runCLI(srv.URL+","+deadURL, "--json", "cluster")
	var got struct {
		Healthy bool `json:"healthy"`
		Nodes   []struct {
			Addr   string                  `json:"addr"`
			Status *jobqhttp.ClusterStatus `json:"status"`
			Error  string                  `json:"error"`
		} `json:"nodes"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil || code != exitOK {
		t.Fatalf("--json: exit %d, %v\n%s", code, err, out)
	}
	if !got.Healthy || len(got.Nodes) != 2 || got.Nodes[0].Status == nil || got.Nodes[0].Status.State != "leader" || got.Nodes[1].Error == "" {
		t.Fatalf("--json: %+v", got)
	}
}
