package main

// Cluster mode: jobqd as one node of a replicated queue. Selected by
// -peers. The node listens twice: -addr serves the same client API a
// single node does, and -peer-addr carries Raft traffic between nodes.
//
// Neither listener authenticates anyone. The peer port in particular must
// be reachable only by the other nodes: whoever can reach it can speak
// Raft to this node.

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"sort"
	"strconv"
	"strings"

	"github.com/aleks-drozy/jobq"
	"github.com/aleks-drozy/jobq/cluster"
	"github.com/aleks-drozy/jobq/jobqhttp"
	"github.com/aleks-drozy/jobq/raft"
)

// parseNodeMap reads "1=http://a:9090,2=http://b:9090" into a map.
func parseNodeMap(flagName, s string) (map[raft.NodeID]string, error) {
	out := map[raft.NodeID]string{}
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, url, ok := strings.Cut(part, "=")
		n, err := strconv.ParseUint(strings.TrimSpace(id), 10, 64)
		url = strings.TrimSpace(url)
		if !ok || err != nil || n == 0 || url == "" {
			return nil, fmt.Errorf("-%s: %q is not ID=URL with a positive numeric ID", flagName, part)
		}
		if !strings.HasPrefix(url, "http://") && !strings.HasPrefix(url, "https://") {
			return nil, fmt.Errorf("-%s: %q must start with http:// or https://", flagName, url)
		}
		if _, dup := out[raft.NodeID(n)]; dup {
			return nil, fmt.Errorf("-%s: node %d is listed twice", flagName, n)
		}
		out[raft.NodeID(n)] = url
	}
	return out, nil
}

// clusterConfig is the validated cluster half of the flags.
type clusterConfig struct {
	id        raft.NodeID
	ids       []raft.NodeID          // every node, sorted
	peers     map[raft.NodeID]string // peer URLs
	advertise map[raft.NodeID]string // client URLs, may be empty
}

func parseClusterFlags(c config) (clusterConfig, error) {
	var cc clusterConfig
	peers, err := parseNodeMap("peers", c.peers)
	if err != nil {
		return cc, err
	}
	advertise, err := parseNodeMap("advertise", c.advertise)
	if err != nil {
		return cc, err
	}
	if c.id == 0 {
		return cc, errors.New("-id is required with -peers")
	}
	if _, ok := peers[raft.NodeID(c.id)]; !ok {
		return cc, fmt.Errorf("-peers does not list this node (-id %d)", c.id)
	}
	for id := range advertise {
		if _, ok := peers[id]; !ok {
			return cc, fmt.Errorf("-advertise names node %d, which -peers does not", id)
		}
	}
	if c.dir == "" {
		// Raft's safety rests on a node remembering its vote and its log
		// across a restart. A node with no disk that restarts can vote
		// twice in one term.
		return cc, errors.New("-dir is required with -peers: a replicated node must keep its log on disk")
	}
	cc.id, cc.peers, cc.advertise = raft.NodeID(c.id), peers, advertise
	for id := range peers {
		cc.ids = append(cc.ids, id)
	}
	sort.Slice(cc.ids, func(i, j int) bool { return cc.ids[i] < cc.ids[j] })
	return cc, nil
}

func runCluster(ctx context.Context, c config, logger *log.Logger) error {
	cc, err := parseClusterFlags(c)
	if err != nil {
		return err
	}
	policy, err := parseSyncPolicy(c.sync)
	if err != nil {
		return err
	}
	if policy != jobq.SyncAlways {
		logger.Printf("WARNING: -sync=%s on a replicated node: a crash can lose a vote or an "+
			"acknowledged entry, which Raft assumes cannot happen", c.sync)
	}

	rlog, report, err := jobq.OpenRaftLog(c.dir, jobq.WithSync(policy), jobq.WithSyncEvery(c.syncEvery))
	if err != nil {
		return fmt.Errorf("opening %s: %w", c.dir, err)
	}
	defer rlog.Close()
	logger.Printf("raft log: %+v", report)

	// Both listeners are opened before the node starts, so a port that is
	// taken fails the process instead of leaving a node that votes but
	// cannot be reached.
	peerLn, err := net.Listen("tcp", c.peerAddr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", c.peerAddr, err)
	}
	defer peerLn.Close()
	ln, err := net.Listen("tcp", c.addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", c.addr, err)
	}
	defer ln.Close()

	transport := cluster.NewHTTPTransport(cc.id, cc.peers)
	defer transport.Close()
	node, err := cluster.Start(cluster.Config{
		ID:         cc.id,
		Peers:      cc.ids,
		Storage:    rlog,
		Transport:  transport,
		Tick:       c.tick,
		ClientAddr: func(id raft.NodeID) string { return cc.advertise[id] },
		Logger:     logger,
	})
	if err != nil {
		return err
	}
	defer node.Close()
	logger.Printf("node %d of %d: clients on %s, peers on %s", cc.id, len(cc.ids), ln.Addr(), peerLn.Addr())

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	handler := jobqhttp.NewHandler(node,
		jobqhttp.WithMaxPayload(c.maxPayload),
		jobqhttp.WithMaxWait(c.maxWait),
		jobqhttp.WithLogger(logger),
	)
	errc := make(chan error, 2)
	go func() { errc <- serve(ctx, peerLn, cluster.PeerHandler(node), c.shutdownTimeout, logger) }()
	go func() { errc <- serve(ctx, ln, handler, c.shutdownTimeout, logger) }()

	// Stop on a signal, on either listener failing, or on the node itself
	// giving up (it does that when it cannot write to disk). Whichever
	// comes first takes the rest down with it.
	var first error
	select {
	case first = <-errc:
		cancel()
		first = errors.Join(first, <-errc)
	case <-node.Done():
		first = node.Err()
		cancel()
		<-errc
		<-errc
	}
	return first
}
