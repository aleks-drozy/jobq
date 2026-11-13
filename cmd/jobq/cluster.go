package main

import (
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/aleks-drozy/jobq/jobqhttp"
)

// cmdCluster asks every address in --addr for its own view of the cluster
// and prints them side by side. Unlike every other command it does not
// follow the leader: the point is to see what each node believes, and
// which of them cannot be reached at all.
//
// It exits 0 only if the nodes that answered agree on one leader and that
// leader is among them.
func cmdCluster(g *globalFlags, args []string, stdout, stderr io.Writer) int {
	fs := newSubFlagSet("cluster", g, stderr)
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	timeout := g.timeout
	if timeout <= 0 || timeout > 5*time.Second {
		timeout = 5 * time.Second // a node that is down should not hold the answer up
	}

	type row struct {
		Addr   string                  `json:"addr"`
		Status *jobqhttp.ClusterStatus `json:"status,omitempty"`
		Error  string                  `json:"error,omitempty"`
	}
	var rows []row
	leaders := map[uint64]bool{}
	answered := map[uint64]bool{}
	for _, addr := range strings.Split(g.addr, ",") {
		addr = strings.TrimSuffix(strings.TrimSpace(addr), "/")
		if addr == "" {
			continue
		}
		// One address, no retries: this node's answer or its absence.
		c := jobqhttp.NewClient(addr, jobqhttp.WithTimeout(timeout), jobqhttp.WithRetryBudget(0))
		st, err := c.ClusterStatus()
		if err != nil {
			rows = append(rows, row{Addr: addr, Error: err.Error()})
			continue
		}
		rows = append(rows, row{Addr: addr, Status: &st})
		leaders[st.Leader] = true
		answered[st.ID] = true
	}
	if len(rows) == 0 {
		return fail(stderr, "cluster: --addr names no node")
	}

	healthy := len(leaders) == 1
	for id := range leaders {
		// Agreeing that nobody leads, or on a leader who did not answer,
		// is not a working cluster from where this command stands.
		if id == 0 || !answered[id] {
			healthy = false
		}
	}

	if g.json {
		if code := printJSON(stdout, stderr, map[string]any{"healthy": healthy, "nodes": rows}); code != exitOK {
			return code
		}
	} else {
		tw := tabwriter.NewWriter(stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "ADDR\tID\tSTATE\tTERM\tLEADER\tCOMMIT\tAPPLIED\tLAST\tSNAPSHOT\tFIRST")
		for _, r := range rows {
			if r.Status == nil {
				fmt.Fprintf(tw, "%s\t-\tunreachable\t-\t-\t-\t-\t-\t-\t-\n", r.Addr)
				continue
			}
			s := r.Status
			leader := "none"
			if s.Leader != 0 {
				leader = fmt.Sprint(s.Leader)
			}
			fmt.Fprintf(tw, "%s\t%d\t%s\t%d\t%s\t%d\t%d\t%d\t%d\t%d\n",
				r.Addr, s.ID, s.State, s.Term, leader, s.Commit, s.Applied, s.LastIndex, s.SnapshotIndex, s.FirstIndex)
		}
		if err := tw.Flush(); err != nil {
			return fail(stderr, "cluster: %v", err)
		}
		for _, r := range rows {
			if r.Status == nil {
				fmt.Fprintf(stdout, "%s: %s\n", r.Addr, r.Error)
			}
		}
	}
	if !healthy {
		return exitError
	}
	return exitOK
}
