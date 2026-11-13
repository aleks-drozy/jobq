package jobqhttp

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aleks-drozy/jobq/raft"
)

// GET /metrics, in the Prometheus text exposition format, written by hand:
// the format is a few lines of text and the root module takes no
// dependencies.
//
// Every number is this node's own view, read without consulting anyone. A
// node cut off from the others reports what it last knew, which is the
// point: jobq_raft_has_leader going to 0 on a node is how the cut shows.

type metricsWriter struct{ b strings.Builder }

func (m *metricsWriter) family(name, kind, help string) {
	fmt.Fprintf(&m.b, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, kind)
}

func (m *metricsWriter) value(name, labels string, v float64) {
	if labels != "" {
		labels = "{" + labels + "}"
	}
	fmt.Fprintf(&m.b, "%s%s %s\n", name, labels, strconv.FormatFloat(v, 'g', -1, 64))
}

func (m *metricsWriter) simple(name, kind, help string, v float64) {
	m.family(name, kind, help)
	m.value(name, "", v)
}

// label renders one label pair, escaped as the format requires.
func label(name, value string) string {
	value = strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(value)
	return name + `="` + value + `"`
}

func boolValue(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

func (h *handler) handleMetrics(w http.ResponseWriter, r *http.Request) {
	var m metricsWriter
	m.simple("jobq_uptime_seconds", "gauge", "Seconds since this process started serving.", time.Since(h.opts.startedAt).Seconds())

	topics := h.q.Topics()
	sort.Strings(topics)
	stats := make([]struct{ ready, inFlight, enqueued, acked, dead, dropped float64 }, len(topics))
	for i, t := range topics {
		st := h.q.Stats(t)
		stats[i].ready, stats[i].inFlight = float64(st.Ready), float64(st.InFlight)
		stats[i].enqueued, stats[i].acked = float64(st.Enqueued), float64(st.Acked)
		stats[i].dead, stats[i].dropped = float64(st.DeadLetters), float64(st.Dropped)
	}
	perTopic := func(name, kind, help string, pick func(i int) float64) {
		m.family(name, kind, help)
		for i, t := range topics {
			m.value(name, label("topic", t), pick(i))
		}
	}
	perTopic("jobq_topic_ready", "gauge", "Jobs waiting to be delivered, delayed ones included.", func(i int) float64 { return stats[i].ready })
	perTopic("jobq_topic_in_flight", "gauge", "Jobs out on a lease.", func(i int) float64 { return stats[i].inFlight })
	perTopic("jobq_topic_enqueued_total", "counter", "Jobs ever added to the topic.", func(i int) float64 { return stats[i].enqueued })
	perTopic("jobq_topic_acked_total", "counter", "Jobs completed.", func(i int) float64 { return stats[i].acked })
	perTopic("jobq_topic_dead_lettered_total", "counter", "Jobs moved to the topic's dead-letter topic.", func(i int) float64 { return stats[i].dead })
	perTopic("jobq_topic_dropped_total", "counter", "Jobs destroyed after exhausting their attempts inside a dead-letter topic.", func(i int) float64 { return stats[i].dropped })

	if cb, ok := h.q.(clusterBackend); ok {
		st, cm := cb.Status(), cb.Metrics()
		m.simple("jobq_raft_term", "gauge", "This node's current term.", float64(st.Term))
		m.simple("jobq_raft_is_leader", "gauge", "1 if this node believes it leads.", boolValue(st.State == raft.Leader))
		m.simple("jobq_raft_has_leader", "gauge", "1 if this node knows of a leader in its term, itself included. 0 during an election or when cut off.", boolValue(st.Leader != raft.None))
		m.simple("jobq_raft_leader_id", "gauge", "ID of the node this one believes leads, 0 if none.", float64(st.Leader))
		m.simple("jobq_raft_leader_changes_total", "counter", "Times this node learned of a leader different from the last one it knew.", float64(cm.LeaderChanges))
		m.simple("jobq_raft_commit_index", "gauge", "Highest log index known to be committed.", float64(st.Commit))
		m.simple("jobq_raft_applied_index", "gauge", "Highest log index this node's queue reflects.", float64(st.Applied))
		m.simple("jobq_raft_last_index", "gauge", "Index of the last entry in this node's log.", float64(st.LastIndex))
		m.simple("jobq_raft_first_index", "gauge", "Index of the oldest entry this node still holds.", float64(st.FirstIndex))
		m.simple("jobq_raft_snapshot_index", "gauge", "Log index this node's snapshot covers, 0 if it has none.", float64(st.SnapshotIndex))

		// Lag is only knowable on the leader, and only for followers.
		m.family("jobq_raft_follower_lag_entries", "gauge", "On the leader: log entries each follower is behind the leader's last entry.")
		peers := make([]raft.NodeID, 0, len(st.Match))
		for id := range st.Match {
			peers = append(peers, id)
		}
		sort.Slice(peers, func(i, j int) bool { return peers[i] < peers[j] })
		for _, id := range peers {
			lag := float64(0)
			if match := st.Match[id]; st.LastIndex > match {
				lag = float64(st.LastIndex - match)
			}
			m.value("jobq_raft_follower_lag_entries", label("peer", strconv.FormatUint(uint64(id), 10)), lag)
		}

		const hist = "jobq_raft_commit_duration_seconds"
		m.family(hist, "histogram", "On the leader: time from proposing an operation to applying it, covering the fsync, a majority's round trip and the queue's own work.")
		for i, bound := range cm.Commit.Bounds {
			m.value(hist+"_bucket", label("le", strconv.FormatFloat(bound.Seconds(), 'g', -1, 64)), float64(cm.Commit.Counts[i]))
		}
		m.value(hist+"_bucket", label("le", "+Inf"), float64(cm.Commit.Count))
		m.value(hist+"_sum", "", cm.Commit.Sum.Seconds())
		m.value(hist+"_count", "", float64(cm.Commit.Count))

		m.simple("jobq_snapshots_total", "counter", "Snapshots this node has taken of its own queue.", float64(cm.Snapshots))
		m.simple("jobq_snapshots_loaded_total", "counter", "Snapshots this node has loaded: its own at a restart, or one sent by a leader.", float64(cm.SnapshotsLoaded))
		m.simple("jobq_snapshot_last_bytes", "gauge", "Size of the newest snapshot this node took.", float64(cm.LastSnapshotBytes))
		m.simple("jobq_snapshot_last_duration_seconds", "gauge", "How long the newest snapshot took. The node does nothing else meanwhile.", cm.LastSnapshotTook.Seconds())
		m.simple("jobq_deduplicated_enqueues_total", "counter", "Enqueues recognised by request ID as already done.", float64(cm.DedupedEnqueues))
	}
	if h.opts.diskUsage != nil {
		logBytes, snapBytes := h.opts.diskUsage()
		m.simple("jobq_raft_log_bytes", "gauge", "Bytes the Raft log's segments take on disk.", float64(logBytes))
		m.simple("jobq_raft_snapshot_file_bytes", "gauge", "Bytes the stored snapshot takes on disk.", float64(snapBytes))
	}

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = w.Write([]byte(m.b.String()))
}
