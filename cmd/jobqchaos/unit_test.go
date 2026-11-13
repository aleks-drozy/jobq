package main

import (
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/aleks-drozy/jobq"
	"github.com/aleks-drozy/jobq/jobqhttp"
)

// A clean run: three jobs enqueued, delivered and acked, one of them twice
// delivered. The nodes agree.
func cleanAccounting() (*accounting, jobq.Stats, jobq.Stats) {
	a := newAccounting()
	for _, id := range []string{"a", "b", "c"} {
		a.enqueued[id], a.completed[id], a.delivered[id] = true, true, 1
	}
	a.delivered["b"] = 2
	return a, jobq.Stats{Enqueued: 3, Acked: 3}, jobq.Stats{}
}

func TestJudgeCleanRunHolds(t *testing.T) {
	a, main, dead := cleanAccounting()
	c := judge(a, main, dead)
	if len(c.Violations) != 0 {
		t.Fatalf("clean run judged broken: %v", c.Violations)
	}
	if c.Acknowledged != 3 || c.Lost != 0 || c.Ghosts != 0 || c.Duplicates != 1 || c.Completed != 3 {
		t.Fatalf("wrong counts: %+v", c)
	}
}

// The harness exists to catch these. Each case breaks a clean run in one
// way; judge must name it.
func TestJudgeCatchesEachBreakage(t *testing.T) {
	cases := []struct {
		name   string
		break_ func(a *accounting, main, dead *jobq.Stats)
		want   string
		check  func(c contract) bool
	}{
		{
			name: "acknowledged enqueue never delivered",
			break_: func(a *accounting, main, dead *jobq.Stats) {
				// The nodes lost it: acknowledged to the client, absent
				// from every count.
				a.enqueued["d"] = true
			},
			want:  "LOST: acknowledged enqueue d",
			check: func(c contract) bool { return c.Lost == 1 },
		},
		{
			name: "acked job delivered again",
			break_: func(a *accounting, main, dead *jobq.Stats) {
				a.ghosts = append(a.ghosts, "a")
				a.delivered["a"] = 2
			},
			want:  "GHOST: job a",
			check: func(c contract) bool { return c.Ghosts == 1 },
		},
		{
			name: "failed ack with no delivery is still a loss",
			break_: func(a *accounting, main, dead *jobq.Stats) {
				a.enqueued["d"], a.maybeAcked["d"] = true, true
				main.Enqueued, main.Acked = 4, 4
			},
			want:  "LOST: acknowledged enqueue d",
			check: func(c contract) bool { return c.Lost == 1 },
		},
		{
			name: "retry enqueued twice",
			break_: func(a *accounting, main, dead *jobq.Stats) {
				main.Enqueued, main.Acked = 4, 4 // one more than acknowledged, and no call failed
			},
			want: "a retry enqueued twice",
		},
		{
			name: "nodes hold fewer than were acknowledged",
			break_: func(a *accounting, main, dead *jobq.Stats) {
				main.Enqueued, main.Acked = 2, 2
			},
			want: "nodes hold 2 enqueues but 3 were acknowledged",
		},
		{
			name: "node counts do not add up",
			break_: func(a *accounting, main, dead *jobq.Stats) {
				main.Acked = 2
			},
			want: "Enqueued 3 != Acked 2 + DeadLetters 0",
		},
		{
			name: "nodes forgot an acknowledged ack",
			break_: func(a *accounting, main, dead *jobq.Stats) {
				main.Acked, main.DeadLetters = 2, 1
				dead.Enqueued, dead.Dropped = 1, 1
			},
			want: "nodes recorded 2 acks but 3 were acknowledged",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a, main, dead := cleanAccounting()
			tc.break_(a, &main, &dead)
			c := judge(a, main, dead)
			if !strings.Contains(strings.Join(c.Violations, "\n"), tc.want) {
				t.Fatalf("want a violation containing %q, got %v", tc.want, c.Violations)
			}
			if tc.check != nil && !tc.check(c) {
				t.Fatalf("wrong counts: %+v", c)
			}
		})
	}
}

// An ack whose reply was lost settles the job without the client knowing.
// That is permitted, and must not be reported as a loss.
func TestJudgeAllowsUnconfirmedAck(t *testing.T) {
	a, main, dead := cleanAccounting()
	delete(a.completed, "c")
	a.maybeAcked["c"] = true
	c := judge(a, main, dead)
	if len(c.Violations) != 0 || c.UnconfirmedAcks != 1 || c.Lost != 0 {
		t.Fatalf("unconfirmed ack misjudged: %+v", c)
	}
}

// An enqueue that failed at the client but landed on the nodes is one
// extra job, allowed for by the failed call.
func TestJudgeAllowsOneExtraJobPerFailedEnqueue(t *testing.T) {
	a, main, dead := cleanAccounting()
	a.failed = 1
	main.Enqueued, main.Acked = 4, 4
	if c := judge(a, main, dead); len(c.Violations) != 0 {
		t.Fatalf("misjudged: %v", c.Violations)
	}
}

func at(msec int) time.Time { return time.Unix(1000, 0).Add(time.Duration(msec) * time.Millisecond) }

func TestFirstAckAfterIgnoresWritesAlreadyOnTheWire(t *testing.T) {
	ws := []write{
		{sent: at(90), done: at(110), ok: true},   // on the wire when the fault landed
		{sent: at(100), done: at(900), ok: false}, // sent after, failed
		{sent: at(120), done: at(700), ok: true},  // sent after, slow
		{sent: at(300), done: at(650), ok: true},  // sent later, acknowledged first
	}
	ack, ok := firstAckAfter(ws, at(100))
	if !ok || !ack.Equal(at(650)) {
		t.Fatalf("got %v, %v; want the ack at 650ms", ack, ok)
	}
	if _, ok := firstAckAfter(ws, at(400)); ok {
		t.Fatal("found a write after the last one was sent")
	}
}

func TestLongestStallCountsBothEdges(t *testing.T) {
	ws := []write{
		{sent: at(0), done: at(50), ok: true}, // before the window
		{sent: at(100), done: at(150), ok: true},
		{sent: at(100), done: at(400), ok: false}, // failures do not end a stall
		{sent: at(100), done: at(600), ok: true},
		{sent: at(600), done: at(640), ok: true},
	}
	if got := longestStall(ws, at(100), at(700)); got != 450*time.Millisecond {
		t.Fatalf("got %v, want 450ms (150 to 600)", got)
	}
	// Nothing acknowledged at all: the whole window is the stall.
	if got := longestStall(nil, at(100), at(700)); got != 600*time.Millisecond {
		t.Fatalf("empty window: got %v, want 600ms", got)
	}
	// The stall runs to the end of the window if writes never resume.
	if got := longestStall(ws[:2], at(100), at(700)); got != 550*time.Millisecond {
		t.Fatalf("no recovery: got %v, want 550ms", got)
	}
}

func TestLatenciesWindowIsBySendTime(t *testing.T) {
	ws := []write{
		{sent: at(90), done: at(200), ok: true},
		{sent: at(100), done: at(130), ok: true},
		{sent: at(150), done: at(160), ok: false},
		{sent: at(199), done: at(500), ok: true},
		{sent: at(200), done: at(210), ok: true},
	}
	took, failed := latencies(ws, at(100), at(200))
	if len(took) != 2 || failed != 1 || took[0] != 30*time.Millisecond || took[1] != 301*time.Millisecond {
		t.Fatalf("got %v, %d failed", took, failed)
	}
}

func TestPercentileNearestRank(t *testing.T) {
	var ds []time.Duration
	for i := 1; i <= 10; i++ {
		ds = append(ds, time.Duration(i)*time.Millisecond)
	}
	for p, want := range map[float64]time.Duration{50: 5, 90: 9, 99: 10, 100: 10, 1: 1} {
		if got := percentile(ds, p); got != want*time.Millisecond {
			t.Errorf("p%v = %v, want %vms", p, got, want)
		}
	}
	if percentile(nil, 50) != 0 {
		t.Error("percentile of nothing should be 0")
	}
}

func TestAgreed(t *testing.T) {
	st := func(state string, term, leader uint64) jobqhttp.ClusterStatus {
		return jobqhttp.ClusterStatus{State: state, Term: term, Leader: leader}
	}
	cases := []struct {
		name   string
		sts    map[uint64]jobqhttp.ClusterStatus
		leader uint64
		ok     bool
	}{
		{"all follow one leader", map[uint64]jobqhttp.ClusterStatus{1: st("follower", 4, 2), 2: st("leader", 4, 2), 3: st("follower", 4, 2)}, 2, true},
		{"no leader", map[uint64]jobqhttp.ClusterStatus{1: st("follower", 4, 0), 2: st("candidate", 5, 0)}, 0, false},
		{"two leaders", map[uint64]jobqhttp.ClusterStatus{1: st("leader", 4, 1), 2: st("leader", 5, 2), 3: st("follower", 5, 2)}, 0, false},
		{"one still on the old term", map[uint64]jobqhttp.ClusterStatus{1: st("follower", 4, 3), 2: st("leader", 5, 2), 3: st("follower", 5, 2)}, 0, false},
		{"nobody answered", map[uint64]jobqhttp.ClusterStatus{}, 0, false},
	}
	for _, tc := range cases {
		leader, _, ok := agreed(tc.sts)
		if leader != tc.leader || ok != tc.ok {
			t.Errorf("%s: got leader %d ok %v, want %d %v", tc.name, leader, ok, tc.leader, tc.ok)
		}
	}
}

func TestParseNodes(t *testing.T) {
	nodes, err := parseNodes(" 3=http://c:8080/ , 1=http://a:8080,2=https://b:8080")
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 3 || nodes[0].id != 1 || nodes[2].id != 3 || nodes[2].url != "http://c:8080" {
		t.Fatalf("got %+v", nodes)
	}
	for _, bad := range []string{
		"1=http://a,2=http://b",              // two nodes cannot survive a fault
		"1=http://a,1=http://b,3=http://c",   // listed twice
		"1=a:8080,2=http://b,3=http://c",     // no scheme
		"0=http://a,2=http://b,3=http://c",   // ID 0
		"one=http://a,2=http://b,3=http://c", // not a number
	} {
		if _, err := parseNodes(bad); err == nil {
			t.Errorf("parseNodes(%q) succeeded", bad)
		}
	}
}

func TestParseFaults(t *testing.T) {
	fs, err := parseFaults("slow-network, leader-kill")
	if err != nil || len(fs) != 2 || fs[0].name != "slow-network" || fs[1].name != "leader-kill" {
		t.Fatalf("got %v, %v", fs, err)
	}
	if _, err := parseFaults("leader-kill,meteor"); err == nil {
		t.Error("unknown fault accepted")
	}
	if _, err := parseFaults(" , "); err == nil {
		t.Error("empty list accepted")
	}
}

// Each fault's own test of whether it took effect must say no when it did
// not: a fault that was never applied must not pass for one that was.
func TestFaultsAreNotSeenWhenNothingHappened(t *testing.T) {
	// Nothing happened: every node still answers, node 1 still leads term 4,
	// writes are as fast as before.
	calm := map[uint64]jobqhttp.ClusterStatus{
		1: {State: "leader", Term: 4, Leader: 1},
		2: {State: "follower", Term: 4, Leader: 1},
		3: {State: "follower", Term: 4, Leader: 1},
	}
	for _, f := range faults {
		targets := f.pick(1, []uint64{1, 2, 3}, rand.New(rand.NewSource(1)))
		seen, evidence := f.seen(observation{
			targets: targets, leaderBefore: 1, termBefore: 4, mid: calm, termAfter: 4,
			baselineP50: 3 * time.Millisecond, faultP50: 3 * time.Millisecond, faultWrites: 100,
		})
		if seen {
			t.Errorf("%s: claimed to have been seen when nothing happened (%s)", f.name, evidence)
		}
	}
}

func TestReportFailsOnAnUnseenFaultOrAViolation(t *testing.T) {
	good := report{Runs: 1, Cells: []cellResult{{Fault: "leader-kill", Runs: []runResult{{Run: 1, Seen: true}}}}}
	if !good.ok() {
		t.Fatal("a clean report was not ok")
	}
	unseen := report{Runs: 1, Cells: []cellResult{{Fault: "leader-kill", Runs: []runResult{{Run: 1, Seen: false, Evidence: "x"}}}}}
	lost := report{Runs: 1, Cells: []cellResult{{Fault: "leader-kill", Runs: []runResult{{Run: 1, Seen: true}},
		Contract: contract{Lost: 1, Violations: []string{"LOST: acknowledged enqueue d"}}}}}
	short := report{Runs: 2, Cells: []cellResult{{Fault: "leader-kill", Runs: []runResult{{Run: 1, Seen: true}}}}}
	for name, r := range map[string]report{"unseen fault": unseen, "lost job": lost, "too few runs": short, "no faults": {Runs: 1}} {
		if r.ok() {
			t.Errorf("%s: report was ok", name)
		}
	}
	if md := lost.markdown(); !strings.Contains(md, "FAILED") || !strings.Contains(md, "LOST: acknowledged enqueue d") {
		t.Errorf("markdown does not show the loss:\n%s", md)
	}
	if md := good.markdown(); !strings.Contains(md, "Contract held") {
		t.Errorf("markdown does not say the contract held:\n%s", md)
	}
}
