package main

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/aleks-drozy/jobq/jobqhttp"
)

// fault is one way of breaking the cluster and how to tell that it broke.
type fault struct {
	name string
	what string
	// inject and undo are hook verbs, applied to every target.
	inject, undo string
	pick         func(leader uint64, ids []uint64, rng *rand.Rand) []uint64
	// seen decides, from what the harness observed, whether the fault
	// took effect. A fault that was never seen proves nothing however
	// clean the counts are, so an unseen fault fails the run.
	seen func(o observation) (bool, string)
}

// observation is what the harness saw around one injection.
type observation struct {
	targets      []uint64
	leaderBefore uint64
	termBefore   uint64
	// mid is each node's answer at the end of the hold, just before the
	// fault was undone. A node that did not answer is missing.
	mid         map[uint64]jobqhttp.ClusterStatus
	termAfter   uint64
	baselineP50 time.Duration
	faultP50    time.Duration
	faultWrites int // writes acknowledged that were sent during the hold
}

func pickLeader(leader uint64, _ []uint64, _ *rand.Rand) []uint64 { return []uint64{leader} }

func pickFollower(leader uint64, ids []uint64, rng *rand.Rand) []uint64 {
	var followers []uint64
	for _, id := range ids {
		if id != leader {
			followers = append(followers, id)
		}
	}
	return []uint64{followers[rng.Intn(len(followers))]}
}

func pickAll(_ uint64, ids []uint64, _ *rand.Rand) []uint64 { return ids }

// newLeader finds a node other than the targets leading in a later term.
func newLeader(o observation) (id, term uint64, ok bool) {
	for id, st := range o.mid {
		if st.State == "leader" && st.Term > o.termBefore && !contains(o.targets, id) {
			return id, st.Term, true
		}
	}
	return 0, 0, false
}

func contains(ids []uint64, id uint64) bool {
	for _, x := range ids {
		if x == id {
			return true
		}
	}
	return false
}

var faults = []fault{
	{
		name: "leader-kill", what: "leader process killed (SIGKILL), restarted after the hold",
		inject: "kill", undo: "start", pick: pickLeader,
		seen: func(o observation) (bool, string) {
			t := o.targets[0]
			if _, up := o.mid[t]; up {
				return false, fmt.Sprintf("node %d still answered after it was killed", t)
			}
			id, term, ok := newLeader(o)
			if !ok {
				return false, fmt.Sprintf("node %d down but no other node took over: %s", t, describe(o.mid))
			}
			return true, fmt.Sprintf("node %d down; node %d led term %d", t, id, term)
		},
	},
	{
		name: "follower-kill", what: "follower process killed (SIGKILL), restarted after the hold",
		inject: "kill", undo: "start", pick: pickFollower,
		seen: func(o observation) (bool, string) {
			t := o.targets[0]
			if _, up := o.mid[t]; up {
				return false, fmt.Sprintf("node %d still answered after it was killed", t)
			}
			return true, fmt.Sprintf("node %d down; term %d -> %d", t, o.termBefore, o.termAfter)
		},
	},
	{
		name: "leader-isolate", what: "leader cut off from the other nodes, still reachable by clients",
		inject: "isolate", undo: "heal", pick: pickLeader,
		seen: func(o observation) (bool, string) {
			t := o.targets[0]
			id, term, ok := newLeader(o)
			if !ok {
				return false, fmt.Sprintf("no other node took over from node %d: %s", t, describe(o.mid))
			}
			old := "did not answer"
			if st, up := o.mid[t]; up {
				old = fmt.Sprintf("was %s in term %d", st.State, st.Term)
			}
			return true, fmt.Sprintf("node %d led term %d; cut-off node %d %s", id, term, t, old)
		},
	},
	{
		name: "follower-isolate", what: "follower cut off from the other nodes, still reachable by clients",
		inject: "isolate", undo: "heal", pick: pickFollower,
		seen: func(o observation) (bool, string) {
			t := o.targets[0]
			st, up := o.mid[t]
			if !up {
				return false, fmt.Sprintf("cut-off node %d did not answer: it should still be reachable by clients", t)
			}
			// A follower that hears nothing stops following: it knows no
			// leader, or stands for election in a term of its own.
			if st.Leader != 0 && st.State == "follower" && st.Term == o.termBefore {
				return false, fmt.Sprintf("node %d still followed node %d in term %d", t, st.Leader, st.Term)
			}
			return true, fmt.Sprintf("cut-off node %d was %s in term %d; cluster term %d -> %d",
				t, st.State, st.Term, o.termBefore, o.termAfter)
		},
	},
	{
		name: "slow-network", what: "every node's network made slow and lossy",
		inject: "slow", undo: "fast", pick: pickAll,
		seen: func(o observation) (bool, string) {
			if o.faultWrites == 0 {
				return true, "no write was acknowledged while the network was slow"
			}
			if o.baselineP50 <= 0 || o.faultP50 < 3*o.baselineP50 {
				return false, fmt.Sprintf("write p50 %s before, %s during: no slower", o.baselineP50, o.faultP50)
			}
			return true, fmt.Sprintf("write p50 %s before, %s during; term %d -> %d",
				o.baselineP50.Round(100*time.Microsecond), o.faultP50.Round(100*time.Microsecond), o.termBefore, o.termAfter)
		},
	},
}

func faultNames() []string {
	names := make([]string, len(faults))
	for i, f := range faults {
		names[i] = f.name
	}
	return names
}

func parseFaults(s string) ([]fault, error) {
	var out []fault
	for _, name := range strings.Split(s, ",") {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		found := false
		for _, f := range faults {
			if f.name == name {
				out, found = append(out, f), true
			}
		}
		if !found {
			return nil, fmt.Errorf("-faults: unknown fault %q (have %s)", name, strings.Join(faultNames(), ", "))
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("-faults: none given (have %s)", strings.Join(faultNames(), ", "))
	}
	return out, nil
}

// runResult is one injection of one fault.
type runResult struct {
	Run          int      `json:"run"`
	Targets      []uint64 `json:"targets"`
	LeaderBefore uint64   `json:"leader_before"`
	LeaderAfter  uint64   `json:"leader_after"`
	TermBefore   uint64   `json:"term_before"`
	TermAfter    uint64   `json:"term_after"`
	// InjectMs is how long the hook took to apply the fault. The fault
	// landed at some moment in that time; the clock for the three figures
	// below starts before the hook is called, so each of them includes
	// it: they are upper bounds.
	InjectMs float64 `json:"inject_ms"`
	// FirstWriteMs: to the first acknowledgement of a write that was sent
	// after the hook had returned, so after the fault was certainly in
	// place. -1 if there was none before the run ended.
	FirstWriteMs float64 `json:"first_write_ms"`
	// NewLeaderMs: from the fault to a node being seen to lead in a later
	// term. -1 if the term never changed.
	NewLeaderMs float64 `json:"new_leader_ms"`
	// LongestStallMs: the longest stretch with no write acknowledged,
	// from the fault until the run ended.
	LongestStallMs float64 `json:"longest_stall_ms"`
	// WholeAgainMs: from undoing the fault to every node agreeing on a
	// leader and caught up.
	WholeAgainMs  float64 `json:"whole_again_ms"`
	WritesOK      int     `json:"writes_ok"`     // sent during the hold and acknowledged
	WritesFailed  int     `json:"writes_failed"` // sent during the hold and failed
	BaselineP50Ms float64 `json:"baseline_p50_ms"`
	BaselineP99Ms float64 `json:"baseline_p99_ms"`
	FaultP50Ms    float64 `json:"fault_p50_ms"`
	FaultP99Ms    float64 `json:"fault_p99_ms"`
	Seen          bool    `json:"seen"`
	Evidence      string  `json:"evidence"`
	Error         string  `json:"error,omitempty"`
}

// cellResult is every run of one fault and the verdict on the work done
// while they ran.
type cellResult struct {
	Fault    string      `json:"fault"`
	What     string      `json:"what"`
	Topic    string      `json:"topic"`
	Runs     []runResult `json:"runs"`
	Contract contract    `json:"contract"`
	Error    string      `json:"error,omitempty"`
}

const wholeTimeout = 90 * time.Second

// runCell gives one fault its runs, with its own topic and its own
// workload, then drains the topic and judges the contract.
func runCell(ctx context.Context, l *lab, f fault, c config, rng *rand.Rand) cellResult {
	cell := cellResult{Fault: f.name, What: f.what, Topic: "chaos-" + f.name, Contract: contract{Violations: []string{}}}
	fmt.Fprintf(l.out, "\n== %s: %s\n", f.name, f.what)
	if _, _, err := l.waitHealthy(ctx, wholeTimeout); err != nil {
		cell.Error = "before starting: " + err.Error()
		fmt.Fprintln(l.out, "   "+cell.Error)
		return cell
	}

	w := startWorkload(l.addrs(), cell.Topic, c.producers, c.consumers, c.pace)
	sleep(ctx, c.settle) // a baseline before the first fault
	for i := 1; i <= c.runs && ctx.Err() == nil; i++ {
		r := runOnce(ctx, l, f, c, w, rng, i)
		cell.Runs = append(cell.Runs, r)
		fmt.Fprintf(l.out, "   run %d: %s\n", i, r.line())
		if r.Error != "" {
			cell.Error = fmt.Sprintf("run %d: %s", i, r.Error)
			break
		}
	}
	w.stopProducers()
	main, dead, err := l.drain(cell.Topic, c.drain)
	w.stopConsumers()
	cell.Contract = judge(w.acct, main, dead)
	if err != nil {
		// Undrained, the counts above are not final and the losses listed
		// may only be jobs still waiting. Say so first.
		cell.Contract.Violations = append([]string{"NOT DRAINED: " + err.Error()}, cell.Contract.Violations...)
	}
	k := cell.Contract
	fmt.Fprintf(l.out, "   contract: %d acknowledged enqueues, %d lost, %d ghosts, %d duplicates\n",
		k.Acknowledged, k.Lost, k.Ghosts, k.Duplicates)
	for _, v := range k.Violations {
		fmt.Fprintln(l.out, "   "+v)
	}
	return cell
}

// runOnce injects the fault once: wait for a whole cluster, break it, hold,
// undo, wait for a whole cluster again, settle.
func runOnce(ctx context.Context, l *lab, f fault, c config, w *workload, rng *rand.Rand, i int) runResult {
	r := runResult{Run: i, FirstWriteMs: -1, NewLeaderMs: -1}
	leader, term, err := l.waitHealthy(ctx, wholeTimeout)
	if err != nil {
		r.Error = err.Error()
		return r
	}
	r.LeaderBefore, r.TermBefore = leader, term
	r.Targets = f.pick(leader, l.ids(), rng)

	stopWatch := make(chan struct{})
	elected := l.watchNewLeader(term, stopWatch)

	baselineFrom := time.Now().Add(-c.settle)
	t0 := time.Now()
	var injectErr error
	for _, id := range r.Targets {
		if err := l.inj.Do(f.inject, id); err != nil && injectErr == nil {
			injectErr = err
		}
	}
	// From here the fault is in place. Until here it may not have been: a
	// write sent while the hook was still running may have been answered
	// by a node that was about to be killed.
	tInjected := time.Now()
	r.InjectMs = ms(tInjected.Sub(t0))
	if injectErr == nil {
		sleep(ctx, c.hold-time.Since(t0))
	}
	mid := l.look()

	// Undo on every target whatever happened above: a fault left in place
	// would poison every run after this one.
	tUndo := time.Now()
	var undoErr error
	for _, id := range r.Targets {
		if err := l.inj.Do(f.undo, id); err != nil && undoErr == nil {
			undoErr = err
		}
	}
	// Whole again is waited for even after an interrupt: the faults are
	// undone by now, and the cluster should be left as it was found.
	leaderAfter, termAfter, healthErr := l.waitHealthy(context.Background(), wholeTimeout)
	r.WholeAgainMs = ms(time.Since(tUndo))
	r.LeaderAfter, r.TermAfter = leaderAfter, termAfter
	sleep(ctx, c.settle)
	tEnd := time.Now()

	close(stopWatch)
	if at := <-elected; !at.IsZero() {
		r.NewLeaderMs = ms(at.Sub(t0))
	}

	ws := w.snapshot()
	if ack, ok := firstAckAfter(ws, tInjected); ok {
		r.FirstWriteMs = ms(ack.Sub(t0))
	}
	r.LongestStallMs = ms(longestStall(ws, t0, tEnd))
	base, _ := latencies(ws, baselineFrom, t0)
	during, failed := latencies(ws, tInjected, tUndo)
	r.WritesOK, r.WritesFailed = len(during), failed
	r.BaselineP50Ms, r.BaselineP99Ms = ms(percentile(base, 50)), ms(percentile(base, 99))
	r.FaultP50Ms, r.FaultP99Ms = ms(percentile(during, 50)), ms(percentile(during, 99))

	switch {
	case injectErr != nil:
		r.Error = "injecting: " + injectErr.Error()
	case undoErr != nil:
		r.Error = "undoing: " + undoErr.Error()
	case healthErr != nil:
		r.Error = "after undoing: " + healthErr.Error()
	}
	r.Seen, r.Evidence = f.seen(observation{
		targets: r.Targets, leaderBefore: leader, termBefore: term, mid: mid, termAfter: termAfter,
		baselineP50: percentile(base, 50), faultP50: percentile(during, 50), faultWrites: len(during),
	})
	return r
}

func (r runResult) line() string {
	if r.Error != "" {
		return "ERROR: " + r.Error
	}
	seen := "seen"
	if !r.Seen {
		seen = "NOT SEEN"
	}
	return fmt.Sprintf("target %v, first write after %s, longest stall %s, new leader after %s, whole again %s after undo [%s: %s]",
		r.Targets, fmtMs(r.FirstWriteMs), fmtMs(r.LongestStallMs), fmtMs(r.NewLeaderMs), fmtMs(r.WholeAgainMs), seen, r.Evidence)
}

// fmtMs renders a millisecond figure, and a negative one as "never".
func fmtMs(v float64) string {
	switch {
	case v < 0:
		return "never"
	case v < 10:
		return fmt.Sprintf("%.1f ms", v)
	default:
		return fmt.Sprintf("%.0f ms", v)
	}
}
