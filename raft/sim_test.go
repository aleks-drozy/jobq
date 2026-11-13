package raft

// A deterministic cluster simulator. One seeded random source decides
// everything that is not the algorithm: which messages are dropped, delayed,
// duplicated or reordered, which nodes crash and when they come back, where
// the network splits, whose clock runs slow, and when clients propose. The
// nodes themselves have no goroutines or timers, so a seed is a complete,
// replayable description of a run:
//
//	go test ./raft -run TestSimulation -raft.seed=12345 -v
//
// After every step the simulator checks the safety properties of Figure 3
// of the Raft paper against a record of everything any node has ever
// claimed. After the chaos it heals the cluster and requires it to make
// progress, so a run cannot pass by electing nobody and committing nothing.
//
// Nodes also compact their logs at random, each on its own schedule, so
// followers are caught up by snapshot as well as by log. The state machine
// the simulator models is a running hash of every applied entry, which
// makes a snapshot's contents checkable: the state after index i has
// exactly one right value.

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"math/rand"
	"runtime"
	"strings"
	"sync"
	"testing"
)

var (
	flagSims = flag.Int("raft.sims", 1000, "number of seeded simulation runs (CI runs 10000)")
	flagSeed = flag.Int64("raft.seed", 0, "replay one simulation seed with a trace")
)

// invariantError is a safety or liveness property the simulator saw broken.
type invariantError struct {
	property string
	detail   string
}

func (e *invariantError) Error() string { return e.property + ": " + e.detail }

func violated(property, format string, args ...any) error {
	return &invariantError{property: property, detail: fmt.Sprintf(format, args...)}
}

type simConfig struct {
	seed   int64
	nodes  int
	steps  int // chaos steps, before the cluster is healed
	batch  int // MaxEntriesPerMsg
	faults faults
	trace  func(format string, args ...any)

	dropP      float64 // per message
	dupP       float64 // per message
	maxDelay   int     // steps a message may spend in flight
	slowP      float64 // per node per step: its clock does not tick
	crashP     float64 // per step
	restartP   float64 // per step
	partitionP float64 // per step
	healP      float64 // per step
	proposeP   float64 // per step
	compactP   float64 // per step: one live node compacts its log
	maxKeep    int     // entries a compaction may keep behind its snapshot
}

func defaultSimConfig(seed int64) simConfig {
	return simConfig{
		seed:       seed,
		nodes:      3 + 2*int(seed&1), // odd seeds run five nodes, even run three
		steps:      400,
		batch:      4, // small, so catch-up batching is exercised
		dropP:      0.10,
		dupP:       0.05,
		maxDelay:   3,
		slowP:      0.10,
		crashP:     0.012,
		restartP:   0.04,
		partitionP: 0.012,
		healP:      0.03,
		proposeP:   0.30,
		compactP:   0.05,
		maxKeep:    3,
	}
}

type simStats struct {
	runs        int
	elections   int // distinct (term, leader) pairs observed
	committed   int // entries in the global committed record
	proposals   int
	crashes     int
	restarts    int
	partitions  int
	dropped     int
	duplicated  int
	delivered   int
	multiLeader int // runs that saw more than one leader term
	compactions int
	installs    int // snapshots a node loaded from its leader
	restores    int // snapshots a node loaded from its own storage on restart
}

func (a *simStats) add(b simStats) {
	a.runs += b.runs
	a.elections += b.elections
	a.committed += b.committed
	a.proposals += b.proposals
	a.crashes += b.crashes
	a.restarts += b.restarts
	a.partitions += b.partitions
	a.dropped += b.dropped
	a.duplicated += b.duplicated
	a.delivered += b.delivered
	a.multiLeader += b.multiLeader
	a.compactions += b.compactions
	a.installs += b.installs
	a.restores += b.restores
}

type envelope struct {
	at int
	m  Message
}

type link struct{ from, to NodeID }

type sim struct {
	cfg simConfig
	rng *rand.Rand
	now int

	ids      []NodeID
	nodes    map[NodeID]*Node // nil while crashed
	stores   map[NodeID]*MemoryStorage
	boots    map[NodeID]int
	applied  map[NodeID]uint64 // highest index applied since the node's last start
	state    map[NodeID]uint64 // each node's state machine: a hash of what it applied
	fresh    map[NodeID]bool   // started, and not yet asked for its first Ready
	inflight []envelope
	cut      map[link]bool

	// The record every claim is checked against.
	leaders   map[uint64]NodeID // term -> the one node allowed to lead it
	committed []Entry           // committed[i] has Index i+1
	hashes    []uint64          // hashes[i] is the state after applying index i

	proposed int
	chaos    bool
	stats    simStats
}

func newSim(cfg simConfig) (*sim, error) {
	s := &sim{
		cfg:     cfg,
		rng:     rand.New(rand.NewSource(cfg.seed)),
		nodes:   map[NodeID]*Node{},
		stores:  map[NodeID]*MemoryStorage{},
		boots:   map[NodeID]int{},
		applied: map[NodeID]uint64{},
		state:   map[NodeID]uint64{},
		fresh:   map[NodeID]bool{},
		cut:     map[link]bool{},
		leaders: map[uint64]NodeID{},
		hashes:  []uint64{emptyState},
		chaos:   true,
	}
	for i := 1; i <= cfg.nodes; i++ {
		id := NodeID(i)
		s.ids = append(s.ids, id)
		s.stores[id] = NewMemoryStorage()
		if err := s.start(id); err != nil {
			return nil, err
		}
	}
	return s, nil
}

func (s *sim) tracef(format string, args ...any) {
	if s.cfg.trace != nil {
		s.cfg.trace("[%4d] "+format, append([]any{s.now}, args...)...)
	}
}

// start boots a node from its storage. A restart is the same call: the
// node comes back with only what it persisted.
func (s *sim) start(id NodeID) error {
	s.boots[id]++
	n, err := New(Config{
		ID:               id,
		Peers:            s.ids0(),
		ElectionTicks:    10,
		HeartbeatTicks:   2,
		MaxEntriesPerMsg: s.cfg.batch,
		Storage:          s.stores[id],
		Rand:             rand.New(rand.NewSource(s.cfg.seed*1000003 + int64(id)*101 + int64(s.boots[id]))),
	})
	if err != nil {
		return err
	}
	n.faults = s.cfg.faults
	s.nodes[id] = n
	s.applied[id] = 0
	s.state[id] = emptyState
	s.fresh[id] = true
	return nil
}

const emptyState uint64 = 0x9e3779b97f4a7c15

// mix folds one applied entry into a state. Order matters and every field
// counts, so two nodes agree on a state only if they applied the same
// entries in the same order.
func mix(state uint64, e Entry) uint64 {
	h := state ^ e.Index*0xff51afd7ed558ccd ^ e.Term*0xc4ceb9fe1a85ec53 ^ uint64(e.Kind)<<56
	for _, b := range e.Data {
		h = (h ^ uint64(b)) * 0x100000001b3
	}
	h ^= h >> 29
	return h * 0xbf58476d1ce4e5b9
}

func encodeState(h uint64) []byte { return binary.LittleEndian.AppendUint64(nil, h) }

// checkSnapshot holds a snapshot, wherever it was found, against the
// record: it must cover only committed entries and contain exactly the
// state those entries produce.
func (s *sim) checkSnapshot(where string, snap Snapshot) error {
	if snap.Index == 0 {
		return nil
	}
	if snap.Index > uint64(len(s.committed)) {
		return violated("state machine safety", "%s has a snapshot at index %d, record ends at %d", where, snap.Index, len(s.committed))
	}
	if want := s.committed[snap.Index-1].Term; snap.Term != want {
		return violated("state machine safety", "%s has a snapshot at index %d of term %d, record says term %d", where, snap.Index, snap.Term, want)
	}
	if !bytes.Equal(snap.Data, encodeState(s.hashes[snap.Index])) {
		return violated("state machine safety", "%s has a snapshot at index %d holding the wrong state", where, snap.Index)
	}
	return nil
}

// entryAt returns the entry a node holds at index, if it still holds it.
func entryAt(log []Entry, index uint64) (Entry, bool) {
	if len(log) == 0 || index < log[0].Index || index > log[len(log)-1].Index {
		return Entry{}, false
	}
	return log[index-log[0].Index], true
}

// ids0 is the full membership, which must be known before every node has
// been started.
func (s *sim) ids0() []NodeID {
	all := make([]NodeID, s.cfg.nodes)
	for i := range all {
		all[i] = NodeID(i + 1)
	}
	return all
}

func (s *sim) live() []NodeID {
	var out []NodeID
	for _, id := range s.ids {
		if s.nodes[id] != nil {
			out = append(out, id)
		}
	}
	return out
}

func (s *sim) dead() []NodeID {
	var out []NodeID
	for _, id := range s.ids {
		if s.nodes[id] == nil {
			out = append(out, id)
		}
	}
	return out
}

func (s *sim) injectFaults() error {
	if live := s.live(); len(live) > 0 && s.rng.Float64() < s.cfg.crashP {
		id := live[s.rng.Intn(len(live))]
		s.tracef("crash node %d (was %v, term %d)", id, s.nodes[id].state, s.nodes[id].term)
		s.nodes[id] = nil
		s.stats.crashes++
	}
	if dead := s.dead(); len(dead) > 0 && s.rng.Float64() < s.cfg.restartP {
		id := dead[s.rng.Intn(len(dead))]
		s.tracef("restart node %d", id)
		if err := s.start(id); err != nil {
			return err
		}
		s.stats.restarts++
	}
	if s.rng.Float64() < s.cfg.partitionP {
		s.stats.partitions++
		if s.rng.Intn(4) == 0 {
			// One direction of one link: A hears B, B does not hear A.
			a := s.ids[s.rng.Intn(len(s.ids))]
			b := s.ids[s.rng.Intn(len(s.ids))]
			if a != b {
				s.cut[link{a, b}] = true
				s.tracef("cut link %d -> %d", a, b)
			}
		} else {
			// Split the cluster in two at a random point of a shuffle.
			order := append([]NodeID(nil), s.ids...)
			s.rng.Shuffle(len(order), func(i, j int) { order[i], order[j] = order[j], order[i] })
			k := 1 + s.rng.Intn(len(order)-1)
			for _, a := range order[:k] {
				for _, b := range order[k:] {
					s.cut[link{a, b}] = true
					s.cut[link{b, a}] = true
				}
			}
			s.tracef("partition %v | %v", order[:k], order[k:])
		}
	}
	if len(s.cut) > 0 && s.rng.Float64() < s.cfg.healP {
		s.cut = map[link]bool{}
		s.tracef("heal network")
	}
	return nil
}

func (s *sim) propose(n *Node) {
	s.proposed++
	s.stats.proposals++
	data := []byte(fmt.Sprintf("p%d", s.proposed))
	if idx, term, err := n.Propose(data); err == nil {
		s.tracef("node %d proposes %s at index %d term %d", n.id, data, idx, term)
	}
}

func (s *sim) step() error {
	s.now++
	if s.chaos {
		if err := s.injectFaults(); err != nil {
			return err
		}
	}

	for _, id := range s.live() {
		if s.chaos && s.rng.Float64() < s.cfg.slowP {
			continue
		}
		if err := s.nodes[id].Tick(); err != nil {
			return err
		}
	}

	// Clients talk to anyone who believes it leads, deposed leaders
	// included: those proposals must never be reported committed.
	if s.chaos && s.rng.Float64() < s.cfg.proposeP {
		for _, id := range s.live() {
			if n := s.nodes[id]; n.state == Leader {
				s.propose(n)
			}
		}
	}

	var due []Message
	rest := s.inflight[:0]
	for _, e := range s.inflight {
		if e.at <= s.now {
			due = append(due, e.m)
		} else {
			rest = append(rest, e)
		}
	}
	s.inflight = rest
	s.rng.Shuffle(len(due), func(i, j int) { due[i], due[j] = due[j], due[i] })
	for _, m := range due {
		n := s.nodes[m.To]
		if n == nil {
			continue
		}
		s.stats.delivered++
		if err := n.Step(m); err != nil {
			return err
		}
	}

	for _, id := range s.live() {
		if err := s.collect(id); err != nil {
			return err
		}
	}
	if s.chaos && s.rng.Float64() < s.cfg.compactP {
		if err := s.compact(); err != nil {
			return err
		}
	}
	return s.checkLeaders()
}

// compact has one live node snapshot its state machine somewhere in what
// it has applied since its last snapshot, keeping a few entries behind.
func (s *sim) compact() error {
	live := s.live()
	if len(live) == 0 {
		return nil
	}
	id := live[s.rng.Intn(len(live))]
	n := s.nodes[id]
	keep := s.rng.Intn(s.cfg.maxKeep + 1)
	span := s.applied[id] - n.snap.Index
	if s.applied[id] <= n.snap.Index {
		return nil
	}
	index := n.snap.Index + 1 + uint64(s.rng.Int63n(int64(span)))
	s.tracef("node %d compacts at index %d keeping %d", id, index, keep)
	s.stats.compactions++
	return n.Compact(index, encodeState(s.hashes[index]), keep)
}

// collect takes a node's output: committed entries go to the global record,
// messages go to the network to be lost, delayed and duplicated.
func (s *sim) collect(id NodeID) error {
	rd := s.nodes[id].Ready()
	if snap := rd.Snapshot; snap != nil {
		// A snapshot replaces the state machine. It may only move a node
		// forwards: loading one from behind would un-apply entries.
		if snap.Index < s.applied[id] {
			return violated("apply order", "node %d loaded a snapshot at index %d after applying %d", id, snap.Index, s.applied[id])
		}
		if err := s.checkSnapshot(fmt.Sprintf("node %d", id), *snap); err != nil {
			return err
		}
		if s.fresh[id] {
			s.stats.restores++
		} else {
			s.stats.installs++
			s.tracef("node %d installs a snapshot at index %d", id, snap.Index)
		}
		s.applied[id] = snap.Index
		s.state[id] = s.hashes[snap.Index]
	}
	s.fresh[id] = false
	for _, e := range rd.Committed {
		if e.Index != s.applied[id]+1 {
			return violated("apply order", "node %d applied index %d after %d", id, e.Index, s.applied[id])
		}
		s.applied[id] = e.Index
		switch {
		case e.Index <= uint64(len(s.committed)):
			if have := s.committed[e.Index-1]; !sameEntry(have, e) {
				return violated("state machine safety",
					"node %d applied %s at index %d, but %s was already applied there", id, show(e), e.Index, show(have))
			}
		case e.Index == uint64(len(s.committed))+1:
			s.committed = append(s.committed, e)
			s.hashes = append(s.hashes, mix(s.hashes[e.Index-1], e))
			s.stats.committed++
		default:
			return violated("apply order", "node %d applied index %d, record ends at %d", id, e.Index, len(s.committed))
		}
		s.state[id] = mix(s.state[id], e)
		if s.state[id] != s.hashes[e.Index] {
			return violated("state machine safety", "node %d reached a different state at index %d", id, e.Index)
		}
	}
	for _, m := range rd.Messages {
		if s.cut[link{m.From, m.To}] {
			s.stats.dropped++
			continue
		}
		if s.chaos && s.rng.Float64() < s.cfg.dropP {
			s.stats.dropped++
			continue
		}
		s.inflight = append(s.inflight, envelope{at: s.now + 1 + s.rng.Intn(s.cfg.maxDelay), m: m})
		if s.chaos && s.rng.Float64() < s.cfg.dupP {
			s.stats.duplicated++
			s.inflight = append(s.inflight, envelope{at: s.now + 1 + s.rng.Intn(2*s.cfg.maxDelay), m: m})
		}
	}
	return nil
}

// checkLeaders enforces election safety on every step, and leader
// completeness at the moment a node is first seen leading a term.
func (s *sim) checkLeaders() error {
	for _, id := range s.live() {
		n := s.nodes[id]
		if n.state != Leader {
			continue
		}
		prev, seen := s.leaders[n.term]
		if seen && prev != id {
			return violated("election safety", "nodes %d and %d both led term %d", prev, id, n.term)
		}
		if seen {
			continue
		}
		s.leaders[n.term] = id
		s.stats.elections++
		s.tracef("node %d leads term %d (log %d, committed record %d)", id, n.term, len(n.log), len(s.committed))
		if err := s.checkSnapshot(fmt.Sprintf("node %d", id), n.snap); err != nil {
			return err
		}
		for _, c := range s.committed {
			have, ok := entryAt(n.log, c.Index)
			if !ok && c.Index <= n.snap.Index {
				continue // inside its snapshot, which was just checked
			}
			if !ok || !sameEntry(have, c) {
				return violated("leader completeness",
					"node %d won term %d without committed entry %s", id, n.term, show(c))
			}
		}
	}
	return nil
}

// checkLogs is the expensive sweep, run periodically and at the end: log
// matching across every pair of stores (crashed nodes included), and no
// node's committed prefix may differ from the record.
func (s *sim) checkLogs() error {
	for i, a := range s.ids {
		la := s.stores[a].entries
		for _, b := range s.ids[i+1:] {
			lb := s.stores[b].entries
			if len(la) == 0 || len(lb) == 0 {
				continue
			}
			// Only the stretch of indexes both still hold can be compared.
			lo := max(la[0].Index, lb[0].Index)
			hi := min(la[len(la)-1].Index, lb[len(lb)-1].Index)
			for k := hi; k >= lo; k-- {
				ea, _ := entryAt(la, k)
				eb, _ := entryAt(lb, k)
				if ea.Term != eb.Term {
					continue
				}
				// Same index and term: everything up to here must agree.
				for j := lo; j <= k; j++ {
					ja, _ := entryAt(la, j)
					jb, _ := entryAt(lb, j)
					if !sameEntry(ja, jb) {
						return violated("log matching",
							"nodes %d and %d agree on term %d at index %d but differ at index %d: %s vs %s",
							a, b, ea.Term, k, j, show(ja), show(jb))
					}
				}
				break
			}
		}
	}
	for _, id := range s.ids {
		// Crashed nodes included: what is on disk must be right too.
		if err := s.checkSnapshot(fmt.Sprintf("node %d's storage", id), s.stores[id].snap); err != nil {
			return err
		}
	}
	for _, id := range s.live() {
		n := s.nodes[id]
		if n.commit > uint64(len(s.committed)) {
			return violated("state machine safety", "node %d has commit index %d, record ends at %d", id, n.commit, len(s.committed))
		}
		for k := n.firstIndex(); k <= n.commit; k++ {
			if have, _ := entryAt(n.log, k); !sameEntry(have, s.committed[k-1]) {
				return violated("state machine safety", "node %d holds a committed prefix that differs from the record at index %d", id, k)
			}
		}
	}
	return nil
}

func sameEntry(a, b Entry) bool {
	return a.Index == b.Index && a.Term == b.Term && a.Kind == b.Kind && bytes.Equal(a.Data, b.Data)
}

func show(e Entry) string {
	if e.Kind == EntryNoop {
		return fmt.Sprintf("(%d,t%d,noop)", e.Index, e.Term)
	}
	return fmt.Sprintf("(%d,t%d,%q)", e.Index, e.Term, e.Data)
}

// converge ends the chaos: every node is restarted, the network is whole
// and lossless, and the cluster must now elect a leader, commit a fresh
// proposal and bring every node to the same applied log.
func (s *sim) converge() error {
	s.chaos = false
	s.cut = map[link]bool{}
	for _, id := range s.dead() {
		if err := s.start(id); err != nil {
			return err
		}
	}
	s.tracef("chaos over: all nodes up, network healed")

	const limit = 3000
	var sentinel []byte
	sentinelCommitted := func() bool {
		for _, e := range s.committed {
			if sentinel != nil && bytes.Equal(e.Data, sentinel) {
				return true
			}
		}
		return false
	}
	for i := 0; i < limit; i++ {
		if err := s.step(); err != nil {
			return err
		}
		if !sentinelCommitted() {
			// Ask again every so often: a proposal made to a leader that
			// is then deposed is allowed to vanish.
			if i%40 == 0 {
				for _, id := range s.ids {
					if n := s.nodes[id]; n.state == Leader {
						sentinel = []byte(fmt.Sprintf("sentinel-%d", i))
						_, _, _ = n.Propose(sentinel)
					}
				}
			}
			continue
		}
		done := true
		for _, id := range s.ids {
			if s.applied[id] != uint64(len(s.committed)) || s.stores[id].lastIndex() != uint64(len(s.committed)) {
				done = false
			}
		}
		if done {
			return nil
		}
	}
	var b strings.Builder
	for _, id := range s.ids {
		n := s.nodes[id]
		fmt.Fprintf(&b, " node %d: %v term %d log %d commit %d;", id, n.state, n.term, len(n.log), n.commit)
	}
	return violated("liveness", "healed cluster did not converge in %d steps:%s", limit, b.String())
}

func runSim(cfg simConfig) (simStats, error) {
	s, err := newSim(cfg)
	if err != nil {
		return simStats{}, err
	}
	for i := 0; i < cfg.steps; i++ {
		if err := s.step(); err != nil {
			return s.stats, err
		}
		if i%25 == 0 {
			if err := s.checkLogs(); err != nil {
				return s.stats, err
			}
		}
	}
	if err := s.checkLogs(); err != nil {
		return s.stats, err
	}
	if err := s.converge(); err != nil {
		return s.stats, err
	}
	if err := s.checkLogs(); err != nil {
		return s.stats, err
	}
	// Every node ended with the same log and applied all of it, so the
	// applied sequences are identical; say so explicitly.
	for _, id := range s.ids {
		for _, e := range s.stores[id].entries {
			if !sameEntry(e, s.committed[e.Index-1]) {
				return s.stats, violated("state machine safety", "node %d final log differs from the record at index %d", id, e.Index)
			}
		}
		if s.state[id] != s.hashes[len(s.committed)] {
			return s.stats, violated("state machine safety", "node %d ended in a different state from the record", id)
		}
	}
	s.stats.runs = 1
	if len(s.leaders) > 1 {
		s.stats.multiLeader = 1
	}
	return s.stats, nil
}

func TestSimulation(t *testing.T) {
	if *flagSeed != 0 {
		cfg := defaultSimConfig(*flagSeed)
		cfg.trace = t.Logf
		stats, err := runSim(cfg)
		t.Logf("stats: %+v", stats)
		if err != nil {
			t.Fatalf("seed %d: %v", *flagSeed, err)
		}
		return
	}

	sims := *flagSims
	if testing.Short() {
		sims = min(sims, 500)
	}
	workers := runtime.GOMAXPROCS(0)
	var (
		mu    sync.Mutex
		total simStats
	)
	t.Run("seeds", func(t *testing.T) {
		for w := 0; w < workers; w++ {
			t.Run(fmt.Sprintf("worker%d", w), func(t *testing.T) {
				t.Parallel()
				var local simStats
				for seed := int64(1 + w); seed <= int64(sims); seed += int64(workers) {
					stats, err := runSim(defaultSimConfig(seed))
					if err != nil {
						t.Fatalf("seed %d: %v\nreplay: go test ./raft -run TestSimulation -raft.seed=%d -v", seed, err, seed)
					}
					local.add(stats)
				}
				mu.Lock()
				total.add(local)
				mu.Unlock()
			})
		}
	})
	if t.Failed() {
		return
	}

	t.Logf("%d runs: %d leaders elected, %d entries committed of %d proposed, %d crashes, %d restarts, %d partitions, %d messages delivered, %d dropped, %d duplicated, %d runs with a leader change, %d compactions, %d snapshots installed from a leader, %d restarts from a snapshot",
		total.runs, total.elections, total.committed, total.proposals, total.crashes, total.restarts,
		total.partitions, total.delivered, total.dropped, total.duplicated, total.multiLeader,
		total.compactions, total.installs, total.restores)

	// A suite that never crashed a node or changed a leader proved nothing.
	if total.runs != sims {
		t.Fatalf("completed %d runs, want %d", total.runs, sims)
	}
	for name, n := range map[string]int{
		"crashes": total.crashes, "restarts": total.restarts, "partitions": total.partitions,
		"dropped messages": total.dropped, "duplicated messages": total.duplicated,
		"runs with a leader change": total.multiLeader,
		"compactions":               total.compactions,
		"snapshots installed":       total.installs,
		"restarts from a snapshot":  total.restores,
	} {
		if n < sims/10 {
			t.Errorf("only %d %s across %d runs: the simulator is not exercising that fault", n, name, sims)
		}
	}
	if total.committed < 10*sims {
		t.Errorf("only %d entries committed across %d runs", total.committed, sims)
	}
}

// Everything downstream of a seed must be a pure function of it, or
// "replay the failing seed" is a promise the suite cannot keep.
func TestSimulationIsDeterministic(t *testing.T) {
	for _, seed := range []int64{1, 2, 20261111} {
		a, errA := runSim(defaultSimConfig(seed))
		b, errB := runSim(defaultSimConfig(seed))
		if errA != nil || errB != nil {
			t.Fatalf("seed %d: %v / %v", seed, errA, errB)
		}
		if a != b {
			t.Fatalf("seed %d ran differently twice:\n%+v\n%+v", seed, a, b)
		}
	}
}

func figure8Weather(cfg *simConfig) {
	cfg.nodes = 5
	cfg.batch = 1
	cfg.crashP = 0.06
	cfg.restartP = 0.10
	cfg.dropP = 0.25
}

// The checks above are only worth something if they fail when Raft is
// wrong. Each case here breaks one rule inside the node and requires the
// simulator to notice, and to name the property that broke.
func TestSimulationCatchesBrokenRaft(t *testing.T) {
	cases := []struct {
		name   string
		faults faults
		tune   func(*simConfig)
		first  int64    // a seed known to expose it, tried before scanning
		want   []string // any of these properties
	}{
		{"votes twice in one term", faults{doubleVote: true}, nil, 2, []string{"election safety"}},
		{"votes for a stale log", faults{voteForStaleLog: true}, nil, 1, []string{"leader completeness", "state machine safety", "log matching"}},
		// Figure 8 needs a leader to die between committing an inherited
		// entry and replicating anything of its own, then lose the next
		// election to a node that never saw that entry. One entry per
		// message and frequent crashes make that window wide enough to hit.
		{"commits an old-term entry by counting", faults{commitOldTerm: true}, figure8Weather, 6446, []string{"leader completeness", "state machine safety", "log matching"}},
		// A snapshot is sent, the follower catches up past it some other
		// way, and then the snapshot arrives late or twice. Installing it
		// throws away committed entries.
		{"installs a snapshot behind its commit index", faults{installStale: true}, nil, 22, []string{"apply order", "leader completeness", "state machine safety", "log matching"}},
	}
	const maxSeeds = 20000
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for i := int64(0); i <= maxSeeds; i++ {
				seed := i
				if i == 0 {
					seed = tc.first
				}
				cfg := defaultSimConfig(seed)
				cfg.faults = tc.faults
				if tc.tune != nil {
					tc.tune(&cfg)
				}
				_, err := runSim(cfg)
				if err == nil {
					continue
				}
				inv, ok := err.(*invariantError)
				if !ok {
					// The node's own assertions (two leaders in a term,
					// overwriting a committed entry) may fire first.
					t.Logf("seed %d: caught by the node itself: %v", seed, err)
					return
				}
				for _, w := range tc.want {
					if inv.property == w {
						t.Logf("seed %d: %v", seed, err)
						return
					}
				}
				t.Fatalf("seed %d: caught as %q, want one of %v: %v", seed, inv.property, tc.want, err)
			}
			t.Fatalf("%d seeds ran clean with the bug in place: the checks cannot see it", maxSeeds)
		})
	}
}
