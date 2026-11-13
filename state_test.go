package jobq

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"
)

// stateQueue is a queue with everything local taken away: a manual clock
// and counted IDs, so two of them given the same operations are the same.
type stateQueue struct {
	q   *Queue
	ids int
}

func (s *stateQueue) nextID() string {
	s.ids++
	return fmt.Sprintf("%032x", s.ids)
}

// The property a snapshot has to have: a queue rebuilt from State is
// indistinguishable, from then on, from the queue it was taken from. One
// queue runs a random history straight through. A second is replaced by a
// copy of itself, built from its own State, at random points along the
// way. They must end equal, down to lease IDs and the order of every list.
func TestQueueRebuiltFromStateBehavesTheSame(t *testing.T) {
	for seed := int64(1); seed <= 30; seed++ {
		rng := rand.New(rand.NewSource(seed))
		clk := newTestClock()
		a, b := &stateQueue{}, &stateQueue{}
		a.q = New(WithClock(clk.Now), WithIDSource(a.nextID), WithPassiveReads())
		b.q = New(WithClock(clk.Now), WithIDSource(b.nextID), WithPassiveReads())
		topics := []string{"work", "mail"}
		var leases []Lease
		rebuilt := 0

		for step := 0; step < 400; step++ {
			topic := topics[rng.Intn(len(topics))]
			if rng.Intn(4) == 0 && rng.Intn(2) == 0 {
				topic = DeadLetterTopic(topic)
			}
			switch op := rng.Intn(10); {
			case op < 3:
				if topic == DeadLetterTopic("work") || topic == DeadLetterTopic("mail") {
					continue
				}
				var opts []EnqueueOption
				if rng.Intn(4) == 0 {
					opts = append(opts, WithDelay(time.Duration(rng.Intn(5))*time.Second))
				}
				if rng.Intn(3) == 0 {
					opts = append(opts, WithMaxAttempts(1+rng.Intn(2)))
				}
				payload := []byte(fmt.Sprintf("job-%d", step))
				ia, ea := a.q.Enqueue(topic, payload, opts...)
				ib, eb := b.q.Enqueue(topic, payload, opts...)
				if ia != ib || ea != eb {
					t.Fatalf("seed %d step %d: enqueue gave %q/%v and %q/%v", seed, step, ia, ea, ib, eb)
				}
			case op < 6:
				vis := time.Duration(1+rng.Intn(4)) * time.Second
				ja, la, ea := a.q.Dequeue(topic, vis)
				jb, lb, eb := b.q.Dequeue(topic, vis)
				if ea != eb || la != lb || !reflect.DeepEqual(ja, jb) {
					t.Fatalf("seed %d step %d: dequeue gave %+v/%+v/%v and %+v/%+v/%v", seed, step, ja, la, ea, jb, lb, eb)
				}
				if ea == nil {
					leases = append(leases, la)
				}
			case op < 9 && len(leases) > 0:
				i := rng.Intn(len(leases))
				l := leases[i]
				var ea, eb error
				switch rng.Intn(3) {
				case 0:
					ea, eb = a.q.Ack(l), b.q.Ack(l)
				case 1:
					ea, eb = a.q.Nack(l), b.q.Nack(l)
				case 2:
					d := time.Duration(1+rng.Intn(3)) * time.Second
					var da, db time.Time
					da, ea = a.q.Extend(l, d)
					db, eb = b.q.Extend(l, d)
					if !da.Equal(db) {
						t.Fatalf("seed %d step %d: extend gave %v and %v", seed, step, da, db)
					}
				}
				if ea != eb {
					t.Fatalf("seed %d step %d: settling %s gave %v and %v", seed, step, l.ID, ea, eb)
				}
				if rng.Intn(2) == 0 {
					leases = append(leases[:i], leases[i+1:]...)
				}
			default:
				clk.Advance(time.Duration(rng.Intn(1500)) * time.Millisecond)
			}

			if rng.Intn(25) == 0 {
				fresh, err := NewFromState(b.q.State(), WithClock(clk.Now), WithIDSource(b.nextID), WithPassiveReads())
				if err != nil {
					t.Fatalf("seed %d step %d: NewFromState: %v", seed, step, err)
				}
				_ = b.q.Close()
				b.q = fresh
				rebuilt++
			}
		}

		sa, sb := a.q.State(), b.q.State()
		if !reflect.DeepEqual(sa, sb) {
			t.Fatalf("seed %d: after %d rebuilds the queues differ:\n%s\n%s", seed, rebuilt, describe(sa), describe(sb))
		}
		// Reads agree too, including the order Peek lists leases in.
		for _, name := range a.q.Topics() {
			ra, fa, _ := a.q.Peek(name, 0, true)
			rb, fb, _ := b.q.Peek(name, 0, true)
			if !reflect.DeepEqual(ra, rb) || !reflect.DeepEqual(fa, fb) || a.q.Stats(name) != b.q.Stats(name) {
				t.Fatalf("seed %d: topic %s reads differently after a rebuild", seed, name)
			}
		}
		if rebuilt == 0 {
			t.Fatalf("seed %d never rebuilt the queue", seed)
		}
		_ = a.q.Close()
		_ = b.q.Close()
	}
}

func describe(st QueueState) string {
	out := ""
	for _, ts := range st.Topics {
		out += fmt.Sprintf("  %s: %+v ready=[", ts.Name, ts.Stats)
		for _, j := range ts.Ready {
			out += fmt.Sprintf(" %s/%d", j.ID[26:], j.Attempts)
		}
		out += " ] leased=["
		for _, l := range ts.Leased {
			out += fmt.Sprintf(" %s:%s@%s", l.Job.ID[26:], l.LeaseID[26:], l.Deadline.Format("05.000"))
		}
		out += " ]\n"
	}
	return out
}

// State promises an order that does not depend on how the queue got here.
// Leases come out soonest deadline first, whatever order they were issued
// in, and Peek lists in-flight jobs the same way.
func TestStateListsLeasesSoonestFirst(t *testing.T) {
	clk := newTestClock()
	q := New(WithClock(clk.Now))
	defer q.Close()
	var want [3]string
	for i, vis := range []time.Duration{3 * time.Second, time.Second, 2 * time.Second} {
		if _, err := q.Enqueue("work", []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
		job, _, err := q.Dequeue("work", vis)
		if err != nil {
			t.Fatal(err)
		}
		want[vis/time.Second-1] = job.ID
	}
	leased := q.State().Topics[0].Leased
	_, inflight, _ := q.Peek("work", 0, true)
	for i := range want {
		if leased[i].Job.ID != want[i] {
			t.Fatalf("State lists lease %d as job %s, want the one expiring %d", i, leased[i].Job.ID, i+1)
		}
		if inflight[i].ID != want[i] {
			t.Fatalf("Peek lists in-flight job %d as %s, want the one expiring %d", i, inflight[i].ID, i+1)
		}
	}
}

func TestStateIsACopy(t *testing.T) {
	q := New()
	defer q.Close()
	if _, err := q.Enqueue("work", []byte("abc")); err != nil {
		t.Fatal(err)
	}
	st := q.State()
	st.Topics[0].Ready[0].Payload[0] = 'X'
	st.Topics[0].Ready[0].Attempts = 99
	job, _, err := q.Dequeue("work", time.Minute)
	if err != nil || string(job.Payload) != "abc" || job.Attempts != 1 {
		t.Fatalf("changing a State changed the queue: %+v, %v", job, err)
	}
}

func TestStateDoesNotObserveExpiry(t *testing.T) {
	clk := newTestClock()
	q := New(WithClock(clk.Now)) // not passive: Stats would observe
	defer q.Close()
	_, _ = q.Enqueue("work", []byte("x"))
	if _, _, err := q.Dequeue("work", time.Second); err != nil {
		t.Fatal(err)
	}
	clk.Advance(time.Minute)
	st := q.State()
	if len(st.Topics[0].Leased) != 1 || len(st.Topics[0].Ready) != 0 {
		t.Fatalf("State moved an expired lease: %s", describe(st))
	}
}

func TestNewFromStateRefusesNonsense(t *testing.T) {
	job := &Job{ID: "a", Topic: "work"}
	for name, st := range map[string]QueueState{
		"empty topic name": {Topics: []TopicState{{Name: " "}}},
		"topic twice":      {Topics: []TopicState{{Name: "work"}, {Name: "work"}}},
		"missing job":      {Topics: []TopicState{{Name: "work", Ready: []*Job{nil}}}},
		"lease without id": {Topics: []TopicState{{Name: "work", Leased: []LeasedJob{{Job: job}}}}},
		"lease id twice":   {Topics: []TopicState{{Name: "work", Leased: []LeasedJob{{Job: job, LeaseID: "l"}, {Job: job, LeaseID: "l"}}}}},
	} {
		if q, err := NewFromState(st); err == nil {
			_ = q.Close()
			t.Errorf("%s: accepted", name)
		}
	}
}
