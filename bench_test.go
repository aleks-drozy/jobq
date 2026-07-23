package jobq

// The fsync-policy cost sheet. Run with:
//
//	go test -bench BenchmarkEnqueue -benchtime 2s -run XXX .
//
// Sequential shows the raw per-call price of each durability level; the
// parallel variant shows group commit amortizing the fsync across
// concurrent producers under SyncAlways.

import (
	"fmt"
	"testing"
)

var benchPayload = make([]byte, 256)

func benchQueue(b *testing.B, policy SyncPolicy) *Queue {
	b.Helper()
	q, _, err := Open(b.TempDir(), WithSync(policy))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = q.Close() })
	return q
}

func BenchmarkEnqueue(b *testing.B) {
	for _, tc := range []struct {
		name   string
		policy SyncPolicy
	}{
		{"always", SyncAlways},
		{"interval5ms", SyncInterval},
		{"never", SyncNever},
	} {
		b.Run("policy="+tc.name, func(b *testing.B) {
			q := benchQueue(b, tc.policy)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := q.Enqueue("bench", benchPayload); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run("policy="+tc.name+"/parallel", func(b *testing.B) {
			q := benchQueue(b, tc.policy)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if _, err := q.Enqueue("bench", benchPayload); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

func BenchmarkDequeueAck(b *testing.B) {
	q := benchQueue(b, SyncInterval)
	for i := 0; i < b.N; i++ {
		if _, err := q.Enqueue("bench", benchPayload); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, lease, err := q.Dequeue("bench", benchVisibility)
		if err != nil {
			b.Fatal(err)
		}
		if err := q.Ack(lease); err != nil {
			b.Fatal(err)
		}
	}
}

const benchVisibility = 1 << 40 // effectively forever; expiry is not what's measured

var _ = fmt.Sprintf // keep fmt if future benches drop it
