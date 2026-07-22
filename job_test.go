package jobq

import (
	"errors"
	"testing"
	"time"
)

func TestEnqueueOptionsNormalize(t *testing.T) {
	tests := []struct {
		name string
		in   EnqueueOptions
		want EnqueueOptions
	}{
		{"zero gets default max attempts", EnqueueOptions{}, EnqueueOptions{MaxAttempts: DefaultMaxAttempts}},
		{"explicit max attempts kept", EnqueueOptions{MaxAttempts: 2}, EnqueueOptions{MaxAttempts: 2}},
		{"negative max attempts defaulted", EnqueueOptions{MaxAttempts: -1}, EnqueueOptions{MaxAttempts: DefaultMaxAttempts}},
		{"negative delay clamped", EnqueueOptions{Delay: -time.Second}, EnqueueOptions{MaxAttempts: DefaultMaxAttempts}},
		{"delay preserved", EnqueueOptions{Delay: time.Minute}, EnqueueOptions{Delay: time.Minute, MaxAttempts: DefaultMaxAttempts}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.in.normalize(); got != tt.want {
				t.Errorf("normalize() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestSentinelErrorsAreDistinct(t *testing.T) {
	all := []error{ErrNoJob, ErrUnknownLease, ErrClosed, ErrEmptyTopic}
	for i, a := range all {
		for j, b := range all {
			if i != j && errors.Is(a, b) {
				t.Errorf("sentinel %v must not match %v", a, b)
			}
		}
	}
}
