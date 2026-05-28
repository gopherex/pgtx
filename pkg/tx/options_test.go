package tx

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestIsRetryable(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"serialization_failure", &pgconn.PgError{Code: "40001"}, true},
		{"deadlock_detected", &pgconn.PgError{Code: "40P01"}, true},
		{"wrapped", errors.Join(errors.New("ctx"), &pgconn.PgError{Code: "40001"}), true},
		{"other_pg", &pgconn.PgError{Code: "23505"}, false},
		{"plain", errors.New("boom"), false},
		{"nil", nil, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := IsRetryable(c.err); got != c.want {
				t.Fatalf("IsRetryable = %v, want %v", got, c.want)
			}
		})
	}
}

func TestRunWithRetry_RetriesUntilSuccess(t *testing.T) {
	p := RetryPolicy{MaxAttempts: 5, Backoff: NewExpBackoff(time.Microsecond, 0, false)}
	var calls int
	err := runWithRetry(context.Background(), p, func(context.Context) error {
		calls++
		if calls < 3 {
			return &pgconn.PgError{Code: "40001"}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if calls != 3 {
		t.Fatalf("calls = %d, want 3", calls)
	}
}

func TestRunWithRetry_ExhaustsAndReturnsLastErr(t *testing.T) {
	p := RetryPolicy{MaxAttempts: 3, Backoff: NewExpBackoff(time.Microsecond, 0, false)}
	last := &pgconn.PgError{Code: "40P01"}
	var calls int
	err := runWithRetry(context.Background(), p, func(context.Context) error {
		calls++
		return last
	})
	if !errors.Is(err, last) {
		t.Fatalf("err = %v, want %v", err, last)
	}
	if calls != 3 {
		t.Fatalf("calls = %d, want 3", calls)
	}
}

func TestRunWithRetry_NonRetryableStopsImmediately(t *testing.T) {
	p := RetryPolicy{MaxAttempts: 5, Backoff: NewExpBackoff(time.Microsecond, 0, false)}
	boom := errors.New("boom")
	var calls int
	err := runWithRetry(context.Background(), p, func(context.Context) error {
		calls++
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want %v", err, boom)
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1", calls)
	}
}

func TestRunWithRetry_Disabled(t *testing.T) {
	var calls int
	err := runWithRetry(context.Background(), RetryPolicy{MaxAttempts: 1}, func(context.Context) error {
		calls++
		return &pgconn.PgError{Code: "40001"}
	})
	if err == nil || calls != 1 {
		t.Fatalf("calls = %d err = %v, want 1 call and error", calls, err)
	}
}

// fixedBackoff stops after a fixed number of retries, ignoring delay.
type fixedBackoff struct {
	left int
}

func (b *fixedBackoff) Next() (time.Duration, bool) {
	if b.left <= 0 {
		return 0, false
	}
	b.left--
	return 0, true
}

func TestRunWithRetry_CustomBackoffStops(t *testing.T) {
	// MaxAttempts is high, but the custom Backoff caps retries at 2 (=> 3 calls).
	p := RetryPolicy{
		MaxAttempts: 100,
		Backoff:     func() Backoff { return &fixedBackoff{left: 2} },
	}
	var calls int
	err := runWithRetry(context.Background(), p, func(context.Context) error {
		calls++
		return &pgconn.PgError{Code: "40001"}
	})
	if err == nil {
		t.Fatal("err = nil, want error")
	}
	if calls != 3 {
		t.Fatalf("calls = %d, want 3", calls)
	}
}

func TestRunWithRetry_ContextCancelStopsBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	p := RetryPolicy{MaxAttempts: 5, Backoff: NewExpBackoff(time.Hour, 0, false)}
	var calls int
	err := runWithRetry(ctx, p, func(context.Context) error {
		calls++
		cancel()
		return &pgconn.PgError{Code: "40001"}
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1", calls)
	}
}
