package tx

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"

	"github.com/avito-tech/go-transaction-manager/trm/settings"
	"github.com/jackc/pgx/v5/pgconn"
)

// Backoff yields the delay to wait before each retry. It is stateful: a fresh
// instance is created per transaction run via BackoffFactory, so it may track
// the attempt count internally.
//
// The signature matches common Go retry libraries (sethvargo/go-retry,
// cenkalti/backoff), so an external implementation can be adapted in a few
// lines without pulling the dependency into pgtx core.
type Backoff interface {
	// Next returns the delay before the next retry. ok=false stops retrying.
	Next() (delay time.Duration, ok bool)
}

// BackoffFactory builds a fresh Backoff for a single transaction run.
type BackoffFactory func() Backoff

// RetryPolicy controls how a transaction is retried when it fails with a
// transient error (serialization failure or deadlock).
//
// Retry re-runs the whole transactional function from scratch in a new
// transaction. The function MUST therefore be safe to run more than once:
// non-database side effects (sending mail, mutating external state) will be
// repeated on every attempt.
//
// Retry only makes sense for top-level transactions. Enabling it on a nested
// transaction that joins a parent is unsafe: once the parent is aborted, a
// re-run cannot succeed.
type RetryPolicy struct {
	// MaxAttempts is the total number of attempts, including the first one.
	// Values <= 1 disable retries. Retrying also stops early if Backoff
	// returns ok=false.
	MaxAttempts int
	// Backoff builds the delay strategy between attempts. nil falls back to
	// DefaultBackoff.
	Backoff BackoffFactory
	// Retryable decides whether err is transient and worth retrying.
	// nil falls back to IsRetryable.
	Retryable func(error) bool
}

// DefaultBackoff is exponential (5ms base, 1s cap) with full jitter.
var DefaultBackoff = NewExpBackoff(5*time.Millisecond, time.Second, true)

// DefaultRetryPolicy is a sane starting point for serializable workloads.
var DefaultRetryPolicy = RetryPolicy{
	MaxAttempts: 5,
	Backoff:     DefaultBackoff,
}

// IsRetryable reports whether err is a transient Postgres error that warrants
// retrying the entire transaction: serialization_failure (40001) or
// deadlock_detected (40P01).
func IsRetryable(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "40001", "40P01":
			return true
		}
	}
	return false
}

// NewExpBackoff returns a factory producing an exponential backoff: delay
// doubles each attempt starting from base, capped at maxDelay (0 = no cap).
// With jitter the delay is randomized in [0, computed].
func NewExpBackoff(base, maxDelay time.Duration, jitter bool) BackoffFactory {
	return func() Backoff {
		return &expBackoff{base: base, maxDelay: maxDelay, jitter: jitter}
	}
}

type expBackoff struct {
	base     time.Duration
	maxDelay time.Duration
	jitter   bool
	attempt  int
}

func (b *expBackoff) Next() (time.Duration, bool) {
	d := b.base << b.attempt //nolint:gosec // attempt bounded by MaxAttempts
	if d < b.base {           // overflow guard
		d = b.maxDelay
	}
	if b.maxDelay > 0 && d > b.maxDelay {
		d = b.maxDelay
	}
	b.attempt++
	if b.jitter && d > 0 {
		d = time.Duration(rand.Int64N(int64(d) + 1))
	}
	return d, true
}

type config struct {
	retry   RetryPolicy
	trmOpts []settings.Opt
}

// Opt configures a Do* call: retry behavior and/or pass-through trm settings.
type Opt func(*config)

// WithRetry enables transaction retries with the given policy.
func WithRetry(p RetryPolicy) Opt {
	return func(c *config) { c.retry = p }
}

// WithTrmSettings forwards options to the underlying transaction manager
// (timeouts, propagation, etc.).
func WithTrmSettings(opts ...settings.Opt) Opt {
	return func(c *config) { c.trmOpts = append(c.trmOpts, opts...) }
}

func newConfig(opts ...Opt) config {
	var c config
	for _, o := range opts {
		o(&c)
	}
	return c
}

// runWithRetry executes do, retrying on transient errors per policy p.
func runWithRetry(ctx context.Context, p RetryPolicy, do func(context.Context) error) error {
	if p.MaxAttempts <= 1 {
		return do(ctx)
	}

	retryable := p.Retryable
	if retryable == nil {
		retryable = IsRetryable
	}
	factory := p.Backoff
	if factory == nil {
		factory = DefaultBackoff
	}
	backoff := factory()

	var err error
	for attempt := 0; attempt < p.MaxAttempts; attempt++ {
		err = do(ctx)
		if err == nil || !retryable(err) {
			return err
		}
		if attempt == p.MaxAttempts-1 {
			break
		}
		delay, ok := backoff.Next()
		if !ok {
			break
		}
		if werr := sleep(ctx, delay); werr != nil {
			return werr
		}
	}
	return err
}

// sleep waits for d, returning early if ctx is cancelled.
func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
