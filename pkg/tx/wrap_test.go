package tx

import (
	"context"
	"testing"
	"time"

	"github.com/avito-tech/go-transaction-manager/trm/v2"
	trmcontext "github.com/avito-tech/go-transaction-manager/trm/v2/context"
	"github.com/avito-tech/go-transaction-manager/trm/v2/settings"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// fakeTrm opens a transaction (puts it in the context under the default key)
// unless one is open, as trm's manager does with PropagationRequired.
type fakeTrm struct{ opened int }

type fakeTx struct{}

func (fakeTx) Transaction() interface{}       { return nil }
func (fakeTx) Commit(context.Context) error   { return nil }
func (fakeTx) Rollback(context.Context) error { return nil }
func (fakeTx) IsActive() bool                 { return true }
func (fakeTx) Closed() <-chan struct{}        { return nil }

func (f *fakeTrm) Do(ctx context.Context, fn func(context.Context) error) error {
	return f.DoWithSettings(ctx, nil, fn)
}

func (f *fakeTrm) DoWithSettings(ctx context.Context, _ trm.Settings, fn func(context.Context) error) error {
	if trmcontext.DefaultManager.ByKey(ctx, settings.DefaultCtxKey) == nil {
		f.opened++
		ctx = trmcontext.DefaultManager.SetByKey(ctx, settings.DefaultCtxKey, fakeTx{})
	}
	return fn(ctx)
}

// A nested Do joins the open transaction and never retries on its own: a
// serialization failure aborts the whole transaction, so the outermost call
// starts it again from scratch.
func TestDo_NestedDoesNotRetry(t *testing.T) {
	manager := &fakeTrm{}
	policy := WithRetry(RetryPolicy{MaxAttempts: 3, Backoff: NewExpBackoff(time.Microsecond, 0, false)})
	var outer, inner int
	err := Do(context.Background(), manager, pgx.Serializable, func(ctx context.Context) error {
		outer++
		return Do(ctx, manager, pgx.Serializable, func(context.Context) error {
			inner++
			if outer < 2 {
				return &pgconn.PgError{Code: "40001"}
			}
			return nil
		}, policy)
	}, policy)
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if outer != 2 || inner != 2 {
		t.Fatalf("outer = %d, inner = %d; want 2 and 2 (one inner run per transaction)", outer, inner)
	}
	if manager.opened != 2 {
		t.Fatalf("transactions opened = %d, want 2", manager.opened)
	}
}

// A top-level Do retries a transient failure in a new transaction.
func TestDo_TopLevelRetries(t *testing.T) {
	manager := &fakeTrm{}
	var calls int
	err := Do(context.Background(), manager, pgx.Serializable, func(context.Context) error {
		calls++
		if calls < 3 {
			return &pgconn.PgError{Code: "40P01"}
		}
		return nil
	}, WithRetry(RetryPolicy{MaxAttempts: 5, Backoff: NewExpBackoff(time.Microsecond, 0, false)}))
	if err != nil || calls != 3 || manager.opened != 3 {
		t.Fatalf("err = %v, calls = %d, opened = %d; want nil, 3, 3", err, calls, manager.opened)
	}
}
