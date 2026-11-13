package pgtx_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	trmpgx "github.com/avito-tech/go-transaction-manager/drivers/pgxv5/v2"
	"github.com/avito-tech/go-transaction-manager/trm/v2"
	"github.com/avito-tech/go-transaction-manager/trm/v2/settings"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/gopherex/pgtx"
	"github.com/gopherex/pgtx/pkg/tx"
)

// countingBegin stands in for a caller's wrapper in front of the pool (for
// example a bounded acquire): it counts every BeginTx and can refuse them.
type countingBegin struct {
	next   trmpgx.Transactional
	refuse error
	begins atomic.Int32
}

func (c *countingBegin) BeginTx(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error) {
	c.begins.Add(1)
	if c.refuse != nil {
		return nil, c.refuse
	}
	return c.next.BeginTx(ctx, opts)
}

// A refusal of the wrapper reaches the caller of Do unchanged, and the work
// never runs.
func TestNewTxManagerWrapperRefusal(t *testing.T) {
	t.Parallel()
	errAcquire := errors.New("acquire timeout")
	begin := &countingBegin{refuse: errAcquire}
	mgr, err := pgtx.NewTxManager(begin, tx.ReadCommitted())
	if err != nil {
		t.Fatal(err)
	}
	ran := false
	err = mgr.Do(t.Context(), func(context.Context) error { ran = true; return nil })
	if !errors.Is(err, errAcquire) || ran || begin.begins.Load() != 1 {
		t.Fatalf("Do = %v, ran = %v, begins = %d; want the refusal, no work, one begin", err, ran, begin.begins.Load())
	}
}

// Every top-level transaction begins through the wrapper; a nested one is a
// savepoint on the parent and never calls it. Needs PGTX_TEST_PG.
func TestNewTxManagerBeginsThroughWrapper(t *testing.T) {
	pool, err := pgxpool.New(t.Context(), pgDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	begin := &countingBegin{next: pool}
	mgr, err := pgtx.NewTxManager(begin, tx.ReadCommitted())
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	if _, err := pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS pgtx_wrapped(n int); TRUNCATE pgtx_wrapped`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = pool.Exec(context.Background(), `DROP TABLE IF EXISTS pgtx_wrapped`) })

	txDB := pgtx.NewTxDB(pool)
	nested := tx.NewSettings(pgx.ReadCommitted, settings.WithPropagation(trm.PropagationNested))
	insert := func(ctx context.Context, n int) error {
		_, err := txDB.Exec(ctx, `INSERT INTO pgtx_wrapped VALUES ($1)`, n)
		return err
	}
	err = mgr.Do(ctx, func(ctx context.Context) error {
		if err := insert(ctx, 1); err != nil {
			return err
		}
		return mgr.DoWithSettings(ctx, nested, func(ctx context.Context) error { return insert(ctx, 2) })
	})
	if err != nil {
		t.Fatal(err)
	}
	for n := 3; n <= 4; n++ {
		if err := mgr.Do(ctx, func(ctx context.Context) error { return insert(ctx, n) }); err != nil {
			t.Fatal(err)
		}
	}
	if got := begin.begins.Load(); got != 3 {
		t.Fatalf("wrapper began %d transactions, want 3 (the nested one is a savepoint)", got)
	}
	var rows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM pgtx_wrapped`).Scan(&rows); err != nil || rows != 4 {
		t.Fatalf("committed rows = %d, %v; want 4", rows, err)
	}
}
