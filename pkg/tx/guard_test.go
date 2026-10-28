package tx

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	trmpgx "github.com/avito-tech/go-transaction-manager/drivers/pgxv5/v2"
	trmcontext "github.com/avito-tech/go-transaction-manager/trm/v2/context"
	"github.com/avito-tech/go-transaction-manager/trm/v2/settings"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// stubTx is a pgx.Tx stand-in mirroring the parts of pgx's real release
// semantics the guard must track: rows free their call on Close, on Next
// returning false, and on a failed Scan or Values; Row frees on Scan; a batch
// frees on Close. execBlock makes Exec busy until the test closes the channel,
// standing in for a long-running statement.
type stubTx struct {
	execs     atomic.Int64
	queries   atomic.Int64
	execBlock chan struct{} // when non-nil, Exec waits here
	started   chan struct{} // buffered, cap 1: signalled on entry to a blocked Exec
	err       error         // every call that can fail returns it
	rows      pgx.Rows      // non-nil: Query returns them instead of a fresh set
	row       pgx.Row
	batch     pgx.BatchResults
	child     pgx.Tx // non-nil: Begin returns it instead of a fresh fake
}

var _ pgx.Tx = (*stubTx)(nil)

func (f *stubTx) Begin(context.Context) (pgx.Tx, error) {
	if f.err != nil {
		return nil, f.err
	}
	if f.child != nil {
		return f.child, nil
	}
	return &stubTx{}, nil
}

func (f *stubTx) Commit(context.Context) error   { return f.err }
func (f *stubTx) Rollback(context.Context) error { return f.err }

func (f *stubTx) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	f.execs.Add(1)
	if f.execBlock != nil {
		f.started <- struct{}{}
		<-f.execBlock
	}
	return pgconn.CommandTag{}, f.err
}

func (f *stubTx) Query(context.Context, string, ...any) (pgx.Rows, error) {
	f.queries.Add(1)
	if f.err != nil {
		return nil, f.err
	}
	if f.rows != nil {
		return f.rows, nil
	}
	return &stubRows{left: 1}, nil
}

func (f *stubTx) QueryRow(context.Context, string, ...any) pgx.Row {
	if f.err != nil {
		return &stubRow{scanErr: f.err}
	}
	if f.row != nil {
		return f.row
	}
	return &stubRow{}
}

func (f *stubTx) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, f.err
}

func (f *stubTx) SendBatch(context.Context, *pgx.Batch) pgx.BatchResults {
	if f.err != nil {
		return &stubBatch{closeErr: f.err}
	}
	if f.batch != nil {
		return f.batch
	}
	return &stubBatch{}
}

func (f *stubTx) LargeObjects() pgx.LargeObjects { return pgx.LargeObjects{} }

func (f *stubTx) Prepare(context.Context, string, string) (*pgconn.StatementDescription, error) {
	return nil, f.err
}

func (f *stubTx) Conn() *pgx.Conn { return nil }

// stubRows serves Next true left times, then closes itself like pgx does.
type stubRows struct {
	left      int
	scanErr   error
	valuesErr error
	closed    bool
}

var _ pgx.Rows = (*stubRows)(nil)

func (r *stubRows) Close()                                       { r.closed = true }
func (r *stubRows) Err() error                                   { return r.scanErr }
func (r *stubRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *stubRows) FieldDescriptions() []pgconn.FieldDescription { return nil }

func (r *stubRows) Next() bool {
	if r.closed {
		return false
	}
	if r.left > 0 {
		r.left--
		return true
	}
	r.closed = true // pgx auto-closes rows when they run out
	return false
}

func (r *stubRows) Scan(...any) error {
	if r.scanErr != nil {
		r.closed = true // a failed scan closes the rows like fatal() does
	}
	return r.scanErr
}

func (r *stubRows) Values() ([]any, error) {
	if r.valuesErr != nil {
		r.closed = true
		return nil, r.valuesErr
	}
	return []any{}, nil
}

func (r *stubRows) RawValues() [][]byte { return nil }
func (r *stubRows) Conn() *pgx.Conn     { return nil }

type stubRow struct {
	scanErr error
	scanned bool
}

var _ pgx.Row = (*stubRow)(nil)

func (r *stubRow) Scan(...any) error {
	r.scanned = true
	return r.scanErr
}

type stubBatch struct {
	closed   bool
	closeErr error
}

var _ pgx.BatchResults = (*stubBatch)(nil)

func (b *stubBatch) Exec() (pgconn.CommandTag, error) { return pgconn.CommandTag{}, nil }
func (b *stubBatch) Query() (pgx.Rows, error)         { return &stubRows{}, nil }
func (b *stubBatch) QueryRow() pgx.Row                { return &stubRow{} }

func (b *stubBatch) Close() error {
	b.closed = true
	return b.closeErr
}

func newGuarded(f pgx.Tx) *guardedTx {
	return &guardedTx{Tx: f, g: new(guard)}
}

// Sequential calls on one transaction all pass; nothing changes for correct use.
func TestGuardedSequential(t *testing.T) {
	t.Parallel()
	g := newGuarded(&stubTx{})
	ctx := context.Background()

	if _, err := g.Exec(ctx, "SELECT 1"); err != nil {
		t.Fatal(err)
	}
	rows, err := g.Query(ctx, "SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		if err := rows.Scan(new(any)); err != nil {
			t.Fatal(err)
		}
	}
	rows.Close()
	if err := g.QueryRow(ctx, "SELECT 1").Scan(new(any)); err != nil {
		t.Fatal(err)
	}
	if err := g.SendBatch(ctx, &pgx.Batch{}).Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := g.CopyFrom(ctx, pgx.Identifier{"t"}, []string{"a"}, nil); err != nil {
		t.Fatal(err)
	}
	child, err := g.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := child.(*guardedTx).Exec(ctx, "SELECT 2"); err != nil {
		t.Fatal(err)
	}
	if err := g.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

// While a call still holds the transaction — its rows, row or batch not yet
// finished — every other call on the whole savepoint chain is refused with
// ErrTxInUse, and the transaction stays usable after the holder is freed.
func TestGuardedRefusesUntilFreed(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		// hold takes the guard and returns the call that frees it
		hold func(t *testing.T, g *guardedTx) (release func())
		// busy is the call that must be refused while the guard is taken
		busy func(g *guardedTx) error
	}{
		{
			name: "rows not closed block Exec",
			hold: holdOpenRows,
			busy: execBusy,
		},
		{
			name: "rows not closed block a savepoint Begin",
			hold: holdOpenRows,
			busy: func(g *guardedTx) error {
				child, err := g.Begin(context.Background())
				if err != nil {
					return err
				}
				_, err = child.(*guardedTx).Exec(context.Background(), "SELECT 2")
				return err
			},
		},
		{
			name: "a child call blocks the parent",
			hold: func(t *testing.T, g *guardedTx) func() {
				child, err := g.Begin(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				return holdOpenRows(t, child.(*guardedTx))
			},
			busy: execBusy,
		},
		{
			name: "an unscanned row blocks Exec",
			hold: func(t *testing.T, g *guardedTx) func() {
				row := g.QueryRow(context.Background(), "SELECT 1")
				return func() {
					if err := row.Scan(new(any)); err != nil {
						t.Error(err)
					}
				}
			},
			busy: execBusy,
		},
		{
			name: "an unclosed batch blocks Exec",
			hold: func(t *testing.T, g *guardedTx) func() {
				res := g.SendBatch(context.Background(), &pgx.Batch{})
				return func() {
					if err := res.Close(); err != nil {
						t.Error(err)
					}
				}
			},
			busy: execBusy,
		},
		{
			name: "rows not closed block a second Query",
			hold: holdOpenRows,
			busy: func(g *guardedTx) error {
				_, err := g.Query(context.Background(), "SELECT 2")
				return err
			},
		},
		{
			name: "rows not closed block CopyFrom",
			hold: holdOpenRows,
			busy: func(g *guardedTx) error {
				_, err := g.CopyFrom(context.Background(), pgx.Identifier{"t"}, []string{"a"}, nil)
				return err
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			g := newGuarded(&stubTx{})
			release := tc.hold(t, g)

			if err := tc.busy(g); !errors.Is(err, ErrTxInUse) {
				t.Fatalf("busy call err = %v, want ErrTxInUse", err)
			}
			release()
			if _, err := g.Exec(context.Background(), "SELECT 3"); err != nil {
				t.Fatalf("after release: %v", err)
			}
		})
	}
}

func holdOpenRows(t *testing.T, g *guardedTx) func() {
	rows, err := g.Query(context.Background(), "SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	if !rows.Next() {
		t.Fatal("want one row")
	}
	return rows.Close
}

func execBusy(g *guardedTx) error {
	_, err := g.Exec(context.Background(), "SELECT 2")
	return err
}

// Refused QueryRow and SendBatch report ErrTxInUse through the result object,
// which has no error slot of its own.
func TestGuardedRefusedResultsReportErr(t *testing.T) {
	t.Parallel()
	g := newGuarded(&stubTx{})
	release := holdOpenRows(t, g)
	defer release()

	ctx := context.Background()
	if err := g.QueryRow(ctx, "SELECT 2").Scan(new(any)); !errors.Is(err, ErrTxInUse) {
		t.Fatalf("QueryRow().Scan err = %v, want ErrTxInUse", err)
	}
	res := g.SendBatch(ctx, &pgx.Batch{})
	if _, err := res.Exec(); !errors.Is(err, ErrTxInUse) {
		t.Fatalf("batch Exec err = %v, want ErrTxInUse", err)
	}
	if _, err := res.Query(); !errors.Is(err, ErrTxInUse) {
		t.Fatalf("batch Query err = %v, want ErrTxInUse", err)
	}
	if err := res.QueryRow().Scan(new(any)); !errors.Is(err, ErrTxInUse) {
		t.Fatalf("batch QueryRow().Scan err = %v, want ErrTxInUse", err)
	}
	if err := res.Close(); !errors.Is(err, ErrTxInUse) {
		t.Fatalf("batch Close err = %v, want ErrTxInUse", err)
	}
}

// Rows free the transaction as soon as pgx frees the connection: drained by
// Next returning false, or closed by a failed Scan or Values — no explicit
// Close needed.
func TestGuardedReleasesAsPgxDoes(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		rows *stubRows
		// run consumes the rows the way the name says and leaves them closed
		run func(t *testing.T, rows pgx.Rows)
	}{
		{"drained by Next", &stubRows{left: 2}, func(t *testing.T, rows pgx.Rows) {
			for rows.Next() {
				if err := rows.Scan(new(any)); err != nil {
					t.Fatal(err)
				}
			}
		}},
		{"failed Scan", &stubRows{left: 1, scanErr: errors.New("bad scan")}, func(t *testing.T, rows pgx.Rows) {
			rows.Next()
			if err := rows.Scan(new(any)); err == nil {
				t.Fatal("want Scan error")
			}
		}},
		{"failed Values", &stubRows{left: 1, valuesErr: errors.New("bad values")}, func(t *testing.T, rows pgx.Rows) {
			rows.Next()
			if _, err := rows.Values(); err == nil {
				t.Fatal("want Values error")
			}
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			g := newGuarded(&stubTx{rows: tc.rows})
			rows, err := g.Query(context.Background(), "SELECT 1")
			if err != nil {
				t.Fatal(err)
			}
			tc.run(t, rows)
			if !tc.rows.closed {
				t.Fatal("fake rows not closed by the run")
			}
			if _, err := g.Exec(context.Background(), "SELECT 2"); err != nil {
				t.Fatalf("after %s: %v", tc.name, err)
			}
		})
	}
}

// Releasing the guard is once-per-acquire: an extra Close on finished rows
// must not free a transaction a later call re-acquired.
func TestGuardedReleaseIsOncePerAcquire(t *testing.T) {
	t.Parallel()
	block := make(chan struct{})
	started := make(chan struct{}, 1)
	g := newGuarded(&stubTx{execBlock: block, started: started, rows: &stubRows{left: 1}})
	ctx := context.Background()

	rows, err := g.Query(ctx, "SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	rows.Next()  // consume the row
	rows.Close() // this Close is the release of the Query's acquire

	var wg sync.WaitGroup
	wg.Go(func() { _, _ = g.Exec(ctx, "SELECT slow") })
	<-started // the Exec holds the guard now

	rows.Close() // stale close must NOT free the transaction
	if _, err := g.Query(ctx, "SELECT 2"); !errors.Is(err, ErrTxInUse) {
		t.Fatalf("concurrent Query err = %v, want ErrTxInUse", err)
	}

	close(block)
	wg.Wait()
	if _, err := g.Query(ctx, "SELECT 3"); err != nil {
		t.Fatalf("after holder finished: %v", err)
	}
}

// Inner errors pass through unchanged and do not wedge the guard.
func TestGuardedPassesThroughErrors(t *testing.T) {
	t.Parallel()
	boom := errors.New("inner boom")
	cases := []struct {
		name string
		call func(g *guardedTx) error
	}{
		{"Exec", execBusy},
		{"Query", func(g *guardedTx) error { _, err := g.Query(context.Background(), "SELECT 1"); return err }},
		{"QueryRow", func(g *guardedTx) error { return g.QueryRow(context.Background(), "SELECT 1").Scan(new(any)) }},
		{"CopyFrom", func(g *guardedTx) error {
			_, err := g.CopyFrom(context.Background(), pgx.Identifier{"t"}, []string{"a"}, nil)
			return err
		}},
		{"Begin", func(g *guardedTx) error { _, err := g.Begin(context.Background()); return err }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			g := newGuarded(&stubTx{err: boom})
			if err := tc.call(g); !errors.Is(err, boom) {
				t.Fatalf("err = %v, want %v", err, boom)
			}
			// the failed call released the guard: the next call reaches pgx
			// and reports the inner error again, never ErrTxInUse
			if err := tc.call(g); !errors.Is(err, boom) || errors.Is(err, ErrTxInUse) {
				t.Fatalf("guard wedged after failed %s: err = %v", tc.name, err)
			}
		})
	}
}

// GuardedFactory hands the manager transactions that the executor — routed by
// context, exactly as in production — sees as guarded, while the pool path
// outside any transaction stays unguarded and concurrent.
func TestGuardedFactoryAndExecutor(t *testing.T) {
	t.Parallel()
	inner := &stubTx{}
	factory := GuardedFactory(&stubTransactional{tx: inner})

	ctx, tr, err := factory(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	trCtx := trmcontext.DefaultManager.SetByKey(ctx, settings.DefaultCtxKey, tr)

	db := NewTxDb(inner)

	rows, err := db.Query(trCtx, "SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	if !rows.Next() {
		t.Fatal("want one row")
	}
	if _, err := db.Exec(trCtx, "SELECT 2"); !errors.Is(err, ErrTxInUse) {
		t.Fatalf("in-transaction Exec err = %v, want ErrTxInUse", err)
	}
	rows.Close()
	if _, err := db.Exec(trCtx, "SELECT 3"); err != nil {
		t.Fatalf("after rows closed: %v", err)
	}

	// outside the transaction the executor runs on the pool: never guarded
	if _, err := db.Exec(ctx, "SELECT 4"); err != nil {
		t.Fatalf("pool-path Exec: %v", err)
	}
	if got := inner.execs.Load(); got != 2 { // SELECT 3 and SELECT 4
		t.Fatalf("inner execs = %d, want 2", got)
	}
}

type stubTransactional struct{ tx pgx.Tx }

func (f *stubTransactional) BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error) {
	return f.tx, nil
}

var _ trmpgx.Transactional = (*stubTransactional)(nil)

func TestGuardedFactoryBeginTxError(t *testing.T) {
	t.Parallel()
	boom := errors.New("begin boom")
	factory := GuardedFactory(&errTransactional{err: boom})
	if _, _, err := factory(context.Background(), nil); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want %v", err, boom)
	}
}

type errTransactional struct{ err error }

func (f *errTransactional) BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error) {
	return nil, f.err
}

// The guard itself: one slot, acquire refuses a second holder, release frees it.
func TestGuard(t *testing.T) {
	t.Parallel()
	g := new(guard)
	if err := g.acquire(); err != nil {
		t.Fatal(err)
	}
	if err := g.acquire(); !errors.Is(err, ErrTxInUse) {
		t.Fatalf("second acquire err = %v, want ErrTxInUse", err)
	}
	g.release()
	if err := g.acquire(); err != nil {
		t.Fatal(err)
	}

	release := g.releaseOnce()
	release() // frees the acquire above
	if err := g.acquire(); err != nil {
		t.Fatal(err) // a new holder takes the slot
	}
	release() // stale: once-per-acquire, it must not free the new holder
	if err := g.acquire(); !errors.Is(err, ErrTxInUse) {
		t.Fatalf("acquire while the new holder is busy err = %v, want ErrTxInUse", err)
	}
	g.release()
}

var benchCtx = context.Background()

// The sequential overhead of the guard per statement: acquire, release, and
// the result wrappers on top of the same call chain.
func BenchmarkExecUnguarded(b *testing.B) {
	var tx pgx.Tx = &stubTx{}
	for b.Loop() {
		if _, err := tx.Exec(benchCtx, "SELECT 1"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkExecGuarded(b *testing.B) {
	g := newGuarded(&stubTx{})
	for b.Loop() {
		if _, err := g.Exec(benchCtx, "SELECT 1"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQueryGuarded(b *testing.B) {
	g := newGuarded(&stubTx{rows: &stubRows{left: 1}})
	for b.Loop() {
		rows, err := g.Query(benchCtx, "SELECT 1")
		if err != nil {
			b.Fatal(err)
		}
		for rows.Next() {
		}
	}
}
