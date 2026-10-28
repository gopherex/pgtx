package tx

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	trmpgx "github.com/avito-tech/go-transaction-manager/drivers/pgxv5/v2"
	"github.com/avito-tech/go-transaction-manager/trm/v2"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// ErrTxInUse reports that a transaction was used while it was still busy
// with an earlier call: a concurrent query from another goroutine, or a new
// call issued before the rows, row or batch results of an earlier one were
// finished. pgx.Tx supports neither; unguarded, both surface as a busy-conn
// or protocol error that does not name the cause.
//
// Every transaction created through NewTxManager is guarded, so a DB call
// routed by context into one returns this error as its only outcome —
// nothing reaches the connection and the transaction stays usable.
var ErrTxInUse = errors.New("pgtx: transaction in use: concurrent query or previous results not closed")

// guard is the single busy slot of one transaction, held for the full
// conn-holding lifetime of a call — an open Query holds it until its rows
// free the connection, not just until Query returns.
type guard struct{ busy atomic.Bool }

func (g *guard) acquire() error {
	if !g.busy.CompareAndSwap(false, true) {
		return ErrTxInUse
	}
	return nil
}

func (g *guard) release() { g.busy.Store(false) }

// releaseOnce binds a release to one result object (rows, row, batch): a
// stale extra Close must not free a guard a later call has re-acquired.
func (g *guard) releaseOnce() func() {
	return sync.OnceFunc(g.release)
}

// GuardedFactory builds the avito factory over db so that every pgx.Tx it
// begins is serialised by the guard. NewTxManager uses it; the driver
// itself is left untouched, the guard lives on the transaction object.
func GuardedFactory(db trmpgx.Transactional) trm.TrFactory {
	return trmpgx.NewFactory(guardedBegin{db})
}

type guardedBegin struct{ trmpgx.Transactional }

func (b guardedBegin) BeginTx(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error) {
	tx, err := b.Transactional.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &guardedTx{Tx: tx, g: new(guard)}, nil
}

var (
	_ pgx.Tx    = (*guardedTx)(nil)
	_ trmpgx.Tr = (*guardedTx)(nil)
)

// guardedTx is a pgx.Tx whose calls are serialised by the guard. The guard
// is shared with the whole savepoint chain — nested transactions run on the
// same connection — while each top-level transaction gets its own.
//
// A panic inside pgx mid-Query/mid-SendBatch leaves the guard taken: the
// panic aborts the transaction and the manager rolls it back, so the guard
// never outlives the transaction it protects.
type guardedTx struct {
	pgx.Tx
	g *guard
}

// Begin opens a savepoint transaction inheriting the guard: the chain shares
// one connection, so one busy statement must block every level. Begin itself
// runs the SAVEPOINT statement on the connection, so it takes the guard too.
func (t *guardedTx) Begin(ctx context.Context) (pgx.Tx, error) {
	if err := t.g.acquire(); err != nil {
		return nil, err
	}
	tx, err := t.Tx.Begin(ctx)
	if err != nil {
		t.g.release()
		return nil, err
	}
	t.g.release()
	return &guardedTx{Tx: tx, g: t.g}, nil
}

func (t *guardedTx) Exec(
	ctx context.Context,
	sql string,
	arguments ...interface{},
) (pgconn.CommandTag, error) {
	if err := t.g.acquire(); err != nil {
		return pgconn.CommandTag{}, err
	}
	defer t.g.release()
	return t.Tx.Exec(ctx, sql, arguments...)
}

func (t *guardedTx) CopyFrom(
	ctx context.Context,
	tableName pgx.Identifier,
	columnNames []string,
	rowSrc pgx.CopyFromSource,
) (int64, error) {
	if err := t.g.acquire(); err != nil {
		return 0, err
	}
	defer t.g.release()
	return t.Tx.CopyFrom(ctx, tableName, columnNames, rowSrc)
}

// Query keeps the guard until its rows free the connection — closed, drained
// by Next returning false, or failed on Scan — mirroring pgx exactly, so
// drained-then-continue code needs no explicit Close.
func (t *guardedTx) Query(
	ctx context.Context,
	sql string,
	arguments ...interface{},
) (pgx.Rows, error) {
	if err := t.g.acquire(); err != nil {
		return nil, err
	}
	rows, err := t.Tx.Query(ctx, sql, arguments...)
	if err != nil {
		t.g.release()
		return nil, err
	}
	return &guardedRows{Rows: rows, release: t.g.releaseOnce()}, nil
}

// QueryRow keeps the guard until the row is Scanned: pgx holds the connection
// from the call until then.
func (t *guardedTx) QueryRow(
	ctx context.Context,
	sql string,
	arguments ...interface{},
) pgx.Row {
	if err := t.g.acquire(); err != nil {
		return rowError{err}
	}
	return &guardedRow{Row: t.Tx.QueryRow(ctx, sql, arguments...), release: t.g.releaseOnce()}
}

func (t *guardedTx) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	if err := t.g.acquire(); err != nil {
		return &batchError{err}
	}
	res := t.Tx.SendBatch(ctx, b)
	return &guardedBatchResults{BatchResults: res, release: t.g.releaseOnce()}
}

var (
	_ pgx.Rows         = (*guardedRows)(nil)
	_ pgx.Row          = (*guardedRow)(nil)
	_ pgx.BatchResults = (*guardedBatchResults)(nil)
	_ pgx.Row          = rowError{}
	_ pgx.BatchResults = (*batchError)(nil)
)

type guardedRows struct {
	pgx.Rows
	release func()
}

func (r *guardedRows) Close() {
	r.Rows.Close()
	r.release()
}

// Next mirrors the pgx rule that rows close themselves when they run out or
// fail: the connection is free once Next returns false.
func (r *guardedRows) Next() bool {
	if r.Rows.Next() {
		return true
	}
	r.release()
	return false
}

func (r *guardedRows) Scan(dest ...interface{}) error {
	err := r.Rows.Scan(dest...)
	if err != nil {
		r.release() // a failed scan closes the rows
	}
	return err
}

func (r *guardedRows) Values() ([]interface{}, error) {
	values, err := r.Rows.Values()
	if err != nil {
		r.release() // a failed decode closes the rows
	}
	return values, err
}

type guardedRow struct {
	pgx.Row
	release func()
}

func (r *guardedRow) Scan(dest ...interface{}) error {
	err := r.Row.Scan(dest...)
	r.release() // Row.Scan always closes its source rows
	return err
}

type guardedBatchResults struct {
	pgx.BatchResults
	release func()
}

func (r *guardedBatchResults) Close() error {
	err := r.BatchResults.Close()
	r.release() // the batch owns the connection until Close
	return err
}

// rowError and batchError report a refusal through result shapes that have
// no error slot of their own.
type rowError struct{ err error }

func (r rowError) Scan(...interface{}) error { return r.err }

type batchError struct{ err error }

func (r *batchError) Exec() (pgconn.CommandTag, error) { return pgconn.CommandTag{}, r.err }
func (r *batchError) Query() (pgx.Rows, error)         { return nil, r.err }
func (r *batchError) QueryRow() pgx.Row                { return rowError{r.err} }
func (r *batchError) Close() error                     { return r.err }
