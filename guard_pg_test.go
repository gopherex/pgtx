package pgtx_test

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"

	"github.com/avito-tech/go-transaction-manager/trm/v2"
	"github.com/avito-tech/go-transaction-manager/trm/v2/settings"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/gopherex/pgtx"
	"github.com/gopherex/pgtx/pkg/tx"
)

func pgDB(t *testing.T) (tx.DB, tx.Trm) {
	t.Helper()
	dsn := os.Getenv("PGTX_TEST_PG")
	if dsn == "" {
		t.Skip("PGTX_TEST_PG not set")
	}
	pool, err := pgxpool.New(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	mgr, err := pgtx.NewTxManager(pool, tx.ReadCommitted())
	if err != nil {
		t.Fatal(err)
	}
	return pgtx.NewTxDB(pool), mgr
}

// Goroutines spawned with the transaction context are refused with
// ErrTxInUse instead of corrupting the pgx protocol, and the transaction
// survives the refusal: sequential work continues and commits.
// Needs PGTX_TEST_PG.
func TestConcurrentUseOfTransactionRefused(t *testing.T) {
	db, mgr := pgDB(t)

	if _, err := db.Exec(t.Context(), "CREATE TABLE IF NOT EXISTS pgtx_guard (n int)"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = db.Exec(context.Background(), "DROP TABLE IF EXISTS pgtx_guard")
	})

	err := tx.DoReadCommitted(t.Context(), mgr, func(ctx context.Context) error {
		// An open Query holds the connection: every other call must be
		// refused while it stays open.
		rows, err := db.Query(ctx, "SELECT * FROM generate_series(1, 1000)")
		if err != nil {
			return err
		}
		if !rows.Next() {
			rows.Close()
			return errors.New("want a first row")
		}

		const racers = 8
		var wg sync.WaitGroup
		errs := make(chan error, racers)
		for range racers {
			wg.Go(func() {
				_, err := db.Exec(ctx, "INSERT INTO pgtx_guard VALUES (1)")
				errs <- err
			})
		}
		wg.Wait()
		close(errs)

		for err := range errs {
			if !errors.Is(err, tx.ErrTxInUse) {
				return err
			}
		}

		// The refused calls never reached pgx: close the rows and the
		// transaction works on.
		rows.Close()
		if _, err := db.Exec(ctx, "INSERT INTO pgtx_guard VALUES (1)"); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	var n int
	if err := db.QueryRow(t.Context(), "SELECT count(*) FROM pgtx_guard").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("inserted rows = %d, want 1 (only the sequential call committed)", n)
	}
}

// A long-running statement held by one goroutine makes the others refuse
// within their attempt window — never a busy-conn or protocol error that
// does not name the cause. Needs PGTX_TEST_PG.
func TestErrgroupDuringLongStatement(t *testing.T) {
	db, mgr := pgDB(t)

	err := tx.DoReadCommitted(t.Context(), mgr, func(ctx context.Context) error {
		// SELECT pg_sleep(1) for every racer: whichever first takes the
		// transaction holds it for a second, the rest overlap with it.
		const racers = 8
		var wg sync.WaitGroup
		errs := make(chan error, racers)
		for range racers {
			wg.Go(func() {
				_, err := db.Exec(ctx, "SELECT pg_sleep(1)")
				errs <- err
			})
		}
		wg.Wait()
		close(errs)

		var refused int
		for err := range errs {
			switch {
			case err == nil:
			case errors.Is(err, tx.ErrTxInUse):
				refused++
			default:
				// anything else means pgx itself was entered concurrently
				var pgErr *pgconn.PgError
				if errors.As(err, &pgErr) {
					t.Errorf("pgx protocol error: %s %s", pgErr.Code, pgErr.Message)
				}
				return err
			}
		}
		if refused == 0 {
			t.Error("no racer was refused: the concurrency window missed")
		}

		_, err := db.Exec(ctx, "SELECT 1")
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
}

// The guard covers the whole savepoint chain: while the parent holds the
// connection, not even a nested SAVEPOINT is issued — the nested Do is
// refused with ErrTxInUse before touching pgx; inside a live child its own
// busy statement blocks the following calls; sequential savepoint work is
// untouched. Needs PGTX_TEST_PG.
func TestSavepointChainSharesGuard(t *testing.T) {
	db, mgr := pgDB(t)
	nested := tx.NewSettings(pgx.ReadCommitted, settings.WithPropagation(trm.PropagationNested))

	err := tx.DoReadCommitted(t.Context(), mgr, func(ctx context.Context) error {
		rows, err := db.Query(ctx, "SELECT * FROM generate_series(1, 1000)")
		if err != nil {
			return err
		}
		rows.Next()

		// busy parent: the nested transaction cannot even begin
		err = tx.DoSettings(ctx, mgr, nested, func(context.Context) error {
			return errors.New("nested body ran while the parent was busy")
		})
		if !errors.Is(err, tx.ErrTxInUse) {
			t.Fatalf("nested Do while parent busy: err = %v, want ErrTxInUse", err)
		}

		rows.Close()

		// free: the same savepoint transaction now runs
		return tx.DoSettings(ctx, mgr, nested, func(ctx context.Context) error {
			childRows, err := db.Query(ctx, "SELECT * FROM generate_series(1, 1000)")
			if err != nil {
				return err
			}
			childRows.Next()

			// busy child blocks every further call on the chain
			_, err = db.Exec(ctx, "SELECT 3")
			childErr := err
			childRows.Close()

			if _, err := db.Exec(ctx, "SELECT 4"); err != nil {
				return err
			}
			if !errors.Is(childErr, tx.ErrTxInUse) {
				t.Errorf("call while child busy: err = %v, want ErrTxInUse", childErr)
			}
			return nil
		})
	})
	if err != nil {
		t.Fatal(err)
	}
}

// The ordinary sequential patterns keep working untouched, including the
// ones that never call Close explicitly (rows drained to the end) and
// QueryRow followed by more statements. Needs PGTX_TEST_PG.
func TestSequentialTransactionPatterns(t *testing.T) {
	db, mgr := pgDB(t)

	if _, err := db.Exec(t.Context(), "CREATE TABLE IF NOT EXISTS pgtx_guard_seq (a int, b text)"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = db.Exec(context.Background(), "DROP TABLE IF EXISTS pgtx_guard_seq")
	})

	err := tx.DoReadCommitted(t.Context(), mgr, func(ctx context.Context) error {
		if _, err := db.Exec(ctx, "DELETE FROM pgtx_guard_seq"); err != nil {
			return err
		}
		if _, err := db.Exec(ctx, "INSERT INTO pgtx_guard_seq VALUES (1, 'a')"); err != nil {
			return err
		}

		// QueryRow without an explicit Close of anything: the next
		// statement must still work
		var a int
		if err := db.QueryRow(ctx, "SELECT a FROM pgtx_guard_seq").Scan(&a); err != nil {
			return err
		}

		// rows drained to the end, never closed by hand
		rows, err := db.Query(ctx, "SELECT a, b FROM pgtx_guard_seq")
		if err != nil {
			return err
		}
		for rows.Next() {
			if err := rows.Scan(new(int), new(string)); err != nil {
				return err
			}
		}
		if err := rows.Err(); err != nil {
			return err
		}

		// a batch, closed
		batch := &pgx.Batch{}
		batch.Queue("INSERT INTO pgtx_guard_seq VALUES (2, 'b')")
		batch.Queue("INSERT INTO pgtx_guard_seq VALUES (3, 'c')")
		res := db.SendBatch(ctx, batch)
		if err := res.Close(); err != nil {
			return err
		}

		// CollectRows drains internally
		got, err := pgx.CollectRows(
			must(db.Query(ctx, "SELECT a FROM pgtx_guard_seq ORDER BY a")),
			pgx.RowTo[int],
		)
		if err != nil {
			return err
		}
		if len(got) != 3 {
			t.Errorf("rows = %v, want 3", got)
		}

		// CopyFrom
		n, err := db.CopyFrom(ctx,
			pgx.Identifier{"pgtx_guard_seq"},
			[]string{"a", "b"},
			pgx.CopyFromRows([][]any{{4, "d"}, {5, "e"}}),
		)
		if err != nil {
			return err
		}
		if n != 2 {
			t.Errorf("copy rows = %d, want 2", n)
		}

		var count int
		if err := db.QueryRow(ctx, "SELECT count(*) FROM pgtx_guard_seq").Scan(&count); err != nil {
			return err
		}
		if count != 5 {
			t.Errorf("count = %d, want 5", count)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func must(rows pgx.Rows, err error) pgx.Rows {
	if err != nil {
		panic(err)
	}
	return rows
}

// Queries outside any transaction run on the pool: fully concurrent, no
// guard. Needs PGTX_TEST_PG.
func TestPoolPathStaysConcurrent(t *testing.T) {
	db, _ := pgDB(t)

	const racers = 8
	var wg sync.WaitGroup
	errs := make(chan error, racers)
	for range racers {
		wg.Go(func() {
			if _, err := db.Exec(t.Context(), "SELECT pg_sleep(0.05)"); err != nil {
				errs <- err
				return
			}
			errs <- nil
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Errorf("pool-path racer: %v", err)
		}
	}
}
