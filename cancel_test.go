package pgtx_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/gopherex/pgtx"
	"github.com/gopherex/pgtx/pkg/tx"
)

// A transaction whose caller gives up — between statements or in the middle
// of one — is rolled back and its connection goes back to the pool: a pool
// of one connection serves the next transaction. Needs PGTX_TEST_PG.
func TestCancelledTransactionReturnsConnection(t *testing.T) {
	dsn := os.Getenv("PGTX_TEST_PG")
	if dsn == "" {
		t.Skip("PGTX_TEST_PG not set")
	}

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}

	cfg.MaxConns = 1

	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	mgr, err := pgtx.NewTxManager(pool, tx.ReadCommitted())
	if err != nil {
		t.Fatal(err)
	}

	db := pgtx.NewTxDB(pool)

	// A table, not a temporary one: a connection whose statement was
	// cancelled is closed, and the next transaction may run on a new one.
	if _, err := db.Exec(t.Context(), "CREATE TABLE IF NOT EXISTS pgtx_cancel (n int)"); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _, _ = pool.Exec(context.Background(), "DROP TABLE IF EXISTS pgtx_cancel") })

	for name, body := range map[string]func(ctx context.Context) error{
		"between statements": func(ctx context.Context) error {
			if _, err := db.Exec(ctx, "INSERT INTO pgtx_cancel VALUES (1)"); err != nil {
				return err
			}

			<-ctx.Done()

			return ctx.Err()
		},
		"during a statement": func(ctx context.Context) error {
			if _, err := db.Exec(ctx, "INSERT INTO pgtx_cancel VALUES (1)"); err != nil {
				return err
			}

			_, err := db.Exec(ctx, "SELECT pg_sleep(5)")

			return err
		},
	} {
		ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)

		if err := tx.DoReadCommitted(ctx, mgr, body); err == nil {
			t.Fatalf("%s: the transaction succeeded", name)
		}

		cancel()

		next, done := context.WithTimeout(t.Context(), 3*time.Second)

		err := tx.DoReadCommitted(next, mgr, func(ctx context.Context) error {
			var n int
			if err := db.QueryRow(ctx, "SELECT count(*) FROM pgtx_cancel").Scan(&n); err != nil {
				return err
			}

			if n != 0 {
				t.Errorf("%s: the cancelled insert was kept (%d rows)", name, n)
			}

			return nil
		})

		done()

		if err != nil {
			t.Fatalf("%s: the next transaction got no connection: %v (acquired %d)", name, err, pool.Stat().AcquiredConns())
		}
	}
}
