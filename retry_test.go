package pgtx_test

import (
	"context"
	"os"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/gopherex/pgtx"
	"github.com/gopherex/pgtx/pkg/tx"
)

// Concurrent serializable transactions bumping one counter conflict
// (40001); with retry every one of them commits. Needs PGTX_TEST_PG.
func TestSerializableConflictsRetry(t *testing.T) {
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

	db := pgtx.NewTxDB(pool)

	if _, err := db.Exec(t.Context(), "CREATE TABLE IF NOT EXISTS pgtx_retry (singleton bool PRIMARY KEY, n bigint NOT NULL)"); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _, _ = pool.Exec(context.Background(), "DROP TABLE IF EXISTS pgtx_retry") })

	bump := func(ctx context.Context) error {
		var n int64
		return db.QueryRow(ctx, "UPDATE pgtx_retry SET n = n + 1 WHERE singleton RETURNING n").Scan(&n)
	}
	policy := tx.RetryPolicy{MaxAttempts: 50, Backoff: tx.DefaultBackoff}

	for name, run := range map[string]func(ctx context.Context) error{
		"plain": func(ctx context.Context) error {
			return tx.DoSerializable(ctx, mgr, bump, tx.WithRetry(policy))
		},
		"nested": func(ctx context.Context) error {
			return tx.DoSerializable(ctx, mgr, func(ctx context.Context) error {
				return tx.DoSerializable(ctx, mgr, bump, tx.WithRetry(policy))
			}, tx.WithRetry(policy))
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := db.Exec(t.Context(), "INSERT INTO pgtx_retry VALUES (true, 0) ON CONFLICT (singleton) DO UPDATE SET n = 0"); err != nil {
				t.Fatal(err)
			}

			const workers = 8

			var wg sync.WaitGroup
			errs := make(chan error, workers)

			for range workers {
				wg.Go(func() { errs <- run(t.Context()) })
			}

			wg.Wait()
			close(errs)

			for err := range errs {
				if err != nil {
					t.Errorf("transaction: %v", err)
				}
			}

			var n int64
			if err := db.QueryRow(t.Context(), "SELECT n FROM pgtx_retry").Scan(&n); err != nil {
				t.Fatal(err)
			}

			if n != workers {
				t.Errorf("counter = %d, want %d", n, workers)
			}
		})
	}
}
