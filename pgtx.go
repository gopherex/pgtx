package pgtx

import (
	"fmt"

	trmpgx "github.com/avito-tech/go-transaction-manager/drivers/pgxv5/v2"
	"github.com/avito-tech/go-transaction-manager/trm/v2/manager"
	"github.com/gopherex/pgtx/pkg/tx"
	"github.com/jackc/pgx/v5/pgxpool"
)

// NewTxManager creates TX manager with pgxpool.Pool.
// Put constructor for TX manage here since manager depends on pgx5 driver.
// Transactions it begins are guarded against concurrent use (tx.ErrTxInUse);
// the pool path stays fully concurrent.
func NewTxManager(
	pool *pgxpool.Pool,
	settings *trmpgx.Settings,
) (tx.Trm, error) {
	m, err := manager.New(
		tx.GuardedFactory(pool),
		manager.WithSettings(settings),
	)
	if err != nil {
		return nil, fmt.Errorf("manager.New: %w", err)
	}

	return m, nil
}

// NewTxDB creates a new TxExecutor with the given defaultTr and options.
//
// Parameters:
// - defaultTr: The default transaction to use.
// - options: Optional configurations for the TxExecutor.
//
// Returns:
// - *TxExecutor: The newly created TxExecutor.
func NewTxDB(defaultTr trmpgx.Tr, options ...tx.DbOption) tx.DB {
	return tx.NewTxDb(defaultTr, options...)
}
