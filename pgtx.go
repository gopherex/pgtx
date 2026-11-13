package pgtx

import (
	"fmt"

	trmpgx "github.com/avito-tech/go-transaction-manager/drivers/pgxv5/v2"
	"github.com/avito-tech/go-transaction-manager/trm/v2/manager"
	"github.com/gopherex/pgtx/pkg/tx"
)

// NewTxManager creates a TX manager that begins every top-level transaction
// through db: a *pgxpool.Pool, or a wrapper in front of one (for example a
// bounded acquire that turns a pool timeout into a typed error — that error
// reaches the caller of Do unchanged). Nested transactions are savepoints on
// their parent and never call db.
// Put constructor for TX manage here since manager depends on pgx5 driver.
// Transactions it begins are guarded against concurrent use (tx.ErrTxInUse);
// the pool path stays fully concurrent.
func NewTxManager(
	db trmpgx.Transactional,
	settings *trmpgx.Settings,
) (tx.Trm, error) {
	m, err := manager.New(
		tx.GuardedFactory(db),
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
