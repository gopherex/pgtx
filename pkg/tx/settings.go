package tx

import (
	trmpgx "github.com/avito-tech/go-transaction-manager/pgxv5"
	"github.com/avito-tech/go-transaction-manager/trm/settings"
	"github.com/jackc/pgx/v5"
)

func NewSettings(level pgx.TxIsoLevel, opts ...settings.Opt) *trmpgx.Settings {
	setts := trmpgx.MustSettings(settings.Must(opts...),
		trmpgx.WithTxOptions(pgx.TxOptions{
			IsoLevel: level,
		}),
	)
	return &setts
}

func Serializable(opts ...settings.Opt) *trmpgx.Settings {
	return NewSettings(pgx.Serializable, opts...)
}

func RepeatableRead(opts ...settings.Opt) *trmpgx.Settings {
	return NewSettings(pgx.RepeatableRead, opts...)
}

func ReadCommitted(opts ...settings.Opt) *trmpgx.Settings {
	return NewSettings(pgx.ReadCommitted, opts...)
}

func ReadUncommitted(opts ...settings.Opt) *trmpgx.Settings {
	return NewSettings(pgx.ReadUncommitted, opts...)
}
