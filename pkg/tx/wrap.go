package tx

import (
	"context"

	trmpgx "github.com/avito-tech/go-transaction-manager/pgxv5"
	"github.com/avito-tech/go-transaction-manager/trm/settings"
	"github.com/jackc/pgx/v5"
)

func DoAndRet[T any](
	ctx context.Context,
	mgr Trm,
	level pgx.TxIsoLevel,
	f func(ctx context.Context) (T, error),
	opts ...settings.Opt,
) (ret T, err error) {
	err = mgr.DoWithSettings(ctx,
		NewSettings(level, opts...),
		func(ctx context.Context) error {
			ret, err = f(ctx)
			return err
		},
	)
	return ret, err
}

func DoSerializableRet[T any](
	ctx context.Context,
	manager Trm,
	f func(ctx context.Context) (T, error),
	opts ...settings.Opt,
) (ret T, err error) {
	return DoAndRet(ctx, manager, pgx.Serializable, f, opts...)
}
func DoReadCommittedRet[T any](
	ctx context.Context,
	manager Trm,
	f func(ctx context.Context) (T, error),
	opts ...settings.Opt,
) (ret T, err error) {
	return DoAndRet(ctx, manager, pgx.ReadCommitted, f, opts...)
}
func DoReadUncommittedRet[T any](
	ctx context.Context,
	manager Trm,
	f func(ctx context.Context) (T, error),
	opts ...settings.Opt,
) (ret T, err error) {
	return DoAndRet(ctx, manager, pgx.ReadUncommitted, f, opts...)
}
func DoRepeatableReadRet[T any](
	ctx context.Context,
	manager Trm,
	f func(ctx context.Context) (T, error),
	opts ...settings.Opt,
) (ret T, err error) {
	return DoAndRet(ctx, manager, pgx.RepeatableRead, f, opts...)
}

func Do(
	ctx context.Context,
	trm Trm,
	level pgx.TxIsoLevel,
	fn func(ctx context.Context) error,
	opts ...settings.Opt,
) error {
	return trm.DoWithSettings(ctx, NewSettings(level, opts...), fn)
}

func DoSettings(
	ctx context.Context,
	trm Trm,
	settings *trmpgx.Settings,
	fn func(ctx context.Context) error,
) error {
	return trm.DoWithSettings(ctx, settings, fn)
}

func DoSerializable(
	ctx context.Context,
	manager Trm,
	fn func(ctx context.Context) error,
	opts ...settings.Opt,
) error {
	return Do(ctx, manager, pgx.Serializable, fn, opts...)
}
func DoRepeatableRead(
	ctx context.Context,
	manager Trm,
	fn func(ctx context.Context) error,
	opts ...settings.Opt,
) error {
	return Do(ctx, manager, pgx.RepeatableRead, fn, opts...)
}
func DoReadCommitted(
	ctx context.Context,
	manager Trm,
	fn func(ctx context.Context) error,
	opts ...settings.Opt,
) error {
	return Do(ctx, manager, pgx.ReadCommitted, fn, opts...)
}
func DoReadUncommitted(
	ctx context.Context,
	manager Trm,
	fn func(ctx context.Context) error,
	opts ...settings.Opt,
) error {
	return Do(ctx, manager, pgx.ReadUncommitted, fn, opts...)
}
