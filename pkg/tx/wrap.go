package tx

import (
	"context"

	trmpgx "github.com/avito-tech/go-transaction-manager/pgxv5"
	"github.com/avito-tech/go-transaction-manager/trm"
	trmcontext "github.com/avito-tech/go-transaction-manager/trm/context"
	"github.com/avito-tech/go-transaction-manager/trm/settings"
	"github.com/jackc/pgx/v5"
)

func DoAndRet[T any](
	ctx context.Context,
	mgr Trm,
	level pgx.TxIsoLevel,
	f func(ctx context.Context) (T, error),
	opts ...Opt,
) (ret T, err error) {
	err = Do(ctx, mgr, level, func(ctx context.Context) error {
		ret, err = f(ctx)
		return err
	}, opts...)
	return ret, err
}

func DoSerializableRet[T any](
	ctx context.Context,
	manager Trm,
	f func(ctx context.Context) (T, error),
	opts ...Opt,
) (ret T, err error) {
	return DoAndRet(ctx, manager, pgx.Serializable, f, opts...)
}
func DoReadCommittedRet[T any](
	ctx context.Context,
	manager Trm,
	f func(ctx context.Context) (T, error),
	opts ...Opt,
) (ret T, err error) {
	return DoAndRet(ctx, manager, pgx.ReadCommitted, f, opts...)
}
func DoReadUncommittedRet[T any](
	ctx context.Context,
	manager Trm,
	f func(ctx context.Context) (T, error),
	opts ...Opt,
) (ret T, err error) {
	return DoAndRet(ctx, manager, pgx.ReadUncommitted, f, opts...)
}
func DoRepeatableReadRet[T any](
	ctx context.Context,
	manager Trm,
	f func(ctx context.Context) (T, error),
	opts ...Opt,
) (ret T, err error) {
	return DoAndRet(ctx, manager, pgx.RepeatableRead, f, opts...)
}

func Do(
	ctx context.Context,
	trm Trm,
	level pgx.TxIsoLevel,
	fn func(ctx context.Context) error,
	opts ...Opt,
) error {
	cfg := newConfig(opts...)
	setts := NewSettings(level, cfg.trmOpts...)
	run := func(ctx context.Context) error { return trm.DoWithSettings(ctx, setts, fn) }
	// A call inside an open transaction joins it (or opens a savepoint in
	// it): after a failed statement the whole transaction is aborted, so only
	// the outermost call may run it again. Retrying here would re-run fn in
	// the aborted transaction ("current transaction is aborted").
	if inTransaction(ctx, setts) {
		return run(ctx)
	}
	return runWithRetry(ctx, cfg.retry, run)
}

// inTransaction reports whether ctx carries an open transaction under the
// settings' context key (the default key when the call sets none).
func inTransaction(ctx context.Context, s *trmpgx.Settings) bool {
	var key trm.CtxKey = settings.DefaultCtxKey
	if k := s.CtxKeyOrNil(); k != nil {
		key = *k
	}
	return trmcontext.DefaultManager.ByKey(ctx, key) != nil
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
	opts ...Opt,
) error {
	return Do(ctx, manager, pgx.Serializable, fn, opts...)
}
func DoRepeatableRead(
	ctx context.Context,
	manager Trm,
	fn func(ctx context.Context) error,
	opts ...Opt,
) error {
	return Do(ctx, manager, pgx.RepeatableRead, fn, opts...)
}
func DoReadCommitted(
	ctx context.Context,
	manager Trm,
	fn func(ctx context.Context) error,
	opts ...Opt,
) error {
	return Do(ctx, manager, pgx.ReadCommitted, fn, opts...)
}
func DoReadUncommitted(
	ctx context.Context,
	manager Trm,
	fn func(ctx context.Context) error,
	opts ...Opt,
) error {
	return Do(ctx, manager, pgx.ReadUncommitted, fn, opts...)
}
