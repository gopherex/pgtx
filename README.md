# pgtx

[![CI](https://github.com/gopherex/pgtx/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/gopherex/pgtx/actions/workflows/ci.yml)

Transaction management for Go + PostgreSQL, built on
[`pgx/v5`](https://github.com/jackc/pgx) and
[`avito-tech/go-transaction-manager`](https://github.com/avito-tech/go-transaction-manager).

A `Trm` manager owns transaction boundaries; a context-aware `DB` executor
transparently routes queries to the active transaction (or the pool when there
is none), so repositories stay unaware of whether they run inside a
transaction. Generic `Do*` helpers wrap a unit of work in a chosen isolation
level and return its result.

```text
pgtx/                 NewTxManager / NewTxDB constructors (single pgtx.go)
pkg/tx/               Trm, DB, executor, settings, Do* isolation helpers
contrib/otel/         pgxpool → OpenTelemetry metrics (its own go.mod)
```

## Install

```bash
go get github.com/gopherex/pgtx
```

## Usage

```go
pool, _ := pgxpool.New(ctx, dsn)

mgr, err := pgtx.NewTxManager(pool, tx.Serializable())
if err != nil {
    return err
}

// context-aware executor: uses the in-context transaction when present
db := pgtx.NewTxDB(trmpgx.DefaultCtxGetter)

// run a unit of work in a transaction, get a typed result back
user, err := tx.DoSerializableRet(ctx, mgr, func(ctx context.Context) (User, error) {
    return repo.FindAndUpdate(ctx, db, id)
})
```

Isolation-level helpers: `tx.Serializable`, `tx.RepeatableRead`,
`tx.ReadCommitted`, `tx.ReadUncommitted`, plus the `Do*` / `Do*Ret` runners.

## OpenTelemetry metrics

The `contrib/otel` module exposes `pgxpool` statistics as OpenTelemetry
metrics. It is a separate module so the core stays free of the OTel dependency:

```bash
go get github.com/gopherex/pgtx/contrib/otel
```

```go
import pgtxotel "github.com/gopherex/pgtx/contrib/otel"

if err := pgtxotel.RegisterMetrics(pool); err != nil {
    return err
}
```

## License

MIT — see [LICENSE](LICENSE).
