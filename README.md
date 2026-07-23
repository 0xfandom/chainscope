# chainscope

A Uniswap V3 indexer on Ethereum mainnet, written in Rust.

Ingests on-chain events into Postgres and serves a read API. Exactly-once
writes, reorg-safe, resumable.

## Workspace

| Crate | Role |
|-------|------|
| `crates/core` | chain-agnostic domain types, cursor, traits |
| `crates/eth-source` | Ethereum `ChainSource` implementation + event decoders |
| `bins/indexer` | ingestion pipeline |
| `bins/api` | read API |

## Status

Early scaffold. Work in progress.

## Getting started

Requires Docker and a Rust toolchain.

```sh
# 1. configure
cp .env.example .env          # edit if port 5432 is taken

# 2. bring up the database
docker compose up -d
docker compose ps             # postgres should report healthy

# 3. build and run — this applies migrations on startup
cargo build
cargo run --bin chainscope-indexer
```

## Schema

Migrations live in `migrations/` and are embedded into the binary at compile
time, so the indexer carries its own schema and applies whatever is missing on
startup. Running it against an already-migrated database does nothing.

Three groups of tables, split by how long they live:

| Group | Tables | Lifetime |
|-------|--------|----------|
| bookkeeping | `chain_state`, `blocks`, `alerts_sent` | small, pruned past finality |
| raw events | `swaps`, `liq_events` | day-partitioned, rolling window, dropped by partition |
| permanent product | `pools`, `ohlcv_*`, `wallet_positions`, `wallet_stats` | forever, and tiny |

`swaps` and `liq_events` are partitioned by day on `block_time`, which is what
makes retention a `DROP TABLE` instead of a mass `DELETE`. There is no default
partition on purpose — an insert into a day with no partition fails loudly
rather than piling up in a catch-all. `ensure_day_partitions()` creates the days
ahead and runs on every startup.

Data lives in the named volume `chainscope-pgdata`, so `docker compose down`
followed by `docker compose up -d` keeps everything previously written. To wipe
it deliberately:

```sh
docker compose down -v
```

Open a psql shell against the running container:

```sh
docker compose exec postgres psql -U chainscope -d chainscope
```

## Build

```sh
cargo build
```

## License

MIT
