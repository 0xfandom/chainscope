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

## Configuration

Two layers. `chainscope.toml` is committed and holds everything shareable —
chain id, pool list, tuning knobs. `.env` is not committed and holds the
secrets: the database URL, and RPC endpoints with API keys in them.

Any value in the file can be overridden from the environment as
`CHAINSCOPE_<SECTION>__<KEY>` (double underscore between the two):

```sh
CHAINSCOPE_PIPELINE__BATCH_SIZE=1000
CHAINSCOPE_CHAIN__POOLS=0xaaa...,0xbbb...   # lists are comma-separated
```

The environment always wins. `DATABASE_URL` and `RUST_LOG` are honoured under
their conventional unprefixed names.

Everything is validated before the process opens a socket: addresses must be
20 bytes of hex, URLs must parse and carry a sensible scheme, the pool list must
be non-empty and free of duplicates, numbers must be inside documented bounds,
and an unknown key is an error rather than a setting that quietly does nothing.
A failure names the field and echoes the bad value:

```
Error: chain.pools[0]: an address is 40 hex characters after the 0x prefix (got `0xdeadbeef`)
Error: database.url is not set. Set DATABASE_URL in .env, or database.url in chainscope.toml.
```

Startup logs a summary of the configuration it ended up with, passwords and API
keys redacted.

## The transport seam

Stages never call each other. Each one publishes to an `EventSink` and reads
from an `EventSource`, both defined in `crates/core`, and none of them knows
what is underneath.

```
producer --[BlockUnit]--> transformer --[RowBatch]--> writer
```

Phase 1 is bounded in-memory channels in one process. Phase 2 (M5) is a
Redpanda topic with the stages split into separate processes. That change is a
new implementation of two traits plus one line of config — no stage is touched.

Bounded capacity is the backpressure mechanism, not a tuning detail: when the
writer falls behind, `publish` suspends the fetcher instead of buffering until
the process runs out of memory. There is a test for exactly that.

The seam is only worth anything if it is actually used, and the failure mode is
quiet — someone reaches for an `mpsc::Sender` directly because it is shorter.
`tests/seam_is_not_leaking.rs` fails the build if any file outside
`crates/core/src/transport.rs` names a transport type.

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
