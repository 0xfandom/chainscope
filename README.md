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

## The writer, and exactly-once

The writer drains blocks from the seam, gathers them into batches, and commits
each batch in **one transaction that also advances the cursor**. That single
transaction is where exactly-once is manufactured: a crash can only ever leave
the database in one of two states — the whole batch is present and the cursor
names its last block, or none of it is and the cursor is unchanged. There is no
state where the cursor claims progress the rows do not back up.

Replaying any range is a no-op, from two things working together: block inserts
use `ON CONFLICT (number) DO NOTHING`, and the cursor only ever moves forward
via `GREATEST`. That is what lets crash recovery be "resume from the cursor and
rerun" with no special cases — the re-fetched blocks simply conflict away.

Batching is only for throughput; one transaction per block would be correct but
slow. A batch flushes at `batch_size` or after `flush_interval_ms`, whichever
comes first, so a quiet chain never holds the last few blocks unwritten. The
cursor is advanced *only* inside that transaction, never anywhere else.

When decoding (M2) and PnL (M6) arrive, they extend this same transaction, and
must derive from the rows that actually inserted here (via `RETURNING`), not the
incoming batch — otherwise a replay would double-count.

In M1 the writer consumes `BlockUnit` directly and writes the `blocks` table.
M2 inserts a transformer ahead of it; the writer then consumes `RowBatch` and
the same transaction gains the decoded rows.

## The producer

The first stage. It reads the live cursor, walks forward one block at a time,
and publishes each block with its logs into the seam.

Sequential on purpose. Correctness and resumability first; throughput is M3's
job and reorg detection is M4's. Every block already carries its parent hash
even though nothing reads it yet — so when reorg detection arrives it changes
the consumer, not the producer and not the message.

**Where it starts.** A stored cursor always wins, which is what makes a restart
resume instead of repeat. With no cursor and no configured start block, the live
follower begins at the *current head* rather than block zero — walking all of
history one block at a time would take years, and history belongs to the
backfill.

**Retries.** Only `Transient` errors are retried, with exponential backoff and
full jitter. The other variants are not retryable by definition: `RangeTooLarge`
needs a different request, `BlockNotFound` a different block, `Fatal` a human.
Retrying them would be a busy-wait dressed up as resilience.

**A block the node does not have yet** is not skipped and not hammered. The
producer waits a poll interval and re-asks for the head — providers behind a
load balancer disagree by a block or two, and the tip's own view is what has to
change before the request can succeed.

**Shutdown** races cancellation against both the backoff sleep and the publish,
so stopping does not wait out a 30-second retry or a full channel. On exit the
producer drops its sink, which closes the stream and is how the next stage
learns to finish.

## The chain boundary

Everything downstream of fetching talks to a `ChainSource` — latest height,
finalized height, one block with its logs, logs over a range, the hash at a
height. `crates/eth-source` is the only crate that knows Ethereum exists, and
`cargo tree -p chainscope-core` shows no chain library at all, which is the
boundary being enforced by the compiler rather than by discipline.

Errors are typed by what the caller should *do*: `Transient` (retry),
`RangeTooLarge` (bisect and retry smaller), `BlockNotFound` (stop asking),
`Fatal` (halt). A stringly-typed error would make every call site match on
message text to decide, which is how a provider rewording something becomes an
outage.

Only standard JSON-RPC. Provider "enhanced" endpoints are off limits — they are
another indexer's output, and consuming them would make our input someone
else's opinion of the chain.

### Free RPC endpoints have very different history limits

Measured 2026-07-23, and it matters more than it looks: live sync only ever
reads near the tip, but backfill does not.

| Endpoint | `eth_getLogs` history |
|---|---|
| `rpc.flashbots.net` | deep, no key — best free option |
| `eth.drpc.org` | a few thousand blocks |
| `ethereum-rpc.publicnode.com` | ~128 blocks, then wants a token |
| `1rpc.io/eth` | caps range width |
| `eth.llamarpc.com` | returning 521 |
| `rpc.ankr.com/eth` | now requires a key |

Deep backfill (M3) needs a paid archive endpoint. The network tests avoid fixed
historical block numbers for this reason — they derive a recent settled block
from the tip, so they test this code rather than a billing tier.

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
