<h1 align="center">chainscope</h1>

<p align="center">
  <b>A smart-money indexer for Uniswap V3 on Ethereum.</b><br>
  Ingest → decode → per-wallet PnL → a read API and Telegram alerts — reorg-safe,
  exactly-once, and flat on disk. Built two ways behind one seam: an in-process
  channel and a distributed Redpanda log.
</p>

<p align="center">
  <img alt="status" src="https://img.shields.io/badge/status-M1–M10%20complete-2dd4c4?labelColor=0c2630">
  <img alt="language" src="https://img.shields.io/badge/Rust-stable-ffc56d?labelColor=0c2630">
  <img alt="store" src="https://img.shields.io/badge/store-Postgres%2016-14f195?labelColor=0c2630">
  <img alt="chain" src="https://img.shields.io/badge/chain-Ethereum%20mainnet-3178c6?labelColor=0c2630">
  <img alt="license" src="https://img.shields.io/badge/license-MIT-6b7280?labelColor=0c2630">
</p>

<p align="center">
  <code>docker compose up -d</code> &nbsp;·&nbsp; API on <code>:8080</code> &nbsp;·&nbsp; Grafana on <code>:3000</code>
</p>

---

## What is chainscope

chainscope follows Uniswap V3 on Ethereum mainnet and answers one question in near
real time: **which wallets are winning.** It ingests swaps and liquidity events,
computes each wallet's realised PnL on a FIFO cost basis, serves it over a read
API, and pushes Telegram alerts when smart money moves.

The point is not another price feed. It is being **correct under the two failure
modes that usually get hand-waved** — chain reorganisations and process crashes —
so every derived number can be proven right after a block is orphaned or the
process is killed mid-write.

- **Exactly-once** — a write and the cursor that names it commit in one transaction.
- **Reorg-safe** — orphaned blocks and everything derived from them (PnL, candles,
  the leaderboard) are walked backwards, not left stale.
- **Flat on disk** — raw events roll off past finality; candles and wallet
  aggregates are the permanent record they burn into.

## Who it's for

| Audience | What they get |
| --- | --- |
| **Analysts / traders** | A live smart-money leaderboard and per-wallet PnL scorecards, wash-trade filtered. |
| **Alert consumers** | Telegram pings on watchlist moves, coordinated cluster buys, and fresh pools clearing a scorecard. |
| **Integrators** | A read API — pools, swaps, OHLCV candles, wallet trades, `/metrics` — with keyset pagination. |
| **Operators** | One-command Docker stack, Prometheus metrics, and a provisioned Grafana dashboard. |
| **Developers** | A full Rust workspace — chain source, pipeline, API, alerter — with exactly-once and reorg recovery implemented two ways to fork and study. |

## The pipeline

Every stage publishes to a transport seam and reads from it; none calls the next
directly. One block of work flows through, in order:

```
producer ──[BlockUnit]──▶ transformer ──[RowBatch]──▶ writer ──▶ PnL · candles · retention
  fetch                     decode                    exactly-once fold
```

- **producer** — walks the chain one block at a time from the stored cursor;
  resumes rather than repeats after a restart. Typed RPC errors (`Transient`,
  `RangeTooLarge`, `BlockNotFound`, `Fatal`) decide retry vs. stop.
- **transformer** — decodes swaps and liquidity events into partitioned raw tables.
- **writer** — commits each batch **and the cursor** in one transaction: exactly-once
  by construction. PnL, candle folds and pool discovery extend that same transaction.
- **retention** — folds candles, rolls them up (1m→1h→1d), and drops raw partitions
  past a finality floor, keeping the footprint flat.

## Two architectures, one seam

chainscope's thesis is that it implements exactly-once and reorg recovery **two
ways**, behind the same seam, switchable with one line of config.

- **Phase 1 — in-process channel (M1–M4).** All stages run in one process over a
  bounded in-memory channel. Exactly-once is a single Postgres transaction; a crash
  leaves "all rows + cursor" or "neither", never a half-state.
- **Phase 2 — Redpanda log (M5).** The same stages split into separate processes
  reading a Kafka-compatible topic. Exactly-once becomes idempotent consumers keyed
  on offset, and a reorg is a compensating **revert event** on the log rather than a
  rollback inside one transaction.

Both satisfy the same behavioural tests. Switching is one setting in
`chainscope.toml` — no stage is touched:

```toml
[pipeline]
transport = "channel"    # phase 1, one process
# transport = "redpanda"   # phase 2, distributed; brokers under docker compose
```

The seam (`crates/core/src/transport.rs`) is the **only** place either transport is
named — enforced by `tests/seam_is_not_leaking.rs`, which fails the build if any
other file reaches for a channel or a Kafka client directly.

## How it works

Three surfaces, all from one indexed store:

- **Read** — the API answers pools, swaps, OHLCV candles, wallet scorecards and the
  leaderboard, keyset-paginated with an opaque cursor and a hot-stats cache.
- **Alert** — the alerter polls the same database and pushes to Telegram on a
  watchlist move, a cluster of watched wallets buying the same pool, or a new pool
  clearing the scorecard threshold. It is a separate process so a hung outbound call
  can never backpressure ingestion.
- **Observe** — Prometheus scrapes the API's `/metrics`; Grafana renders ingest lag,
  the block heads, and the raw-vs-aggregate disk footprint M9 keeps flat.

## Run it

Everything runs under Docker. `docker compose up` builds the three binaries and
brings up the full stack — Postgres, indexer, API, alerter, Prometheus, Grafana.

```bash
# 1. configure — set RPC endpoint(s) and, for alerts, a Telegram bot token
cp .env.example .env

# 2. one command up
docker compose up -d --build
docker compose ps                 # postgres healthy, then indexer/api/alerter up

# 3. prove it end to end
./ops/smoke.sh                    # brings up, gates every health check, asserts the surface
```

`ops/smoke.sh` is the exit criterion: it waits for each service's health check, then
asserts `/status` and `/metrics` answer, Prometheus is actually scraping the API
(`up == 1`), and Grafana is serving — one green line or a named failure. To drive a
binary from the host instead, run just the database (migrations apply on startup):

```bash
docker compose up -d postgres
cargo run --bin chainscope-indexer
```

### Endpoints

| Surface | Where |
| --- | --- |
| Ingest status + lag | `GET /status` |
| Indexed / new pools | `GET /pools`, `GET /pools/:address`, `GET /pools/new` |
| Pool swaps / candles | `GET /pools/:address/swaps`, `.../candles?resolution=1m\|1h\|1d` |
| Wallet PnL scorecard | `GET /wallets/:address` |
| Wallet realised trades | `GET /wallets/:address/trades` |
| Smart-money leaderboard | `GET /leaderboard` |
| Prometheus metrics / health | `GET /metrics`, `GET /healthz` |
| Grafana dashboard | `http://localhost:3000` (admin/admin) — lag, heads, footprint |
| Prometheus | `http://localhost:9090` |

## Architecture

The read API and alerter are read-only consumers of the store the indexer owns:

```
   ChainSource (JSON-RPC)          ← the only thing that knows Ethereum exists
         │  blocks + logs
         ▼
   indexer pipeline               ← fetch → decode → PnL → retention, exactly-once
         │  writes
         ▼
   Postgres (partitioned)          ← raw events roll off; candles + PnL are permanent
         │  read-only
         ├────────────▶ api        ← pools, swaps, candles, scorecards, /metrics
         └────────────▶ alerter    ← Telegram: moves, cluster buys, new pools
```

Only `crates/eth-source` knows Ethereum exists — `cargo tree -p chainscope-core`
shows no chain library at all, the boundary enforced by the compiler rather than by
discipline.

```
chainscope/
├─ crates/
│  ├─ core/          # chain-agnostic domain types, cursor, the transport seam
│  └─ eth-source/    # the only crate that knows Ethereum: ChainSource + decoders
├─ bins/
│  ├─ indexer/       # ingestion pipeline: fetch → decode → PnL → retention
│  ├─ api/           # read API (axum): keyset pagination, hot-stats cache, /metrics
│  └─ alerter/       # Telegram alerts: watchlist moves, cluster buys, new pools
├─ migrations/       # embedded, applied on startup
├─ ops/              # Dockerfile helpers, prometheus.yml, grafana provisioning, smoke.sh
└─ scripts/          # redpanda topic setup (phase 2)
```

## Testing

chainscope is verified at levels that each catch what the one below can't — many
are **behavioural**, asserting an invariant rather than a fixed output:

| Level | Where | What it proves |
| --- | --- | --- |
| Unit | `cargo test` (per crate) | math, decoding, cursor and config logic |
| Crash resumability | `tests/crash_resumability.rs` | kill at any point, resume gap-free — 50 randomised trials |
| Reorg recovery | `tests/reorg_*` | orphaned blocks and their derived rows walk backwards |
| Seam isolation | `tests/seam_is_not_leaking.rs` | no stage names a transport directly |
| Store integration | `tests/*_db.rs` | writer, PnL, candles, retention, API against real Postgres |
| Log transport | `tests/kafka_*` | idempotent consumers, offsets, broadcast under a storm |
| End-to-end | `ops/smoke.sh` | one command up, stack healthy, dashboard live |

The load-bearing test is `crash_resumability`: it runs the real producer and writer
against a synthetic chain, aborts at a randomised point, restarts from the cursor,
and asserts blocks form a gap-free run with no duplicates — and a companion test
deliberately breaks atomicity to prove the invariant can actually fail.

## Project status

Every milestone is complete — **the stack comes up with one command, reaches the
mainnet tip, and renders live on Grafana.**

| Milestone | What it added |
| --- | --- |
| ✅ **M1** | Crash-safe ingestion: single-transaction exactly-once, cursor resume, supervised shutdown |
| ✅ **M2** | Event decoding: swaps + liquidity events into partitioned raw tables |
| ✅ **M3** | Throughput: concurrent backfill, batching, range bisection under RPC limits |
| ✅ **M4** | Reorg recovery: fork detection, rollback of orphaned blocks and everything derived |
| ✅ **M5** | Phase-2 transport: Redpanda log, separate processes, idempotent consumers + revert events |
| ✅ **M6** | Per-wallet PnL: FIFO cost basis, lot-consumption ledger for exact reorg reversal, wash flagging |
| ✅ **M7** | Read API: keyset pagination, hot-stats cache, leaderboard materialised view |
| ✅ **M8** | Alerts + sniffer: Telegram watchlist moves, cluster buys, new-pool scorecards |
| ✅ **M9** | Retention: live candle fold, 1m→1h→1d downsampling, partition pruning past finality |
| ✅ **M10** | Ops: Prometheus `/metrics`, provisioned Grafana, Dockerfiles, one-command full-stack compose |

## Configuration

`chainscope.toml` is committed and holds everything shareable — chain id, pool list,
tuning knobs. `.env` is not committed and holds the secrets: the database URL and RPC
endpoints. Any value is overridable from the environment as
`CHAINSCOPE_<SECTION>__<KEY>` (the environment always wins), and `DATABASE_URL` /
`RUST_LOG` keep their conventional names.

Everything is validated before a socket opens — addresses must be 20 bytes of hex,
the pool list non-empty and duplicate-free, unknown keys are errors — and a failure
names the field and echoes the bad value rather than starting mis-configured.

> **RPC note (measured 2026-07-23):** free endpoints differ wildly in history depth.
> `rpc.flashbots.net` is the best keyless option; most others cap `eth_getLogs`
> range or want a token past ~128 blocks. Deep backfill (M3) needs a paid archive
> endpoint; the network tests derive a recent settled block from the tip rather than
> pinning historical block numbers, so they test the code, not a billing tier.

## Toolchain

- **Rust (stable)** for the whole workspace — indexer, API, alerter, core, eth-source.
- **Postgres 16** as the store; migrations are embedded and applied on startup.
- **Docker + Docker Compose** for the full stack.
- **Redpanda** (Kafka-compatible) for the phase-2 log transport.
- **Prometheus + Grafana** for metrics and the dashboard.

## License

MIT
