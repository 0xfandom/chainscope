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

# 3. build and run
cargo build
cargo run --bin indexer
```

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
