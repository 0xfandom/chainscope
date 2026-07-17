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

## Build

```sh
cargo build
```

## License

MIT
