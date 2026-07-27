//! Decoded rows match Etherscan, and survive the database round-trip unchanged.
//!
//! The M2 exit criterion is "sample rows match Etherscan exactly". This pins a
//! real mainnet Swap — visible on Etherscan — decodes it through the real
//! `map_log`, stores it through the real `write_row_batches`, reads it back, and
//! asserts every field is byte-for-byte what the transaction actually emitted.
//! It catches the two silent-corruption risks decoding has: a wrong sign or a
//! truncated amount at decode time, and any loss crossing `BigDecimal` ->
//! `NUMERIC` -> `BigDecimal` through Postgres.
//!
//! Deterministic and offline (no RPC): the log bytes are frozen from the chain.
//! Ignored by default because it needs a Postgres it can create databases on.
//! Run with:
//!
//!   docker compose up -d
//!   DATABASE_URL=postgres://chainscope:chainscope@localhost:5432/chainscope \
//!     cargo test -p chainscope-indexer --test decode_parity -- --ignored --nocapture

use bigdecimal::BigDecimal;
use std::str::FromStr;

use chainscope_core::{
    types::{Address20, Hash32, RawLog},
    RowBatch,
};
use chainscope_eth_source::{map_log, Mapped};
use chainscope_indexer::db;
use sqlx::{postgres::PgPoolOptions, PgPool, Row};

fn h(hex: &str) -> Hash32 {
    let mut out = [0u8; 32];
    hex::decode_to_slice(hex, &mut out).unwrap();
    out
}
fn addr(hex: &str) -> Address20 {
    let mut out = [0u8; 20];
    hex::decode_to_slice(hex, &mut out).unwrap();
    out
}
fn bytes(hex: &str) -> Vec<u8> {
    hex::decode(hex).unwrap()
}
fn bd(s: &str) -> BigDecimal {
    BigDecimal::from_str(s).unwrap()
}

// ---------------------------------------------------------------------------
// The canonical fixture: a real Swap, cross-checkable on Etherscan.
//
//   block   25601357
//   tx      0xe18a03325588278d1d9605c762339598b31f34a5f8b2fd62a7ff0bfed60eb5dc
//   log     index 39, USDC/WETH 0.05% pool 0x8ad5…6d8
//
// The five values below are what Etherscan shows for that log (and what a
// hand-decode of the raw data words gives):
//   amount0        140586
//   amount1        -74025266944810
//   sqrtPriceX96   1820754252512732283398282170500178
//   liquidity      1620197336976127727
//   tick           200858
// ---------------------------------------------------------------------------

const POOL: &str = "8ad599c3a0ff1de082011efddc58f1908eb6e6d8";
const TX: &str = "e18a03325588278d1d9605c762339598b31f34a5f8b2fd62a7ff0bfed60eb5dc";
const TRADER: &str = "06cff7088619c7178f5e14f0b119458d08d2f5ef";

fn swap_log() -> RawLog {
    RawLog {
        address: addr(POOL),
        topics: vec![
            h("c42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"),
            h("00000000000000000000000006cff7088619c7178f5e14f0b119458d08d2f5ef"),
            h("00000000000000000000000006cff7088619c7178f5e14f0b119458d08d2f5ef"),
        ],
        data: bytes(
            "000000000000000000000000000000000000000000000000000000000002252a\
             ffffffffffffffffffffffffffffffffffffffffffffffffffffbcaca64264d6\
             00000000000000000000000000000000000059c52649e6ea40cba55920aa8452\
             000000000000000000000000000000000000000000000000167c18e4d07ef6ef\
             000000000000000000000000000000000000000000000000000000000003109a",
        ),
        block_number: 25_601_357,
        tx_hash: h(TX),
        log_index: 39,
    }
}

async fn admin() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL").ok()?;
    PgPoolOptions::new().max_connections(2).connect(&url).await.ok()
}

async fn fresh_db(admin: &PgPool) -> (PgPool, String) {
    let name = format!("chainscope_parity_{}", std::process::id());
    sqlx::query(&format!(r#"DROP DATABASE IF EXISTS "{name}" WITH (FORCE)"#))
        .execute(admin)
        .await
        .ok();
    sqlx::query(&format!(r#"CREATE DATABASE "{name}""#))
        .execute(admin)
        .await
        .unwrap();
    let base = std::env::var("DATABASE_URL").unwrap();
    let mut url = url::Url::parse(&base).unwrap();
    url.set_path(&format!("/{name}"));
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(url.as_str())
        .await
        .unwrap();
    db::migrate(&pool).await.unwrap();
    db::ensure_partitions(&pool).await.unwrap();
    (pool, name)
}

/// The whole path: decode -> store -> read back -> compare to Etherscan.
#[tokio::test]
#[ignore = "requires a running Postgres"]
async fn a_real_swap_matches_etherscan_through_decode_and_storage() {
    let Some(admin) = admin().await else {
        eprintln!("skipped: set DATABASE_URL to a running Postgres");
        return;
    };
    let (pool, name) = fresh_db(&admin).await;

    // --- decode ---
    let Mapped::Swap(row) = map_log(&swap_log()) else {
        panic!("the fixture must decode to a swap");
    };
    assert_eq!(row.amount0, bd("140586"), "decode: amount0");
    assert_eq!(row.amount1, bd("-74025266944810"), "decode: amount1 (sign preserved)");
    assert_eq!(row.sqrt_price_x96, bd("1820754252512732283398282170500178"), "decode: sqrtPriceX96");
    assert_eq!(row.liquidity, bd("1620197336976127727"), "decode: liquidity");
    assert_eq!(row.tick, 200858, "decode: tick");

    // --- store, in the same transaction shape the writer uses ---
    // block_time uses "now" so the day partition (migrated for the current date)
    // always exists; it is block metadata, not a decoded swap field, so it does
    // not affect the value parity being checked.
    let now_epoch: i64 = sqlx::query("SELECT extract(epoch FROM now())::bigint")
        .fetch_one(&pool)
        .await
        .unwrap()
        .get(0);
    let batch = RowBatch {
        block_number: 25_601_357,
        block_hash: h("00000000000000000000000000000000000000000000000000000000deadbeef"),
        parent_hash: h("00000000000000000000000000000000000000000000000000000000deadbeee"),
        block_time: now_epoch,
        swaps: vec![row],
        liq_events: vec![],
    };
    db::write_row_batches(&pool, std::slice::from_ref(&batch), false).await.unwrap();

    // --- read back and compare to Etherscan, field by field ---
    let r = sqlx::query(
        "SELECT block_number, encode(pool,'hex') AS pool, encode(sender,'hex') AS sender,
                encode(recipient,'hex') AS recipient, encode(tx_hash,'hex') AS tx,
                log_index, amount0, amount1, sqrt_price_x96, liquidity, tick
           FROM swaps WHERE tx_hash = $1 AND log_index = 39",
    )
    .bind(bytes(TX))
    .fetch_one(&pool)
    .await
    .unwrap();

    assert_eq!(r.get::<i64, _>("block_number"), 25_601_357);
    assert_eq!(r.get::<String, _>("pool"), POOL);
    assert_eq!(r.get::<String, _>("sender"), TRADER);
    assert_eq!(r.get::<String, _>("recipient"), TRADER);
    assert_eq!(r.get::<String, _>("tx"), TX);
    assert_eq!(r.get::<i32, _>("log_index"), 39);
    // The NUMERIC columns come back as BigDecimal — the round-trip must be exact.
    assert_eq!(r.get::<BigDecimal, _>("amount0"), bd("140586"), "stored amount0");
    assert_eq!(r.get::<BigDecimal, _>("amount1"), bd("-74025266944810"), "stored amount1");
    assert_eq!(
        r.get::<BigDecimal, _>("sqrt_price_x96"),
        bd("1820754252512732283398282170500178"),
        "stored sqrtPriceX96"
    );
    assert_eq!(r.get::<BigDecimal, _>("liquidity"), bd("1620197336976127727"), "stored liquidity");
    assert_eq!(r.get::<i32, _>("tick"), 200858, "stored tick");

    pool.close().await;
    sqlx::query(&format!(r#"DROP DATABASE IF EXISTS "{name}" WITH (FORCE)"#))
        .execute(&admin)
        .await
        .ok();
    eprintln!("parity OK: block 25601357 swap decodes and stores exactly as Etherscan shows");
}
