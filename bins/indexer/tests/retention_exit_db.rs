//! M9 exit criterion (#116): a flat footprint, against a real Postgres.
//!
//! Index swaps across several days, run retention (roll up then prune), and
//! assert the raw window stays bounded while the candles cover the full history
//! — the raw footprint shrinks, the aggregates persist.
//!
//!   DATABASE_URL=postgres://chainscope:chainscope@localhost:5432/chainscope \
//!     cargo test -p chainscope-indexer --test retention_exit_db -- --ignored --nocapture

use std::str::FromStr;

use bigdecimal::BigDecimal;
use chainscope_core::{
    types::{Address20, Hash32, SwapRow},
    RowBatch,
};
use chainscope_indexer::db;
use sqlx::{postgres::PgPoolOptions, PgPool};

const POOL: Address20 = [0x9a; 20];
const SQRT_1: &str = "79228162514264337593543950336"; // 2^96 -> price 1

async fn admin() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL").ok()?;
    PgPoolOptions::new().max_connections(2).connect(&url).await.ok()
}

async fn fresh_db(admin: &PgPool) -> (PgPool, String) {
    let name = format!("chainscope_retexit_{}", std::process::id());
    sqlx::query(&format!(r#"DROP DATABASE IF EXISTS "{name}" WITH (FORCE)"#)).execute(admin).await.ok();
    sqlx::query(&format!(r#"CREATE DATABASE "{name}""#)).execute(admin).await.unwrap();
    let base = std::env::var("DATABASE_URL").unwrap();
    let mut url = url::Url::parse(&base).unwrap();
    url.set_path(&format!("/{name}"));
    let pool = PgPoolOptions::new().max_connections(4).connect(url.as_str()).await.unwrap();
    db::migrate(&pool).await.unwrap();
    sqlx::query("UPDATE chain_state SET finalized_height = 1000000 WHERE id = 1")
        .execute(&pool)
        .await
        .unwrap();
    (pool, name)
}

async fn drop_db(admin: &PgPool, pool: PgPool, name: &str) {
    pool.close().await;
    sqlx::query(&format!(r#"DROP DATABASE IF EXISTS "{name}" WITH (FORCE)"#)).execute(admin).await.ok();
}

/// A day string `current_date - offset` and its noon epoch.
async fn day_and_epoch(pool: &PgPool, offset: i64) -> (String, i64) {
    let day: String = sqlx::query_scalar("SELECT to_char(current_date - $1::int, 'YYYYMMDD')")
        .bind(offset as i32)
        .fetch_one(pool)
        .await
        .unwrap();
    let epoch: i64 = sqlx::query_scalar(
        "SELECT extract(epoch FROM (current_date - $1::int) + interval '12 hours')::bigint",
    )
    .bind(offset as i32)
    .fetch_one(pool)
    .await
    .unwrap();
    (day, epoch)
}

async fn make_partition(pool: &PgPool, day: &str) {
    sqlx::query(&format!(
        "CREATE TABLE IF NOT EXISTS swaps_{day} PARTITION OF swaps \
         FOR VALUES FROM (to_date('{day}','YYYYMMDD')) TO (to_date('{day}','YYYYMMDD') + 1)"
    ))
    .execute(pool)
    .await
    .unwrap();
}

fn block(block_number: u64, block_time: i64, tx: u8) -> RowBatch {
    let mut h: Hash32 = [0; 32];
    h[..8].copy_from_slice(&block_number.to_be_bytes());
    let mut txh: Hash32 = [0; 32];
    txh[0] = tx;
    txh[24..].copy_from_slice(&block_number.to_be_bytes());
    RowBatch {
        block_number,
        block_hash: h,
        parent_hash: [0; 32],
        block_time,
        swaps: vec![SwapRow {
            tx_hash: txh,
            log_index: 0,
            pool: POOL,
            sender: [0xff; 20],
            recipient: [0x11; 20],
            amount0: BigDecimal::from(10),
            amount1: BigDecimal::from(-20),
            sqrt_price_x96: BigDecimal::from_str(SQRT_1).unwrap(),
            liquidity: BigDecimal::from(1),
            tick: 0,
        }],
        liq_events: vec![],
    }
}

async fn table_exists(pool: &PgPool, t: &str) -> bool {
    let r: Option<String> = sqlx::query_scalar("SELECT to_regclass($1)::text").bind(t).fetch_one(pool).await.unwrap();
    r.is_some()
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn a_week_of_history_leaves_a_flat_footprint() {
    let Some(admin) = admin().await else { return };
    let (pool, name) = fresh_db(&admin).await;

    let (old_day, old_ts) = day_and_epoch(&pool, 100).await; // out of window, finalized
    let (recent_day, recent_ts) = day_and_epoch(&pool, 1).await; // inside window
    make_partition(&pool, &old_day).await;
    make_partition(&pool, &recent_day).await;

    // Live-index one block on each day — swaps land in their day partition, and
    // the live path folds 1m candles for both.
    db::write_row_batches(&pool, &[block(500, old_ts, 0xA1)], false).await.unwrap();
    db::write_row_batches(&pool, &[block(600, recent_ts, 0xA2)], false).await.unwrap();

    let before = db::footprint(&pool).await.unwrap();

    // Retention: roll the candles up, then prune out-of-window raw partitions.
    db::downsample(&pool).await.unwrap();
    let dropped = db::prune_raw_partitions(&pool, 30, None).await.unwrap();

    // Raw window bounded: the old day partition is gone, the recent one stays.
    assert_eq!(dropped, vec![format!("swaps_{old_day}")]);
    assert!(!table_exists(&pool, &format!("swaps_{old_day}")).await);
    assert!(table_exists(&pool, &format!("swaps_{recent_day}")).await);

    // History preserved: a 1d candle still covers the pruned old day.
    let old_day_candles: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM ohlcv_1d WHERE bucket = to_date($1,'YYYYMMDD')",
    )
    .bind(&old_day)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(old_day_candles, 1, "the dropped day still has a daily candle");

    // Footprint flat: raw shrank, aggregates held.
    let after = db::footprint(&pool).await.unwrap();
    assert!(after.raw_bytes < before.raw_bytes, "raw footprint shrank on prune");
    assert!(after.aggregate_bytes >= before.aggregate_bytes, "aggregates persisted");

    drop_db(&admin, pool, &name).await;
}
