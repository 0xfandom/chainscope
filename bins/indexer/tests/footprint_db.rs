//! Disk-footprint metric (#115), against a real Postgres.
//!
//!   DATABASE_URL=postgres://chainscope:chainscope@localhost:5432/chainscope \
//!     cargo test -p chainscope-indexer --test footprint_db -- --ignored --nocapture

use chainscope_core::types::Address20;
use chainscope_indexer::db;
use sqlx::{postgres::PgPoolOptions, PgPool};

const POOL: Address20 = [0x9a; 20];

async fn admin() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL").ok()?;
    PgPoolOptions::new().max_connections(2).connect(&url).await.ok()
}

async fn fresh_db(admin: &PgPool) -> (PgPool, String) {
    let name = format!("chainscope_fp_{}", std::process::id());
    sqlx::query(&format!(r#"DROP DATABASE IF EXISTS "{name}" WITH (FORCE)"#)).execute(admin).await.ok();
    sqlx::query(&format!(r#"CREATE DATABASE "{name}""#)).execute(admin).await.unwrap();
    let base = std::env::var("DATABASE_URL").unwrap();
    let mut url = url::Url::parse(&base).unwrap();
    url.set_path(&format!("/{name}"));
    let pool = PgPoolOptions::new().max_connections(4).connect(url.as_str()).await.unwrap();
    db::migrate(&pool).await.unwrap();
    sqlx::query("UPDATE chain_state SET finalized_height = 100000 WHERE id = 1")
        .execute(&pool)
        .await
        .unwrap();
    (pool, name)
}

async fn drop_db(admin: &PgPool, pool: PgPool, name: &str) {
    pool.close().await;
    sqlx::query(&format!(r#"DROP DATABASE IF EXISTS "{name}" WITH (FORCE)"#)).execute(admin).await.ok();
}

async fn seed_day(pool: &PgPool, day: &str, rows: i64, block: i64) {
    let name = format!("swaps_{day}");
    sqlx::query(&format!(
        "CREATE TABLE IF NOT EXISTS {name} PARTITION OF swaps \
         FOR VALUES FROM (to_date('{day}','YYYYMMDD')) TO (to_date('{day}','YYYYMMDD') + 1)"
    ))
    .execute(pool)
    .await
    .unwrap();
    for i in 0..rows {
        let mut tx = [0u8; 32];
        tx[..8].copy_from_slice(&(i as u64).to_be_bytes());
        tx[8] = day.as_bytes()[6];
        sqlx::query(
            "INSERT INTO swaps (block_time, tx_hash, log_index, block_number, pool, sender, \
                 recipient, amount0, amount1, sqrt_price_x96, liquidity, tick) \
             VALUES (to_date($1,'YYYYMMDD') + interval '12 hours', $2, $3, $4, $5, $6, $6, 1, -1, 1, 1, 0)",
        )
        .bind(day)
        .bind(tx.as_slice())
        .bind(i as i32)
        .bind(block)
        .bind(POOL.as_slice())
        .bind([0xffu8; 20].as_slice())
        .execute(pool)
        .await
        .unwrap();
    }
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn footprint_splits_raw_from_aggregate_and_shrinks_on_prune() {
    let Some(admin) = admin().await else { return };
    let (pool, name) = fresh_db(&admin).await;

    let old = db::footprint(&pool).await.unwrap();
    // Two day partitions of raw swaps, both below the finality line.
    let d_old: String = sqlx::query_scalar("SELECT to_char(current_date - 100, 'YYYYMMDD')")
        .fetch_one(&pool).await.unwrap();
    let d_new: String = sqlx::query_scalar("SELECT to_char(current_date - 1, 'YYYYMMDD')")
        .fetch_one(&pool).await.unwrap();
    seed_day(&pool, &d_old, 200, 500).await;
    seed_day(&pool, &d_new, 200, 600).await;

    let before = db::footprint(&pool).await.unwrap();
    assert!(before.raw_bytes > old.raw_bytes, "raw grew with the swaps");
    assert!(!before.per_table.is_empty());

    // Prune the old partition; the raw footprint drops, aggregates untouched.
    db::prune_raw_partitions(&pool, 30, None).await.unwrap();
    let after = db::footprint(&pool).await.unwrap();
    assert!(after.raw_bytes < before.raw_bytes, "pruning shrank the raw footprint");
    assert_eq!(after.aggregate_bytes, before.aggregate_bytes, "aggregates unchanged");

    drop_db(&admin, pool, &name).await;
}
