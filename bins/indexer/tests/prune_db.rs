//! Partition pruner with a finality floor (#113), against a real Postgres.
//!
//!   DATABASE_URL=postgres://chainscope:chainscope@localhost:5432/chainscope \
//!     cargo test -p chainscope-indexer --test prune_db -- --ignored --nocapture

use chainscope_core::types::Address20;
use chainscope_indexer::db;
use sqlx::{postgres::PgPoolOptions, PgPool};

const POOL: Address20 = [0x9a; 20];
const FINALIZED: i64 = 1_000;

async fn admin() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL").ok()?;
    PgPoolOptions::new().max_connections(2).connect(&url).await.ok()
}

async fn fresh_db(admin: &PgPool) -> (PgPool, String) {
    let name = format!("chainscope_prune_{}", std::process::id());
    sqlx::query(&format!(r#"DROP DATABASE IF EXISTS "{name}" WITH (FORCE)"#)).execute(admin).await.ok();
    sqlx::query(&format!(r#"CREATE DATABASE "{name}""#)).execute(admin).await.unwrap();
    let base = std::env::var("DATABASE_URL").unwrap();
    let mut url = url::Url::parse(&base).unwrap();
    url.set_path(&format!("/{name}"));
    let pool = PgPoolOptions::new().max_connections(4).connect(url.as_str()).await.unwrap();
    db::migrate(&pool).await.unwrap();
    sqlx::query("UPDATE chain_state SET finalized_height = $1 WHERE id = 1")
        .bind(FINALIZED)
        .execute(&pool)
        .await
        .unwrap();
    (pool, name)
}

async fn drop_db(admin: &PgPool, pool: PgPool, name: &str) {
    pool.close().await;
    sqlx::query(&format!(r#"DROP DATABASE IF EXISTS "{name}" WITH (FORCE)"#)).execute(admin).await.ok();
}

/// A day string `current_date - offset` as YYYYMMDD.
async fn day(pool: &PgPool, offset_days: i64) -> String {
    sqlx::query_scalar("SELECT to_char(current_date - $1::int, 'YYYYMMDD')")
        .bind(offset_days as i32)
        .fetch_one(pool)
        .await
        .unwrap()
}

/// Create a swaps day partition and put one swap in it at `block`.
async fn seed_day(pool: &PgPool, day: &str, block: i64) {
    let name = format!("swaps_{day}");
    sqlx::query(&format!(
        "CREATE TABLE IF NOT EXISTS {name} PARTITION OF swaps \
         FOR VALUES FROM (to_date('{day}','YYYYMMDD')) TO (to_date('{day}','YYYYMMDD') + 1)"
    ))
    .execute(pool)
    .await
    .unwrap();
    let mut tx = [0u8; 32];
    tx[..8].copy_from_slice(&(block as u64).to_be_bytes());
    sqlx::query(
        "INSERT INTO swaps (block_time, tx_hash, log_index, block_number, pool, sender, \
             recipient, amount0, amount1, sqrt_price_x96, liquidity, tick) \
         VALUES (to_date($1,'YYYYMMDD') + interval '12 hours', $2, 0, $3, $4, $5, $5, 1, -1, 1, 1, 0)",
    )
    .bind(day)
    .bind(tx.as_slice())
    .bind(block)
    .bind(POOL.as_slice())
    .bind([0xffu8; 20].as_slice())
    .execute(pool)
    .await
    .unwrap();
}

async fn exists(pool: &PgPool, table: &str) -> bool {
    let reg: Option<String> = sqlx::query_scalar("SELECT to_regclass($1)::text")
        .bind(table)
        .fetch_one(pool)
        .await
        .unwrap();
    reg.is_some()
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn drops_old_finalized_partitions_keeps_the_rest() {
    let Some(admin) = admin().await else { return };
    let (pool, name) = fresh_db(&admin).await;

    let old_final = day(&pool, 100).await; // old, below finality  -> DROP
    let old_pending = day(&pool, 60).await; // old, but above finality -> KEEP
    let recent = day(&pool, 1).await; // inside the window -> KEEP

    seed_day(&pool, &old_final, 500).await; // block 500 < finalized 1000
    seed_day(&pool, &old_pending, 2_000).await; // block 2000 > finalized 1000
    seed_day(&pool, &recent, 3_000).await;

    let dropped = db::prune_raw_partitions(&pool, 30, None).await.unwrap();

    assert_eq!(dropped, vec![format!("swaps_{old_final}")], "only the old, finalized day");
    assert!(!exists(&pool, &format!("swaps_{old_final}")).await, "old finalized dropped");
    assert!(exists(&pool, &format!("swaps_{old_pending}")).await, "reorg-eligible kept though old");
    assert!(exists(&pool, &format!("swaps_{recent}")).await, "recent kept");

    drop_db(&admin, pool, &name).await;
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn nothing_dropped_before_finality_is_known() {
    let Some(admin) = admin().await else { return };
    let (pool, name) = fresh_db(&admin).await;
    sqlx::query("UPDATE chain_state SET finalized_height = NULL WHERE id = 1").execute(&pool).await.unwrap();

    let old = day(&pool, 100).await;
    seed_day(&pool, &old, 500).await;

    assert!(db::prune_raw_partitions(&pool, 30, None).await.unwrap().is_empty(), "no finality line -> keep all");
    assert!(exists(&pool, &format!("swaps_{old}")).await);

    drop_db(&admin, pool, &name).await;
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn dumps_a_partition_to_csv_before_dropping_it() {
    let Some(admin) = admin().await else { return };
    let (pool, name) = fresh_db(&admin).await;

    let old = day(&pool, 100).await;
    seed_day(&pool, &old, 500).await; // below finality -> will be dropped

    let dir = std::env::temp_dir().join(format!("chainscope_dump_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);

    let dropped = db::prune_raw_partitions(&pool, 30, Some(dir.as_path())).await.unwrap();
    assert_eq!(dropped, vec![format!("swaps_{old}")]);

    // The dump exists and carries the row (its block number appears in the CSV).
    let csv = std::fs::read_to_string(dir.join(format!("swaps_{old}.csv"))).unwrap();
    assert!(csv.contains("block_number"), "header written");
    assert!(csv.contains("500"), "the dropped row is in the dump");
    assert!(!exists(&pool, &format!("swaps_{old}")).await, "partition dropped after dump");

    let _ = std::fs::remove_dir_all(&dir);
    drop_db(&admin, pool, &name).await;
}
