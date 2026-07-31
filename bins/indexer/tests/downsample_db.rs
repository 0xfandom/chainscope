//! Candle downsampler (#112), against a real Postgres.
//!
//!   DATABASE_URL=postgres://chainscope:chainscope@localhost:5432/chainscope \
//!     cargo test -p chainscope-indexer --test downsample_db -- --ignored --nocapture

use bigdecimal::BigDecimal;
use chainscope_core::types::Address20;
use chainscope_indexer::db;
use sqlx::{postgres::PgPoolOptions, PgPool};

const POOL: Address20 = [0x9a; 20];

async fn admin() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL").ok()?;
    PgPoolOptions::new().max_connections(2).connect(&url).await.ok()
}

async fn fresh_db(admin: &PgPool) -> (PgPool, String) {
    let name = format!("chainscope_dsample_{}", std::process::id());
    sqlx::query(&format!(r#"DROP DATABASE IF EXISTS "{name}" WITH (FORCE)"#)).execute(admin).await.ok();
    sqlx::query(&format!(r#"CREATE DATABASE "{name}""#)).execute(admin).await.unwrap();
    let base = std::env::var("DATABASE_URL").unwrap();
    let mut url = url::Url::parse(&base).unwrap();
    url.set_path(&format!("/{name}"));
    let pool = PgPoolOptions::new().max_connections(4).connect(url.as_str()).await.unwrap();
    db::migrate(&pool).await.unwrap();
    (pool, name)
}

async fn drop_db(admin: &PgPool, pool: PgPool, name: &str) {
    pool.close().await;
    sqlx::query(&format!(r#"DROP DATABASE IF EXISTS "{name}" WITH (FORCE)"#)).execute(admin).await.ok();
}

#[allow(clippy::too_many_arguments)]
async fn insert_1m(pool: &PgPool, minute: &str, open: i64, high: i64, low: i64, close: i64, v0: i64, v1: i64, n: i32) {
    sqlx::query(
        "INSERT INTO ohlcv_1m (pool, bucket, open, high, low, close, volume0, volume1, trade_count) \
         VALUES ($1, $2::timestamptz, $3, $4, $5, $6, $7, $8, $9)",
    )
    .bind(POOL.as_slice())
    .bind(minute)
    .bind(BigDecimal::from(open))
    .bind(BigDecimal::from(high))
    .bind(BigDecimal::from(low))
    .bind(BigDecimal::from(close))
    .bind(BigDecimal::from(v0))
    .bind(BigDecimal::from(v1))
    .bind(n)
    .execute(pool)
    .await
    .unwrap();
}

async fn hour_candle(pool: &PgPool) -> Option<(String, String, String, String, String, i32)> {
    let row: Option<(BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, i32)> = sqlx::query_as(
        "SELECT open, high, low, close, volume0, trade_count FROM ohlcv_1h \
         WHERE bucket = '2026-07-24 12:00:00+00'",
    )
    .fetch_optional(pool)
    .await
    .unwrap();
    row.map(|r| {
        let n = |v: BigDecimal| v.normalized().to_string();
        (n(r.0), n(r.1), n(r.2), n(r.3), n(r.4), r.5)
    })
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn rolls_complete_buckets_and_is_idempotent() {
    let Some(admin) = admin().await else { return };
    let (pool, name) = fresh_db(&admin).await;

    // Three complete minutes in the (long past) hour 2026-07-24 12:00.
    insert_1m(&pool, "2026-07-24 12:00:00+00", 10, 15, 8, 12, 100, 200, 3).await;
    insert_1m(&pool, "2026-07-24 12:01:00+00", 12, 20, 11, 18, 50, 80, 2).await;
    insert_1m(&pool, "2026-07-24 12:02:00+00", 18, 19, 17, 17, 30, 40, 1).await;

    db::downsample(&pool).await.unwrap();

    // open = first minute open (10), close = last minute close (17),
    // high = max (20), low = min (8), volume0 = 180, count = 6.
    let h = hour_candle(&pool).await.unwrap();
    assert_eq!(h, ("10".into(), "20".into(), "8".into(), "17".into(), "180".into(), 6));

    // The day roll-up covers it too.
    let day: i32 = sqlx::query_scalar(
        "SELECT trade_count FROM ohlcv_1d WHERE bucket = '2026-07-24 00:00:00+00'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(day, 6);

    // Idempotent: a second pass recomputes to the same values.
    db::downsample(&pool).await.unwrap();
    assert_eq!(hour_candle(&pool).await.unwrap(), h);

    drop_db(&admin, pool, &name).await;
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn a_still_filling_bucket_is_not_rolled() {
    let Some(admin) = admin().await else { return };
    let (pool, name) = fresh_db(&admin).await;

    // A 1m candle in the *current* wall-clock hour must not be rolled yet.
    sqlx::query(
        "INSERT INTO ohlcv_1m (pool, bucket, open, high, low, close, volume0, volume1, trade_count) \
         VALUES ($1, date_trunc('minute', now()), 1, 1, 1, 1, 1, 1, 1)",
    )
    .bind(POOL.as_slice())
    .execute(&pool)
    .await
    .unwrap();

    db::downsample(&pool).await.unwrap();
    let rolled: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM ohlcv_1h WHERE bucket = date_trunc('hour', now())",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(rolled, 0, "the current, still-filling hour is not frozen");

    drop_db(&admin, pool, &name).await;
}
