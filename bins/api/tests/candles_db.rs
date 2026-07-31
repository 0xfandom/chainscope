//! OHLCV candles endpoint (#87), against a real Postgres.
//!
//!   DATABASE_URL=postgres://chainscope:chainscope@localhost:5432/chainscope \
//!     cargo test -p chainscope-api --test candles_db -- --ignored --nocapture

use axum::body::Body;
use axum::http::{Request, StatusCode};
use chainscope_api::{app, AppState};
use sqlx::postgres::{PgPool, PgPoolOptions};
use tower::ServiceExt;

const POOL: [u8; 20] = [0x9a; 20];
const T0: i64 = 1_784_894_400; // 2026-07-24T12:00:00Z

fn hex0x(b: &[u8]) -> String {
    format!("0x{}", hex::encode(b))
}

async fn admin() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL").ok()?;
    PgPoolOptions::new().max_connections(2).connect(&url).await.ok()
}

async fn fresh_db(admin: &PgPool) -> (PgPool, String) {
    let name = format!("chainscope_api87_{}", std::process::id());
    sqlx::query(&format!(r#"DROP DATABASE IF EXISTS "{name}" WITH (FORCE)"#)).execute(admin).await.ok();
    sqlx::query(&format!(r#"CREATE DATABASE "{name}""#)).execute(admin).await.unwrap();
    let base = std::env::var("DATABASE_URL").unwrap();
    let mut url = url::Url::parse(&base).unwrap();
    url.set_path(&format!("/{name}"));
    let pool = PgPoolOptions::new().max_connections(4).connect(url.as_str()).await.unwrap();
    sqlx::migrate!("../../migrations").run(&pool).await.unwrap();
    sqlx::query(
        "INSERT INTO pools (address, token0, token1, fee, tick_spacing, is_indexed) \
         VALUES ($1,$2,$3,3000,60,true)",
    )
    .bind(POOL.as_slice())
    .bind([0x70u8; 20].as_slice())
    .bind([0xdcu8; 20].as_slice())
    .execute(&pool)
    .await
    .unwrap();
    // Three 1m candles at T0, T0+60, T0+120.
    for i in 0i64..3 {
        sqlx::query(
            "INSERT INTO ohlcv_1m (pool, bucket, open, high, low, close, volume0, volume1, trade_count) \
             VALUES ($1, to_timestamp($2), 1, 2, 0, 1.5, 10, 20, 3)",
        )
        .bind(POOL.as_slice())
        .bind(T0 + i * 60)
        .execute(&pool)
        .await
        .unwrap();
    }
    (pool, name)
}

async fn drop_db(admin: &PgPool, pool: PgPool, name: &str) {
    pool.close().await;
    sqlx::query(&format!(r#"DROP DATABASE IF EXISTS "{name}" WITH (FORCE)"#)).execute(admin).await.ok();
}

async fn get(state: &AppState, uri: &str) -> (StatusCode, serde_json::Value) {
    let resp = app(state.clone())
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let json = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    (status, json)
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn candles_paginate_newest_first() {
    let Some(admin) = admin().await else { return };
    let (pool, name) = fresh_db(&admin).await;
    let state = AppState { pool: pool.clone() };
    let base = format!("/pools/{}/candles", hex0x(&POOL));

    let mut buckets = Vec::new();
    let mut uri = format!("{base}?resolution=1m&limit=2");
    loop {
        let (st, page) = get(&state, &uri).await;
        assert_eq!(st, StatusCode::OK);
        for c in page["items"].as_array().unwrap() {
            buckets.push(c["bucket"].as_i64().unwrap());
        }
        match page["next_cursor"].as_str() {
            Some(c) => uri = format!("{base}?resolution=1m&limit=2&cursor={c}"),
            None => break,
        }
    }
    assert_eq!(buckets, vec![T0 + 120, T0 + 60, T0], "newest bucket first, each once");

    // Candle fields survive as decimal strings.
    let (_, first) = get(&state, &format!("{base}?resolution=1m&limit=1")).await;
    assert_eq!(first["items"][0]["close"], "1.5");
    assert_eq!(first["items"][0]["trade_count"], 3);

    // Unknown resolution → 400.
    let (st, _) = get(&state, &format!("{base}?resolution=5m")).await;
    assert_eq!(st, StatusCode::BAD_REQUEST);

    drop_db(&admin, pool, &name).await;
}
