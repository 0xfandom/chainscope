//! Pools and keyset-paginated swaps (#86), against a real Postgres.
//!
//!   DATABASE_URL=postgres://chainscope:chainscope@localhost:5432/chainscope \
//!     cargo test -p chainscope-api --test pools_swaps_db -- --ignored --nocapture

use axum::body::Body;
use axum::http::{Request, StatusCode};
use chainscope_api::{app, AppState};
use sqlx::postgres::{PgPool, PgPoolOptions};
use tower::ServiceExt;

const POOL: [u8; 20] = [0x9a; 20];
const TOKEN0: [u8; 20] = [0x70; 20];
const TOKEN1: [u8; 20] = [0xdc; 20];

fn hex0x(b: &[u8]) -> String {
    format!("0x{}", hex::encode(b))
}

async fn admin() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL").ok()?;
    PgPoolOptions::new().max_connections(2).connect(&url).await.ok()
}

async fn fresh_db(admin: &PgPool) -> (PgPool, String) {
    let name = format!("chainscope_api86_{}", std::process::id());
    sqlx::query(&format!(r#"DROP DATABASE IF EXISTS "{name}" WITH (FORCE)"#))
        .execute(admin)
        .await
        .ok();
    sqlx::query(&format!(r#"CREATE DATABASE "{name}""#)).execute(admin).await.unwrap();
    let base = std::env::var("DATABASE_URL").unwrap();
    let mut url = url::Url::parse(&base).unwrap();
    url.set_path(&format!("/{name}"));
    let pool = PgPoolOptions::new().max_connections(4).connect(url.as_str()).await.unwrap();
    sqlx::migrate!("../../migrations").run(&pool).await.unwrap();
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS swaps_20260724 PARTITION OF swaps \
         FOR VALUES FROM ('2026-07-24') TO ('2026-07-25')",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO pools (address, token0, token1, fee, tick_spacing, \
             token0_symbol, token0_decimals, token1_symbol, token1_decimals, is_indexed) \
         VALUES ($1,$2,$3,3000,60,'TKN',18,'USDC',6,true)",
    )
    .bind(POOL.as_slice())
    .bind(TOKEN0.as_slice())
    .bind(TOKEN1.as_slice())
    .execute(&pool)
    .await
    .unwrap();
    // Five swaps at blocks 100..=104.
    for n in 100i64..=104 {
        let mut tx = [0u8; 32];
        tx[24..].copy_from_slice(&(n as u64).to_be_bytes());
        sqlx::query(
            "INSERT INTO swaps (block_time, tx_hash, log_index, block_number, pool, sender, \
                 recipient, amount0, amount1, sqrt_price_x96, liquidity, tick) \
             VALUES (to_timestamp(1784894400), $1, 0, $2, $3, $4, $5, 1, -1, 1, 1, 0)",
        )
        .bind(tx.as_slice())
        .bind(n)
        .bind(POOL.as_slice())
        .bind([0xffu8; 20].as_slice())
        .bind([0x11u8; 20].as_slice())
        .execute(&pool)
        .await
        .unwrap();
    }
    (pool, name)
}

async fn drop_db(admin: &PgPool, pool: PgPool, name: &str) {
    pool.close().await;
    sqlx::query(&format!(r#"DROP DATABASE IF EXISTS "{name}" WITH (FORCE)"#))
        .execute(admin)
        .await
        .ok();
}

async fn get(state: &AppState, uri: &str) -> (StatusCode, serde_json::Value) {
    let resp = app(state.clone())
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let json = if bytes.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null)
    };
    (status, json)
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn pools_endpoints() {
    let Some(admin) = admin().await else { return };
    let (pool, name) = fresh_db(&admin).await;
    let state = AppState { pool: pool.clone() };

    let (st, list) = get(&state, "/pools").await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(list.as_array().unwrap().len(), 1);
    assert_eq!(list[0]["address"], hex0x(&POOL));
    assert_eq!(list[0]["token0_symbol"], "TKN");

    let (st, one) = get(&state, &format!("/pools/{}", hex0x(&POOL))).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(one["fee"], 3000);

    // A well-formed but unknown pool is 404; a malformed address is 400.
    let (st, _) = get(&state, &format!("/pools/{}", hex0x(&[0x00; 20]))).await;
    assert_eq!(st, StatusCode::NOT_FOUND);
    let (st, _) = get(&state, "/pools/0xnothex").await;
    assert_eq!(st, StatusCode::BAD_REQUEST);

    drop_db(&admin, pool, &name).await;
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn swaps_paginate_by_keyset() {
    let Some(admin) = admin().await else { return };
    let (pool, name) = fresh_db(&admin).await;
    let state = AppState { pool: pool.clone() };
    let base = format!("/pools/{}/swaps", hex0x(&POOL));

    // Walk every page of size 2 and collect the block numbers seen.
    let mut seen = Vec::new();
    let mut uri = format!("{base}?limit=2");
    let mut pages = 0;
    loop {
        let (st, page) = get(&state, &uri).await;
        assert_eq!(st, StatusCode::OK);
        pages += 1;
        for s in page["items"].as_array().unwrap() {
            seen.push(s["block_number"].as_i64().unwrap());
        }
        match page["next_cursor"].as_str() {
            Some(c) => uri = format!("{base}?limit=2&cursor={c}"),
            None => break,
        }
        assert!(pages < 10, "pagination did not terminate");
    }

    // Newest-first, every swap once, no overlap.
    assert_eq!(seen, vec![104, 103, 102, 101, 100]);
    assert_eq!(pages, 3); // 2 + 2 + 1

    // A malformed cursor is rejected.
    let (st, _) = get(&state, &format!("{base}?cursor=zzz")).await;
    assert_eq!(st, StatusCode::BAD_REQUEST);

    drop_db(&admin, pool, &name).await;
}
