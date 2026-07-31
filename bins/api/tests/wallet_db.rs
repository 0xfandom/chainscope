//! Wallet scorecard and trade history (#88), against a real Postgres.
//!
//!   DATABASE_URL=postgres://chainscope:chainscope@localhost:5432/chainscope \
//!     cargo test -p chainscope-api --test wallet_db -- --ignored --nocapture

use axum::body::Body;
use axum::http::{Request, StatusCode};
use chainscope_api::{app, AppState};
use sqlx::postgres::{PgPool, PgPoolOptions};
use tower::ServiceExt;

const W: [u8; 20] = [0x11; 20];
const TOKEN: [u8; 20] = [0x70; 20];

fn hex0x(b: &[u8]) -> String {
    format!("0x{}", hex::encode(b))
}

async fn admin() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL").ok()?;
    PgPoolOptions::new().max_connections(2).connect(&url).await.ok()
}

async fn fresh_db(admin: &PgPool) -> (PgPool, String) {
    let name = format!("chainscope_api88_{}", std::process::id());
    sqlx::query(&format!(r#"DROP DATABASE IF EXISTS "{name}" WITH (FORCE)"#)).execute(admin).await.ok();
    sqlx::query(&format!(r#"CREATE DATABASE "{name}""#)).execute(admin).await.unwrap();
    let base = std::env::var("DATABASE_URL").unwrap();
    let mut url = url::Url::parse(&base).unwrap();
    url.set_path(&format!("/{name}"));
    let pool = PgPoolOptions::new().max_connections(4).connect(url.as_str()).await.unwrap();
    sqlx::migrate!("../../migrations").run(&pool).await.unwrap();

    sqlx::query(
        "INSERT INTO wallet_stats \
             (wallet, realized_pnl_usd, trades, wins, volume_usd, avg_size_usd, last_active_block, excluded) \
         VALUES ($1, 550, 5, 2, 1500, 300, 104, false)",
    )
    .bind(W.as_slice())
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO wallet_positions (wallet, token, qty_held, cost_basis_usd, lots, updated_block) \
         VALUES ($1, $2, 2, 450, '[]'::jsonb, 104)",
    )
    .bind(W.as_slice())
    .bind(TOKEN.as_slice())
    .execute(&pool)
    .await
    .unwrap();
    for (i, blk) in [100i64, 101, 102].into_iter().enumerate() {
        let mut tx = [0u8; 32];
        tx[0] = 0x5e;
        tx[24..].copy_from_slice(&(blk as u64).to_be_bytes());
        sqlx::query(
            "INSERT INTO lot_consumptions \
                 (sell_tx, sell_log, consume_seq, wallet, token, qty_consumed, \
                  lot_unit_cost_usd, lot_acquired_block, proceeds_usd, realized_pnl_usd, sell_block) \
             VALUES ($1, 0, 0, $2, $3, 1, 100, 99, 150, 50, $4)",
        )
        .bind(tx.as_slice())
        .bind(W.as_slice())
        .bind(TOKEN.as_slice())
        .bind(blk)
        .execute(&pool)
        .await
        .unwrap();
        let _ = i;
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
async fn scorecard_reports_stats_positions_and_trail() {
    let Some(admin) = admin().await else { return };
    let (pool, name) = fresh_db(&admin).await;
    let state = AppState::new(pool.clone(), std::time::Duration::ZERO);

    let (st, sc) = get(&state, &format!("/wallets/{}", hex0x(&W))).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(sc["realized_pnl_usd"], "550");
    assert_eq!(sc["trades"], 5);
    assert_eq!(sc["wins"], 2);
    assert_eq!(sc["excluded"], false);
    assert_eq!(sc["open_positions"].as_array().unwrap().len(), 1);
    assert_eq!(sc["open_positions"][0]["token"], hex0x(&TOKEN));
    assert_eq!(sc["open_positions"][0]["qty_held"], "2");
    assert_eq!(sc["recent_realized"].as_array().unwrap().len(), 3);
    // Newest first.
    assert_eq!(sc["recent_realized"][0]["sell_block"], 102);

    // Unknown wallet → 404; bad address → 400.
    let (st, _) = get(&state, &format!("/wallets/{}", hex0x(&[0x99; 20]))).await;
    assert_eq!(st, StatusCode::NOT_FOUND);
    let (st, _) = get(&state, "/wallets/nothex").await;
    assert_eq!(st, StatusCode::BAD_REQUEST);

    drop_db(&admin, pool, &name).await;
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn trades_paginate_by_keyset() {
    let Some(admin) = admin().await else { return };
    let (pool, name) = fresh_db(&admin).await;
    let state = AppState::new(pool.clone(), std::time::Duration::ZERO);
    let base = format!("/wallets/{}/trades", hex0x(&W));

    let mut blocks = Vec::new();
    let mut uri = format!("{base}?limit=2");
    loop {
        let (st, page) = get(&state, &uri).await;
        assert_eq!(st, StatusCode::OK);
        for t in page["items"].as_array().unwrap() {
            blocks.push(t["sell_block"].as_i64().unwrap());
        }
        match page["next_cursor"].as_str() {
            Some(c) => uri = format!("{base}?limit=2&cursor={c}"),
            None => break,
        }
    }
    assert_eq!(blocks, vec![102, 101, 100], "newest-first, each once");

    drop_db(&admin, pool, &name).await;
}
