//! Leaderboard and new-pools endpoints (#89), against a real Postgres.
//!
//!   DATABASE_URL=postgres://chainscope:chainscope@localhost:5432/chainscope \
//!     cargo test -p chainscope-api --test leaderboard_newpools_db -- --ignored --nocapture

use axum::body::Body;
use axum::http::{Request, StatusCode};
use chainscope_api::{app, AppState};
use sqlx::postgres::{PgPool, PgPoolOptions};
use tower::ServiceExt;

const W1: [u8; 20] = [0x0a; 20]; // realises 100, kept
const W2: [u8; 20] = [0x0c; 20]; // realises 200, excluded
const P1: [u8; 20] = [0x01; 20];
const P2: [u8; 20] = [0x02; 20];

fn hex0x(b: &[u8]) -> String {
    format!("0x{}", hex::encode(b))
}

async fn admin() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL").ok()?;
    PgPoolOptions::new().max_connections(2).connect(&url).await.ok()
}

async fn fresh_db(admin: &PgPool, tag: &str) -> (PgPool, String) {
    let name = format!("chainscope_api89_{}_{}", std::process::id(), tag);
    sqlx::query(&format!(r#"DROP DATABASE IF EXISTS "{name}" WITH (FORCE)"#)).execute(admin).await.ok();
    sqlx::query(&format!(r#"CREATE DATABASE "{name}""#)).execute(admin).await.unwrap();
    let base = std::env::var("DATABASE_URL").unwrap();
    let mut url = url::Url::parse(&base).unwrap();
    url.set_path(&format!("/{name}"));
    let pool = PgPoolOptions::new().max_connections(4).connect(url.as_str()).await.unwrap();
    sqlx::migrate!("../../migrations").run(&pool).await.unwrap();
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
    (status, serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null))
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn leaderboard_is_ranked_and_wash_excluded() {
    let Some(admin) = admin().await else { return };
    let (pool, name) = fresh_db(&admin, "lb").await;
    let state = AppState { pool: pool.clone() };

    // Before any refresh the matview is unpopulated -> the endpoint returns [].
    let (st, empty) = get(&state, "/leaderboard").await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(empty.as_array().unwrap().len(), 0, "unpopulated matview is empty, not an error");

    for (w, pnl, excl) in [(W1, 100, false), (W2, 200, true)] {
        sqlx::query(
            "INSERT INTO wallet_stats (wallet, realized_pnl_usd, trades, wins, volume_usd, excluded) \
             VALUES ($1, $2, 1, 1, $2, $3)",
        )
        .bind(w.as_slice())
        .bind(pnl)
        .bind(excl)
        .execute(&pool)
        .await
        .unwrap();
    }
    sqlx::query("REFRESH MATERIALIZED VIEW leaderboard").execute(&pool).await.unwrap();

    let (st, board) = get(&state, "/leaderboard").await;
    assert_eq!(st, StatusCode::OK);
    let arr = board.as_array().unwrap();
    assert_eq!(arr.len(), 1, "the excluded wallet is absent");
    assert_eq!(arr[0]["wallet"], hex0x(&W1));
    assert_eq!(arr[0]["realized_pnl_usd"], "100");

    drop_db(&admin, pool, &name).await;
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn new_pools_newest_first() {
    let Some(admin) = admin().await else { return };
    let (pool, name) = fresh_db(&admin, "np").await;
    let state = AppState { pool: pool.clone() };

    for (addr, when) in [(P1, 1_784_800_000i64), (P2, 1_784_900_000)] {
        sqlx::query(
            "INSERT INTO pools (address, token0, token1, fee, tick_spacing, is_indexed, discovered_at) \
             VALUES ($1, $2, $3, 3000, 60, false, to_timestamp($4))",
        )
        .bind(addr.as_slice())
        .bind([0x70u8; 20].as_slice())
        .bind([0xdcu8; 20].as_slice())
        .bind(when)
        .execute(&pool)
        .await
        .unwrap();
    }

    let (st, page) = get(&state, "/pools/new").await;
    assert_eq!(st, StatusCode::OK, "/pools/new resolves, not shadowed by /pools/:address");
    let items = page["items"].as_array().unwrap();
    assert_eq!(items.len(), 2);
    assert_eq!(items[0]["address"], hex0x(&P2), "most recently discovered first");
    assert_eq!(items[1]["address"], hex0x(&P1));

    drop_db(&admin, pool, &name).await;
}
