//! New-pool capture (#103), against a real Postgres.
//!
//!   DATABASE_URL=postgres://chainscope:chainscope@localhost:5432/chainscope \
//!     cargo test -p chainscope-indexer --test sniffer_capture_db -- --ignored --nocapture

use chainscope_core::types::Address20;
use chainscope_indexer::{db, sniffer::risk_flags};
use sqlx::{postgres::PgPoolOptions, PgPool, Row};

const POOL: Address20 = [0x9a; 20];
const T0: Address20 = [0x70; 20];
const T1: Address20 = [0xdc; 20];

async fn admin() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL").ok()?;
    PgPoolOptions::new().max_connections(2).connect(&url).await.ok()
}

async fn fresh_db(admin: &PgPool) -> (PgPool, String) {
    let name = format!("chainscope_sniff_{}", std::process::id());
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

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn capture_is_idempotent_and_not_indexed() {
    let Some(admin) = admin().await else { return };
    let (pool, name) = fresh_db(&admin).await;
    let flags = risk_flags(false, 0);

    // First capture inserts.
    assert!(db::capture_new_pool(&pool, &POOL, &T0, &T1, 3000, 60, 19_000_000, &flags).await.unwrap());
    // Re-scan of the same pool captures nothing.
    assert!(!db::capture_new_pool(&pool, &POOL, &T0, &T1, 3000, 60, 19_000_000, &flags).await.unwrap());

    let row = sqlx::query("SELECT is_indexed, fee, risk_flags::text AS risk FROM pools WHERE address = $1")
        .bind(POOL.as_slice())
        .fetch_one(&pool)
        .await
        .unwrap();
    let is_indexed: bool = row.get("is_indexed");
    let fee: i32 = row.get("fee");
    let risk: String = row.get("risk");
    assert!(!is_indexed, "discovery never widens ingestion");
    assert_eq!(fee, 3000);
    assert!(risk.contains("no_liquidity"), "risk scorecard stored: {risk}");

    drop_db(&admin, pool, &name).await;
}
