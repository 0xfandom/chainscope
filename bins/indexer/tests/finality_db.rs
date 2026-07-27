//! The finality tier (#44), against a real Postgres.
//!
//! Proves the three promises the tier rests on:
//!   * `finalized_height` advances with the chain and is strictly monotonic —
//!     a lower report never un-finalises a frozen block;
//!   * the `blocks` reorg window is pruned down to the still-eligible band —
//!     headers at or below the finality line are gone, those above remain;
//!   * a live `FinalityTracker` tick reads the tip and its finality line off a
//!     source and folds both into `chain_state`.
//!
//!   DATABASE_URL=postgres://chainscope:chainscope@localhost:5432/chainscope \
//!     cargo test -p chainscope-indexer --test finality_db -- --ignored --nocapture

use std::sync::Arc;
use std::time::Duration;

use chainscope_core::source::ChainSource;
use chainscope_indexer::{
    db,
    finality::FinalityTracker,
    testkit::SyntheticChain,
};
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use tokio_util::sync::CancellationToken;

async fn admin() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL").ok()?;
    PgPoolOptions::new().max_connections(2).connect(&url).await.ok()
}

async fn fresh_db(admin: &PgPool, tag: &str) -> (PgPool, String) {
    let name = format!("chainscope_finality_{}_{}", std::process::id(), tag);
    sqlx::query(&format!(r#"DROP DATABASE IF EXISTS "{name}" WITH (FORCE)"#))
        .execute(admin)
        .await
        .ok();
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

/// Insert stub headers `0..=high` into the `blocks` reorg window.
async fn seed_headers(pool: &PgPool, high: u64) {
    for n in 0..=high {
        sqlx::query(
            "INSERT INTO blocks (number, block_hash, parent_hash, block_time)
             VALUES ($1, $2, $3, now()) ON CONFLICT (number) DO NOTHING",
        )
        .bind(n as i64)
        .bind(vec![n as u8; 32])
        .bind(vec![n.saturating_sub(1) as u8; 32])
        .execute(pool)
        .await
        .unwrap();
    }
}

async fn header_numbers(pool: &PgPool) -> Vec<i64> {
    sqlx::query("SELECT number FROM blocks ORDER BY number")
        .fetch_all(pool)
        .await
        .unwrap()
        .iter()
        .map(|r| r.get::<i64, _>("number"))
        .collect()
}

async fn head_height(pool: &PgPool) -> Option<i64> {
    sqlx::query("SELECT head_height FROM chain_state WHERE id = 1")
        .fetch_one(pool)
        .await
        .unwrap()
        .get("head_height")
}

/// Advancing the tier moves the finality line and prunes the window down to the
/// blocks still eligible for a reorg.
#[tokio::test]
#[ignore = "requires a running Postgres"]
async fn advancing_finality_prunes_the_reorg_window() {
    let Some(admin) = admin().await else {
        eprintln!("skipped: set DATABASE_URL to a running Postgres");
        return;
    };
    let (pool, name) = fresh_db(&admin, "prune").await;

    // A fresh database has no finality line yet.
    assert_eq!(db::load_finalized_height(&pool).await.unwrap(), None);

    seed_headers(&pool, 10).await;
    let u = db::advance_finality(&pool, 10, 5).await.unwrap();

    assert_eq!(u.finalized_height, 5, "the line is at the reported finalised block");
    assert_eq!(u.headers_pruned, 6, "blocks 0..=5 are frozen and pruned");
    assert_eq!(db::load_finalized_height(&pool).await.unwrap(), Some(5));
    assert_eq!(head_height(&pool).await, Some(10), "head tracks the tip");
    assert_eq!(
        header_numbers(&pool).await,
        vec![6, 7, 8, 9, 10],
        "only the still-eligible band survives"
    );

    eprintln!("finality prune OK: line=5, window=[6..=10]");
    drop_db(&admin, pool, &name).await;
}

/// A later poll that reports an earlier finality line (a provider hiccup) never
/// regresses the stored line and never prunes a block back.
#[tokio::test]
#[ignore = "requires a running Postgres"]
async fn finality_is_monotonic_under_a_lower_report() {
    let Some(admin) = admin().await else {
        eprintln!("skipped: set DATABASE_URL to a running Postgres");
        return;
    };
    let (pool, name) = fresh_db(&admin, "monotonic").await;

    seed_headers(&pool, 20).await;
    db::advance_finality(&pool, 20, 12).await.unwrap();
    assert_eq!(db::load_finalized_height(&pool).await.unwrap(), Some(12));

    // A regressive report: lower head, lower finalised.
    let u = db::advance_finality(&pool, 15, 4).await.unwrap();

    assert_eq!(u.finalized_height, 12, "the line held at its high-water mark");
    assert_eq!(u.headers_pruned, 0, "a regressive report prunes nothing");
    assert_eq!(db::load_finalized_height(&pool).await.unwrap(), Some(12));
    assert_eq!(head_height(&pool).await, Some(20), "head did not drop either");
    // The window is exactly the post-12 band, unchanged by the regressive call.
    assert_eq!(header_numbers(&pool).await, (13..=20).collect::<Vec<_>>());

    eprintln!("finality monotonic OK: held at 12 under a 4 report");
    drop_db(&admin, pool, &name).await;
}

/// A `FinalityTracker` tick reads the tip and finality line off a source and
/// stores them — the synthetic chain finalises at `height - 64`.
#[tokio::test]
#[ignore = "requires a running Postgres"]
async fn a_tracker_tick_reads_the_chain_and_advances() {
    let Some(admin) = admin().await else {
        eprintln!("skipped: set DATABASE_URL to a running Postgres");
        return;
    };
    let (pool, name) = fresh_db(&admin, "tick").await;

    let source: Arc<dyn ChainSource> = Arc::new(SyntheticChain::new(200));
    let tracker = FinalityTracker::new(
        Arc::clone(&source),
        pool.clone(),
        Duration::from_millis(10),
        CancellationToken::new(),
    );

    let u = tracker.tick().await.unwrap();

    assert_eq!(u.finalized_height, 136, "200 - 64 finality depth");
    assert_eq!(db::load_finalized_height(&pool).await.unwrap(), Some(136));
    assert_eq!(head_height(&pool).await, Some(200), "head is the tip");

    eprintln!("tracker tick OK: head=200, finalized=136");
    drop_db(&admin, pool, &name).await;
}
