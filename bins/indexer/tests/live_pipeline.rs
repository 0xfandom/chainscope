//! End-to-end against live Ethereum: the real three-stage pipeline, a real RPC,
//! a real Postgres.
//!
//! This is the M2 exit criterion's "it runs" half — decoded swaps actually land
//! in the database for watched pools, the cursor tracks the head, and a clean
//! stop leaves a resumable state. The field-by-field match against Etherscan is
//! #25.
//!
//! Ignored by default because it needs both a Postgres it can create databases
//! on and a live archive-ish RPC. Run with:
//!
//!   docker compose up -d
//!   DATABASE_URL=postgres://chainscope:chainscope@localhost:5432/chainscope \
//!   CHAINSCOPE_LIVE_RPC=https://rpc.flashbots.net \
//!     cargo test -p chainscope-indexer --test live_pipeline -- --ignored --nocapture
//!
//! It works in its own freshly-created database and drops it at the end, so it
//! never touches development data.

use std::{sync::Arc, time::Duration};

use chainscope_core::{source::ChainSource, types::Address20, BlockUnit, RowBatch};
use chainscope_eth_source::EthSource;
use chainscope_indexer::{consumer::Writer, db, producer::Producer, transformer::Transformer};
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use tokio_util::sync::CancellationToken;

/// The two USDC/WETH pools the project watches — among the busiest on Ethereum,
/// so a short recent window is very likely to contain swaps.
const POOLS: [&str; 2] = [
    "8ad599c3a0ff1de082011efddc58f1908eb6e6d8", // 0.05%
    "88e6a0c2ddd26feeb64f039a2c41296fcb3f5640", // 0.30%
];

/// How many blocks behind the head to begin, so there is history to index
/// immediately rather than waiting one block per 12s.
const WINDOW: u64 = 15;

fn addr(hex: &str) -> Address20 {
    let mut out = [0u8; 20];
    hex::decode_to_slice(hex, &mut out).unwrap();
    out
}

async fn admin() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL").ok()?;
    PgPoolOptions::new().max_connections(2).connect(&url).await.ok()
}

async fn fresh_db(admin: &PgPool) -> (PgPool, String) {
    let name = format!("chainscope_live_{}", std::process::id());
    sqlx::query(&format!(r#"DROP DATABASE IF EXISTS "{name}" WITH (FORCE)"#))
        .execute(admin)
        .await
        .ok();
    sqlx::query(&format!(r#"CREATE DATABASE "{name}""#))
        .execute(admin)
        .await
        .expect("create test database");

    let base = std::env::var("DATABASE_URL").unwrap();
    let mut url = url::Url::parse(&base).unwrap();
    url.set_path(&format!("/{name}"));
    let pool = PgPoolOptions::new()
        .max_connections(6)
        .connect(url.as_str())
        .await
        .expect("connect to test database");
    db::migrate(&pool).await.expect("migrate");
    db::ensure_partitions(&pool).await.expect("partitions");
    (pool, name)
}

async fn drop_db(admin: &PgPool, pool: PgPool, name: &str) {
    pool.close().await;
    sqlx::query(&format!(r#"DROP DATABASE IF EXISTS "{name}" WITH (FORCE)"#))
        .execute(admin)
        .await
        .ok();
}

async fn cursor(pool: &PgPool) -> Option<i64> {
    sqlx::query("SELECT live_cursor FROM chain_state WHERE id = 1")
        .fetch_one(pool)
        .await
        .unwrap()
        .get::<Option<i64>, _>(0)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires a Postgres server and a live RPC (set DATABASE_URL and CHAINSCOPE_LIVE_RPC)"]
async fn live_pipeline_decodes_swaps_and_resumes() {
    let (Some(admin), Ok(rpc)) = (admin().await, std::env::var("CHAINSCOPE_LIVE_RPC")) else {
        eprintln!("skipped: set DATABASE_URL and CHAINSCOPE_LIVE_RPC");
        return;
    };
    let rpc = url::Url::parse(&rpc).expect("CHAINSCOPE_LIVE_RPC must be a URL");
    let watched: Vec<Address20> = POOLS.iter().map(|p| addr(p)).collect();

    let (pool, name) = fresh_db(&admin).await;

    // Reach the chain and pick a recent start so there is history to index now.
    let source: Arc<dyn ChainSource> = Arc::new(EthSource::new(&rpc, &watched));
    let head = source.latest_block().await.expect("reach the chain");
    let start = head - WINDOW;
    eprintln!("head={head} starting at {start} (head-{WINDOW})");

    // --- First run: index the window, then stop. -------------------------------
    let stored_to = run_until_caught_up(&pool, Arc::clone(&source), &watched, None, start, head).await;
    assert!(
        stored_to >= head,
        "first run should reach the head: stored_to={stored_to} head={head}"
    );

    // Contiguity and the swap count, in one snapshot.
    let row = sqlx::query(
        "SELECT
            (SELECT count(*) FROM blocks)                                   AS n,
            (SELECT min(number) FROM blocks)                                AS lo,
            (SELECT max(number) FROM blocks)                                AS hi,
            (SELECT count(*) FROM swaps WHERE block_number BETWEEN $1 AND $2) AS swaps",
    )
    .bind(start as i64)
    .bind(head as i64)
    .fetch_one(&pool)
    .await
    .unwrap();
    let n: i64 = row.get("n");
    let lo: i64 = row.get("lo");
    let hi: i64 = row.get("hi");
    let swaps: i64 = row.get("swaps");
    eprintln!("stored {n} blocks [{lo}..={hi}], {swaps} swaps");

    assert_eq!(lo as u64, start, "should start exactly at the configured start");
    assert_eq!(hi - lo + 1, n, "blocks must be gap-free and unique");
    assert!(
        swaps >= 1,
        "the two busiest USDC/WETH pools should yield at least one swap over {WINDOW} blocks; got {swaps}"
    );

    // --- Restart: resume from the stored cursor, no re-indexing, no dupes. ------
    let resume = cursor(&pool).await;
    let before = n;
    let head2 = source.latest_block().await.expect("reach the chain again");
    run_until_caught_up(&pool, Arc::clone(&source), &watched, resume, start, head2).await;

    let row = sqlx::query(
        "SELECT count(*) AS n, min(number) AS lo, max(number) AS hi,
                (count(*) = max(number) - min(number) + 1) AS contiguous
           FROM blocks",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let n2: i64 = row.get("n");
    let contiguous: bool = row.get("contiguous");
    assert!(contiguous, "still gap-free and unique after a restart");
    assert!(n2 >= before, "the restart must not lose blocks");
    // The resume point was the cursor, not the start: block `start` was not
    // re-fetched into a duplicate (guaranteed by contiguity + no count drop).
    eprintln!("after restart: {n2} blocks, cursor was {resume:?}");

    drop_db(&admin, pool, &name).await;
    eprintln!("live pipeline OK: decoded swaps stored, cursor tracked, resumed cleanly");
}

/// Run the real producer -> transformer -> writer until the stored cursor
/// reaches `target` (or a timeout), then cancel and wait for a clean stop.
/// Returns the cursor reached.
async fn run_until_caught_up(
    pool: &PgPool,
    source: Arc<dyn ChainSource>,
    watched: &[Address20],
    resume: Option<i64>,
    start: u64,
    target: u64,
) -> u64 {
    let (raw_sink, raw_source) =
        chainscope_core::build_transport::<BlockUnit>(chainscope_core::TransportKind::Channel, 64);
    let (row_sink, row_source) =
        chainscope_core::build_transport::<RowBatch>(chainscope_core::TransportKind::Channel, 64);

    let cancel = CancellationToken::new();
    let producer = Producer::new(
        source,
        raw_sink,
        resume.map(|c| c as u64),
        start,
        Duration::from_millis(200),
        cancel.clone(),
    );
    let transformer = Transformer::new(raw_source, row_sink, watched.to_vec());
    let writer = Writer::new(pool.clone(), row_source, 8, Duration::from_millis(200));

    let ph = tokio::spawn(producer.run());
    let th = tokio::spawn(transformer.run());
    let wh = tokio::spawn(writer.run());

    // Poll the cursor until it reaches the target the run began with, capped so a
    // stalled RPC fails the test rather than hanging it.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    loop {
        if cursor(pool).await.map(|c| c as u64).unwrap_or(0) >= target {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            eprintln!("timed out before reaching {target}");
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // Clean shutdown: cancel the producer; the closed streams wind the rest down.
    cancel.cancel();
    let _ = ph.await;
    let _ = th.await;
    let _ = wh.await;

    cursor(pool).await.map(|c| c as u64).unwrap_or(0)
}
