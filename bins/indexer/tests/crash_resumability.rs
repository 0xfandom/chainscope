//! The M1 exit criterion, as a repeatable test.
//!
//! The claim M1 makes is behavioural: "kill it at any moment and it resumes
//! correctly." This harness makes that claim testable. It runs the *real*
//! `Producer` and `Writer` against a deterministic synthetic chain, aborts the
//! tasks at a randomised point — the in-process equivalent of `kill -9`, since
//! any transaction that had not committed is simply dropped — restarts from the
//! stored cursor, and repeats until the whole chain is consumed. After every
//! restart it asserts the same invariants: no gaps, no duplicates, and a cursor
//! that never runs ahead of the rows.
//!
//! Ignored by default because it needs a Postgres server it can create
//! databases on. Run it with:
//!
//!   docker compose up -d
//!   DATABASE_URL=postgres://chainscope:chainscope@localhost:5432/chainscope \
//!     cargo test -p chainscope-indexer --test crash_resumability -- --ignored --nocapture
//!
//! Each run works in its own freshly-created database and drops it at the end,
//! so it never touches the developer's data.

use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use chainscope_core::{source::ChainSource, BlockUnit};
use chainscope_indexer::{consumer::Writer, db, producer::Producer, testkit::SyntheticChain};
use rand::{rngs::StdRng, Rng, SeedableRng};
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use tokio_util::sync::CancellationToken;

const HEIGHT: u64 = 150;

// ---------------------------------------------------------------------------
// Ephemeral database plumbing
// ---------------------------------------------------------------------------

static DB_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Admin connection to the server named by `DATABASE_URL`. `None` means the
/// test should skip, so an offline machine still passes.
async fn admin() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL").ok()?;
    PgPoolOptions::new().max_connections(2).connect(&url).await.ok()
}

/// Create a fresh database, migrate it, and hand back a pool plus its name.
async fn fresh_db(admin: &PgPool) -> (PgPool, String) {
    let name = format!(
        "chainscope_harness_{}_{}",
        std::process::id(),
        DB_COUNTER.fetch_add(1, Ordering::SeqCst)
    );
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
    db::migrate(&pool).await.expect("migrate test database");
    (pool, name)
}

async fn drop_db(admin: &PgPool, pool: PgPool, name: &str) {
    pool.close().await;
    // WITH (FORCE) terminates any lingering backend so the drop cannot hang on a
    // connection the pool has not finished releasing.
    sqlx::query(&format!(r#"DROP DATABASE IF EXISTS "{name}" WITH (FORCE)"#))
        .execute(admin)
        .await
        .ok();
}

// ---------------------------------------------------------------------------
// The invariant under test
// ---------------------------------------------------------------------------

/// The exactly-once invariant, checkable at any moment: the stored blocks are a
/// gap-free run starting at 1, there are no duplicates, and the cursor never
/// exceeds the highest stored block.
///
/// This is the whole point of the harness. It must hold after every crash, and
/// it is what a broken, non-atomic cursor update would violate.
async fn check_consistency(pool: &PgPool) -> Result<(), String> {
    // One statement, one snapshot. Reading the blocks and the cursor in two
    // separate statements is a bug: under READ COMMITTED each takes its own
    // snapshot, so a concurrently-landing atomic commit can fall between them
    // and make a perfectly consistent database look inconsistent. The write is
    // atomic; the check must read atomically too, or it invents violations that
    // are not there.
    let row = sqlx::query(
        "SELECT
            (SELECT count(*)                  FROM blocks)                    AS n,
            (SELECT COALESCE(min(number), 0)  FROM blocks)                    AS lo,
            (SELECT COALESCE(max(number), 0)  FROM blocks)                    AS hi,
            (SELECT live_cursor               FROM chain_state WHERE id = 1)  AS cursor",
    )
    .fetch_one(pool)
    .await
    .map_err(|e| e.to_string())?;
    let n: i64 = row.get("n");
    let lo: i64 = row.get("lo");
    let hi: i64 = row.get("hi");
    let cursor: Option<i64> = row.get("cursor");

    if n > 0 {
        if lo != 1 {
            return Err(format!("blocks do not start at 1 (min = {lo})"));
        }
        // count == range width is a gap check and, together with the primary
        // key, a duplicate check.
        if hi - lo + 1 != n {
            return Err(format!("gap: range [{lo}..={hi}] holds only {n} rows"));
        }
    }

    if let Some(c) = cursor {
        if c > hi {
            return Err(format!("cursor {c} exceeds the highest stored block {hi}"));
        }
    }

    Ok(())
}

async fn cursor(pool: &PgPool) -> u64 {
    let c: Option<i64> = sqlx::query("SELECT live_cursor FROM chain_state WHERE id = 1")
        .fetch_one(pool)
        .await
        .unwrap()
        .get(0);
    c.map(|v| v as u64).unwrap_or(0)
}

// ---------------------------------------------------------------------------
// One crash segment: run the real pipeline, then kill it
// ---------------------------------------------------------------------------

/// Start the real producer and writer, let them run for `kill_after`, then abort
/// both — the in-process stand-in for a hard kill. An aborted task's in-flight
/// transaction is dropped and rolled back, exactly as a killed process leaves an
/// uncommitted transaction behind.
async fn run_then_kill(pool: &PgPool, kill_after: Duration) {
    let resume = db::load_live_cursor(pool).await.unwrap();

    let (sink, source) =
        chainscope_core::build_transport::<BlockUnit>(chainscope_core::TransportKind::Channel, 32);

    let chain: Arc<dyn ChainSource> = Arc::new(SyntheticChain::new(HEIGHT));
    let producer = Producer::new(
        chain,
        sink,
        resume,
        1, // configured_start: with no cursor, begin at block 1, not the head
        Duration::from_millis(1),
        CancellationToken::new(),
    );
    // Small batches and a short flush so a randomised kill frequently lands
    // mid-batch, which is the case that matters most.
    let writer = Writer::new(pool.clone(), source, 8, Duration::from_millis(4));

    let ph = tokio::spawn(producer.run());
    let wh = tokio::spawn(writer.run());

    tokio::time::sleep(kill_after).await;

    ph.abort();
    wh.abort();
    // Awaiting the aborted handles guarantees their futures — and the sqlx
    // transaction one of them may hold — have been dropped before the next
    // segment reuses the pool.
    let _ = ph.await;
    let _ = wh.await;
}

/// One full trial: crash and restart at randomised points until the whole chain
/// is stored, checking the invariant after every crash.
async fn run_trial(pool: &PgPool, seed: u64) {
    // Fresh state for this trial; the ephemeral DB is shared across the 50.
    sqlx::query("TRUNCATE blocks").execute(pool).await.unwrap();
    sqlx::query("UPDATE chain_state SET live_cursor = NULL WHERE id = 1")
        .execute(pool)
        .await
        .unwrap();

    let mut rng = StdRng::seed_from_u64(seed);
    let mut restarts = 0u32;

    loop {
        // The invariant must hold at every restart boundary, not just the end.
        if let Err(e) = check_consistency(pool).await {
            panic!("seed {seed}: inconsistent after {restarts} crashes: {e}");
        }
        if cursor(pool).await >= HEIGHT {
            break;
        }

        let kill_after = Duration::from_millis(rng.random_range(2..30));
        run_then_kill(pool, kill_after).await;

        restarts += 1;
        assert!(
            restarts < 2_000,
            "seed {seed}: not converging after {restarts} crashes (cursor {})",
            cursor(pool).await
        );
    }

    // The end state must be the whole chain, exactly once, cursor at the top.
    let row = sqlx::query("SELECT count(*) AS n, COALESCE(max(number),0) AS hi FROM blocks")
        .fetch_one(pool)
        .await
        .unwrap();
    let n: i64 = row.get("n");
    let hi: i64 = row.get("hi");
    assert_eq!(n as u64, HEIGHT, "seed {seed}: wrong block count (gaps or duplicates)");
    assert_eq!(hi as u64, HEIGHT, "seed {seed}: highest block is not the chain tip");
    assert_eq!(cursor(pool).await, HEIGHT, "seed {seed}: cursor is not at the tip");
}

// ---------------------------------------------------------------------------
// The tests
// ---------------------------------------------------------------------------

/// Fifty randomised kill points, every one converging with no gaps or
/// duplicates.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires a Postgres server the test can create databases on"]
async fn fifty_random_kill_points_resume_without_gaps_or_duplicates() {
    let Some(admin) = admin().await else {
        eprintln!("skipped: set DATABASE_URL to a running Postgres");
        return;
    };
    let (pool, name) = fresh_db(&admin).await;

    for seed in 0..50 {
        run_trial(&pool, seed).await;
    }

    drop_db(&admin, pool, &name).await;
    eprintln!("50 trials converged, every restart boundary consistent");
}

/// Proof that the harness has teeth: break the atomic cursor update — advance
/// the cursor without the rows, exactly what a non-atomic writer would leave if
/// it crashed between committing the cursor and committing the rows — and the
/// invariant must catch it.
///
/// A test that cannot fail proves nothing; this shows the check does fail on the
/// specific regression it exists to guard against.
#[tokio::test]
#[ignore = "requires a Postgres server the test can create databases on"]
async fn a_cursor_ahead_of_the_rows_is_detected() {
    let Some(admin) = admin().await else {
        eprintln!("skipped: set DATABASE_URL to a running Postgres");
        return;
    };
    let (pool, name) = fresh_db(&admin).await;

    // Write blocks 1..=5 the correct, atomic way. The invariant holds.
    let chain = SyntheticChain::new(HEIGHT);
    let batch: Vec<_> = (1..=5).map(|n| chain.unit(n)).collect();
    db::write_block_batch(&pool, &batch, false).await.unwrap();
    check_consistency(&pool)
        .await
        .expect("a clean atomic write must be consistent");

    // Now break atomicity by hand: move the cursor to 6 without block 6. This is
    // precisely the state an interrupted non-atomic writer would leave.
    sqlx::query("UPDATE chain_state SET live_cursor = 6 WHERE id = 1")
        .execute(&pool)
        .await
        .unwrap();

    let err = check_consistency(&pool)
        .await
        .expect_err("a cursor ahead of the rows must be detected");
    assert!(err.contains("cursor"), "wrong failure reported: {err}");

    drop_db(&admin, pool, &name).await;
    eprintln!("regression detected as expected: {err}");
}
