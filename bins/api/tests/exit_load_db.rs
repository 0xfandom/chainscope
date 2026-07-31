//! M7 exit criterion (#91): p99 < 50 ms at 200 rps on paginated swaps.
//!
//! Seeds a pool with many swaps, then drives the paginated swaps endpoint at a
//! sustained ~200 rps for a fixed window, from deep cursors (so the keyset — not
//! offset — path is what is measured), and asserts the p99 latency. Ignored by
//! default: it needs a database and takes a few seconds.
//!
//!   DATABASE_URL=postgres://chainscope:chainscope@localhost:5432/chainscope \
//!     cargo test -p chainscope-api --test exit_load_db -- --ignored --nocapture
//!
//! In-process (`oneshot`) so the number is the read path — router, handler,
//! query — not the machine's loopback TCP stack.

use std::time::{Duration, Instant};

use axum::body::Body;
use axum::http::{Request, StatusCode};
use chainscope_api::pagination::Keyset;
use chainscope_api::{app, AppState};
use sqlx::postgres::{PgPool, PgPoolOptions};
use tower::ServiceExt;

const POOL: [u8; 20] = [0x9a; 20];
const SWAPS: i64 = 20_000;
const RPS: usize = 200;
const SECONDS: usize = 3;
const P99_BUDGET: Duration = Duration::from_millis(50);

fn hex0x(b: &[u8]) -> String {
    format!("0x{}", hex::encode(b))
}

async fn admin() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL").ok()?;
    PgPoolOptions::new().max_connections(2).connect(&url).await.ok()
}

async fn fresh_db(admin: &PgPool) -> (PgPool, String) {
    let name = format!("chainscope_api91_{}", std::process::id());
    sqlx::query(&format!(r#"DROP DATABASE IF EXISTS "{name}" WITH (FORCE)"#)).execute(admin).await.ok();
    sqlx::query(&format!(r#"CREATE DATABASE "{name}""#)).execute(admin).await.unwrap();
    let base = std::env::var("DATABASE_URL").unwrap();
    let mut url = url::Url::parse(&base).unwrap();
    url.set_path(&format!("/{name}"));
    // A generous pool so the load harness is not throttled by connection count.
    let pool = PgPoolOptions::new().max_connections(24).connect(url.as_str()).await.unwrap();
    sqlx::migrate!("../../migrations").run(&pool).await.unwrap();
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS swaps_20260724 PARTITION OF swaps \
         FOR VALUES FROM ('2026-07-24') TO ('2026-07-25')",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO pools (address, token0, token1, fee, tick_spacing, is_indexed) \
         VALUES ($1, $2, $3, 3000, 60, true)",
    )
    .bind(POOL.as_slice())
    .bind([0x70u8; 20].as_slice())
    .bind([0xdcu8; 20].as_slice())
    .execute(&pool)
    .await
    .unwrap();
    // Bulk-insert the swaps in one statement: tx_hash is the block number
    // rendered to 32 bytes, so every row is unique.
    sqlx::query(
        "INSERT INTO swaps \
             (block_time, tx_hash, log_index, block_number, pool, sender, recipient, \
              amount0, amount1, sqrt_price_x96, liquidity, tick) \
         SELECT to_timestamp(1784894400), \
                decode(lpad(to_hex(g), 64, '0'), 'hex'), 0, g, $1, $2, $3, 1, -1, 1, 1, 0 \
           FROM generate_series(1, $4) AS g",
    )
    .bind(POOL.as_slice())
    .bind([0xffu8; 20].as_slice())
    .bind([0x11u8; 20].as_slice())
    .bind(SWAPS)
    .execute(&pool)
    .await
    .unwrap();
    (pool, name)
}

async fn drop_db(admin: &PgPool, pool: PgPool, name: &str) {
    pool.close().await;
    sqlx::query(&format!(r#"DROP DATABASE IF EXISTS "{name}" WITH (FORCE)"#)).execute(admin).await.ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "requires DATABASE_URL; load test"]
async fn p99_under_50ms_at_200rps_on_paginated_swaps() {
    let Some(admin) = admin().await else { return };
    let (pool, name) = fresh_db(&admin).await;
    // TTL 0: no caching on this path (swaps are not cached anyway) — the number
    // is the live query, not a memoised response.
    let state = AppState::new(pool.clone(), Duration::ZERO);
    let router = app(state);
    let base = format!("/pools/{}/swaps", hex0x(&POOL));

    let total = RPS * SECONDS;
    let tick = Duration::from_secs(1) / RPS as u32;
    let mut handles = Vec::with_capacity(total);

    // A fixed-rate ticker paces the offered load and compensates for per-spawn
    // work, which a plain sleep-per-iteration cannot.
    let mut ticker = tokio::time::interval(tick);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let started = Instant::now();
    for i in 0..total {
        ticker.tick().await;
        // Vary the cursor so reads land at different depths — proving the keyset
        // path stays flat, not just the first page.
        let block = SWAPS - (i as i64 * 7) % (SWAPS - 100);
        let cursor = Keyset {
            block_number: block,
            log_index: 0,
        }
        .encode();
        let uri = format!("{base}?limit=50&cursor={cursor}");
        let r = router.clone();

        handles.push(tokio::spawn(async move {
            let t = Instant::now();
            let resp = r
                .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
                .await
                .unwrap();
            let status = resp.status();
            // Drain the body so the query result is fully materialised.
            let _ = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
            (status, t.elapsed())
        }));
    }

    let mut latencies = Vec::with_capacity(total);
    for h in handles {
        let (status, elapsed) = h.await.unwrap();
        assert_eq!(status, StatusCode::OK);
        latencies.push(elapsed);
    }
    let wall = started.elapsed();

    latencies.sort_unstable();
    let pct = |p: f64| latencies[((p * total as f64).ceil() as usize).min(total) - 1];
    let achieved_rps = total as f64 / wall.as_secs_f64();

    println!(
        "load: {total} reqs over {:.2}s = {:.0} rps | p50 {:?} p95 {:?} p99 {:?} max {:?}",
        wall.as_secs_f64(),
        achieved_rps,
        pct(0.50),
        pct(0.95),
        pct(0.99),
        latencies.last().unwrap(),
    );

    assert!(
        achieved_rps >= RPS as f64 * 0.85,
        "offered ~{RPS} rps, only sustained {achieved_rps:.0}"
    );
    assert!(
        pct(0.99) < P99_BUDGET,
        "p99 {:?} exceeds the {P99_BUDGET:?} budget",
        pct(0.99)
    );

    drop_db(&admin, pool, &name).await;
}
