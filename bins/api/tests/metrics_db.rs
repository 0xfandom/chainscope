//! Prometheus /metrics endpoint (#123), against a real Postgres.
//!
//!   DATABASE_URL=postgres://chainscope:chainscope@localhost:5432/chainscope \
//!     cargo test -p chainscope-api --test metrics_db -- --ignored --nocapture

use axum::body::Body;
use axum::http::{Request, StatusCode};
use chainscope_api::{app, db, AppState};
use sqlx::postgres::{PgPool, PgPoolOptions};
use std::time::Duration;
use tower::ServiceExt;

async fn admin() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL").ok()?;
    PgPoolOptions::new().max_connections(2).connect(&url).await.ok()
}

async fn fresh_db(admin: &PgPool) -> (PgPool, String) {
    let name = format!("chainscope_metrics_{}", std::process::id());
    sqlx::query(&format!(r#"DROP DATABASE IF EXISTS "{name}" WITH (FORCE)"#)).execute(admin).await.ok();
    sqlx::query(&format!(r#"CREATE DATABASE "{name}""#)).execute(admin).await.unwrap();
    let base = std::env::var("DATABASE_URL").unwrap();
    let mut url = url::Url::parse(&base).unwrap();
    url.set_path(&format!("/{name}"));
    let pool = PgPoolOptions::new().max_connections(4).connect(url.as_str()).await.unwrap();
    sqlx::migrate!("../../migrations").run(&pool).await.unwrap();
    sqlx::query("UPDATE chain_state SET head_height = 100, live_cursor = 90, finalized_height = 40 WHERE id = 1")
        .execute(&pool)
        .await
        .unwrap();
    (pool, name)
}

async fn drop_db(admin: &PgPool, pool: PgPool, name: &str) {
    pool.close().await;
    sqlx::query(&format!(r#"DROP DATABASE IF EXISTS "{name}" WITH (FORCE)"#)).execute(admin).await.ok();
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn metrics_renders_prometheus_gauges() {
    let Some(admin) = admin().await else { return };
    let (pool, name) = fresh_db(&admin).await;
    let _ = db::footprint(&pool).await.unwrap(); // sanity: query runs
    let state = AppState::new(pool.clone(), Duration::ZERO);

    let resp = app(state)
        .oneshot(Request::builder().uri("/metrics").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let ct = resp.headers().get("content-type").unwrap().to_str().unwrap().to_owned();
    assert!(ct.starts_with("text/plain"), "prometheus content type: {ct}");

    let body = String::from_utf8(
        axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap().to_vec(),
    )
    .unwrap();

    for metric in [
        "chainscope_head_height",
        "chainscope_ingest_lag",
        "chainscope_footprint_raw_bytes",
        "chainscope_footprint_aggregate_bytes",
    ] {
        assert!(body.contains(metric), "missing {metric} in:\n{body}");
    }
    // lag = head(100) - live_cursor(90) = 10.
    assert!(body.contains("chainscope_ingest_lag 10"), "lag value:\n{body}");

    drop_db(&admin, pool, &name).await;
}
