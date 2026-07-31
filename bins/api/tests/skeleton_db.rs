//! API skeleton (#85), against a real Postgres.
//!
//! Builds the exact router the binary serves and drives it through `oneshot`, no
//! socket. Gated on DATABASE_URL so an offline machine still passes.
//!
//!   DATABASE_URL=postgres://chainscope:chainscope@localhost:5432/chainscope \
//!     cargo test -p chainscope-api --test skeleton_db -- --ignored --nocapture

use axum::body::Body;
use axum::http::{Request, StatusCode};
use chainscope_api::{app, db, AppState};
use tower::ServiceExt; // oneshot

async fn state() -> Option<AppState> {
    let url = std::env::var("DATABASE_URL").ok()?;
    let pool = db::connect(&url, 4).await.ok()?;
    Some(AppState { pool })
}

async fn body_json(resp: axum::response::Response) -> serde_json::Value {
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn healthz_is_ok_when_the_database_answers() {
    let Some(state) = state().await else { return };
    let resp = app(state)
        .oneshot(Request::builder().uri("/healthz").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn status_reports_the_ingestion_heartbeat() {
    let Some(state) = state().await else { return };
    let resp = app(state)
        .oneshot(Request::builder().uri("/status").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let json = body_json(resp).await;
    // The five heartbeat fields are always present (values may be null before the
    // pipeline has run).
    for key in ["head_height", "finalized_height", "live_cursor", "backfill_cursor", "lag"] {
        assert!(json.get(key).is_some(), "status is missing {key}: {json}");
    }
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn an_unknown_route_is_404() {
    let Some(state) = state().await else { return };
    let resp = app(state)
        .oneshot(Request::builder().uri("/nope").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}
