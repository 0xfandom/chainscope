//! chainscope ingestion pipeline.

mod db;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // A missing .env is not an error — the environment may already carry the
    // variables, which is how it works in Docker.
    let _ = dotenvy::dotenv();
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let pool = db::connect().await?;
    tracing::info!("connected to postgres");

    db::migrate(&pool).await?;
    tracing::info!("migrations up to date");

    let created = db::ensure_partitions(&pool).await?;
    tracing::info!(created, "day partitions ensured");

    tracing::info!("schema ready; pipeline not implemented yet");
    Ok(())
}
