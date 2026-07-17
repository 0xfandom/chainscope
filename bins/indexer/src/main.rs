//! chainscope ingestion pipeline (skeleton).

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    tracing::info!("chainscope-indexer: skeleton");
    Ok(())
}
