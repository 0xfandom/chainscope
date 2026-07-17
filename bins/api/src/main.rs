//! chainscope read API (skeleton).

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    tracing::info!("chainscope-api: skeleton");
    Ok(())
}
