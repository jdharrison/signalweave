#![deny(unsafe_code)]

use signalweave_server::{ServerConfig, serve};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    serve(ServerConfig::default()).await?;
    Ok(())
}
