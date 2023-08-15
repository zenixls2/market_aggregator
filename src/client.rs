#![allow(dead_code)]
mod config;
mod proto;
use anyhow::{anyhow, Result};
use clap::Parser;
use config::Config;
use futures_util::StreamExt;
use proto::Empty;
use proto::OrderbookAggregatorClient;

#[tokio::main]
async fn main() -> Result<()> {
    let mut config = Config::parse();
    println!("loading from {}", config.config_path);
    config.load()?;
    let server_addr = config
        .inner
        .server_addr
        .unwrap_or_else(|| "127.0.0.1".to_string());
    let connect_addr = format!("http://{}:{}", server_addr, config.inner.server_port);
    let mut client = OrderbookAggregatorClient::connect(connect_addr)
        .await
        .map_err(|e| anyhow!("{:?}", e))?;
    let req = tonic::Request::new(Empty {});
    let mut stream = client
        .book_summary(req)
        .await
        .map_err(|e| anyhow!("{:?}", e))?
        .into_inner();
    while let Some(result) = stream.next().await {
        match result {
            Ok(summary) => {
                println!("{:?}", summary);
            }
            Err(err) => {
                println!("{:?}", err);
                return Err(anyhow!("{:?}", err));
            }
        }
    }
    Ok(())
}
