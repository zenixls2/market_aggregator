mod proto;
use anyhow::{anyhow, Result};
use futures_util::StreamExt;
use proto::Empty;
use proto::OrderbookAggregatorClient;

#[tokio::main]
async fn main() -> Result<()> {
    let mut client = OrderbookAggregatorClient::connect("http://127.0.0.1:50051")
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
