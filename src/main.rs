mod apitree;
use anyhow::{anyhow, Result};
use futures_util::{SinkExt, StreamExt};
use log::info;
use serde_json::json;
use std::string::String;
use std::vec::Vec;

pub struct Exchange {
    name: String,
    pairs: Vec<String>,
    client: awc::Client,
    level: u32,
    connection: Option<actix_codec::Framed<awc::BoxedSocket, awc::ws::Codec>>,
}

impl Exchange {
    pub fn new(name: &str) -> Exchange {
        let client = awc::Client::builder()
            .max_http_version(awc::http::Version::HTTP_11)
            .finish();

        Exchange {
            name: name.to_string(),
            client,
            pairs: vec![],
            level: 20,
            connection: None,
        }
    }

    pub async fn connect(&mut self) -> Result<()> {
        let url = apitree::WS_APIMAP
            .get(&self.name)
            .ok_or_else(|| anyhow!("Exchange not supported"))?
            .endpoint;
        let (result, conn) = self
            .client
            .ws(url)
            .connect()
            .await
            .map_err(|e| anyhow!("{:?}", e))?;
        self.connection = Some(conn);
        info!("{:?}", result);
        Ok(())
    }

    pub async fn subscribe(&mut self, pair: &str) -> Result<()> {
        if let Some(conn) = &mut self.connection {
            let request = apitree::WS_APIMAP
                .get(&self.name)
                .ok_or_else(|| anyhow!("Exchange not supported"))?
                .subscribe_text(pair, 20)?;
            info!("{:?}", request);
            conn.send(awc::ws::Message::Text(request.into()))
                .await
                .map(|_| ())
                .map_err(|e| anyhow!("{:?}", e))
        } else {
            Err(anyhow!("Not connect yet. Please run connect first."))
        }
    }
}

fn setup_logger() -> Result<(), fern::InitError> {
    fern::Dispatch::new()
        .format(|out, message, _record| out.finish(format_args!("{}", message)))
        .level(log::LevelFilter::Debug)
        .chain(std::io::stdout())
        .chain(fern::log_file("out.log")?)
        .apply()?;
    Ok(())
}

#[actix::main]
async fn main() -> Result<()> {
    setup_logger()?;
    let mut binance = Exchange::new("binance");
    let mut bitstamp = Exchange::new("bitstamp");
    binance.connect().await?;
    binance.subscribe("btcusdt").await?;

    Ok(())
}
