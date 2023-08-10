mod apitree;
mod orderbook;
use actix_http::ws::Item::*;
use anyhow::{anyhow, Result};
use awc::ws::Frame::*;
use futures_util::{pin_mut, select, FutureExt, SinkExt, StreamExt};
use log::info;
use orderbook::{AggregatedOrderbook, Orderbook};
use std::string::String;
use std::vec::Vec;

pub struct Exchange {
    name: String,
    pairs: Vec<String>,
    client: awc::Client,
    level: u32,
    connection: Option<actix_codec::Framed<awc::BoxedSocket, awc::ws::Codec>>,
    cache: String,
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
            cache: "".to_string(),
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
        self.pairs.push(pair.to_string());
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
    pub async fn next(&mut self) -> Result<Option<Orderbook>> {
        if let Some(result) = self
            .connection
            .as_mut()
            .ok_or_else(|| anyhow!("Not connect yet. Please run connect first"))?
            .next()
            .await
        {
            let raw = match result? {
                Text(msg) => std::str::from_utf8(&msg)?.to_string(),
                Binary(msg) => std::str::from_utf8(&msg)?.to_string(),
                Continuation(item) => match item {
                    FirstText(b) | FirstBinary(b) | Continue(b) => {
                        self.cache += std::str::from_utf8(&b)?;
                        return Ok(None);
                    }
                    Last(b) => {
                        let output = self.cache.clone() + std::str::from_utf8(&b)?;
                        self.cache = "".to_string();
                        output
                    }
                },
                Ping(_) | Pong(_) => return Ok(None),
                Close(_) => return Err(anyhow!("close")),
            };

            let mut parsed = (apitree::WS_APIMAP
                .get(&self.name)
                .ok_or_else(|| anyhow!("Exchange not supported"))?
                .parse)(raw)?;
            parsed.trim(self.level);
            Ok(Some(parsed))
        } else {
            Ok(None)
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
    bitstamp.connect().await?;
    bitstamp.subscribe("btcusd").await?;
    let mut binance_cache: Option<Orderbook> = None;
    let mut bitstamp_cache: Option<Orderbook> = None;
    loop {
        let b1 = binance.next().fuse();
        let b2 = bitstamp.next().fuse();
        pin_mut!(b1, b2);
        select! {
            x = b1 => {
                if let Some(orderbook) = x? {
                    let mut agg = AggregatedOrderbook::new();
                    binance_cache.replace(orderbook);
                    if let Some(o) = bitstamp_cache.as_ref() { agg.merge(o); }
                    if let Some(o) = binance_cache.as_ref() { agg.merge(o); }
                    info!("{:?}", agg);
                }
            },
            x = b2 => {
                if let Some(orderbook) = x? {
                    let mut agg = AggregatedOrderbook::new();
                    bitstamp_cache.replace(orderbook);
                    if let Some(o) = binance_cache.as_ref() { agg.merge(o); }
                    if let Some(o) = bitstamp_cache.as_ref() { agg.merge(o); }
                    info!("{:?}", agg);
                }
            },
        };
    }
}
