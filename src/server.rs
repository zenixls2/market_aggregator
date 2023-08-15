#![feature(btree_cursors)]

mod apitree;
mod config;
mod orderbook;
mod proto;
use actix_http::ws::Item::*;
use anyhow::{anyhow, Result};
use awc::ws::Frame::*;
use clap::Parser;
use config::Config;
use futures_util::{pin_mut, select, FutureExt, SinkExt, StreamExt};
use log::{error, info};
use orderbook::{AggregatedOrderbook, Orderbook};
use proto::{AggServer, OrderbookAggregatorServer, Summary};
use std::string::String;
use std::vec::Vec;
use tokio::sync::mpsc::UnboundedSender;
use tonic::{transport::Server, Code, Status};

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

fn setup_logger(
    log_file: Option<String>,
    log_level: config::LogLevel,
) -> Result<(), fern::InitError> {
    let tmp = fern::Dispatch::new()
        .format(|out, message, _record| out.finish(format_args!("{}", message)))
        .level(log_level.to_level_filter())
        .chain(std::io::stdout());
    if let Some(path) = log_file {
        tmp.chain(fern::log_file(path)?).apply()?;
    } else {
        tmp.apply()?;
    }
    Ok(())
}

async fn setup_marketdata(
    binance_pair: &str,
    bitstamp_pair: &str,
    tx: UnboundedSender<Result<Summary, Status>>,
) -> Result<()> {
    let mut binance = Exchange::new("binance");
    let mut bitstamp = Exchange::new("bitstamp");
    binance.connect().await?;
    binance.subscribe(binance_pair).await?;
    bitstamp.connect().await?;
    bitstamp.subscribe(bitstamp_pair).await?;
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
                    let summary = agg.finalize(10).map_err(|e|
                        Status::new(Code::InvalidArgument, format!("{:?}", e))
                    );
                    tx.send(summary).map_err(|e| anyhow!("{:?}", e))?;
                    info!("{:?}", agg);
                }
            },
            x = b2 => {
                if let Some(orderbook) = x? {
                    let mut agg = AggregatedOrderbook::new();
                    bitstamp_cache.replace(orderbook);
                    if let Some(o) = binance_cache.as_ref() { agg.merge(o); }
                    if let Some(o) = bitstamp_cache.as_ref() { agg.merge(o); }
                    let summary = agg.finalize(10).map_err(|e|
                        Status::new(Code::InvalidArgument, format!("{:?}", e)));
                    tx.send(summary).map_err(|e| anyhow!("{:?}", e))?;
                    info!("{:?}", agg);
                }
            },
        };
    }
}

#[actix::main]
async fn main() -> Result<()> {
    let mut config = Config::parse();
    println!("loading from {}", config.config_path);
    config.load()?;
    setup_logger(config.inner.log_path, config.inner.log_level)?;

    let bind_addr = config
        .inner
        .bind_addr
        .unwrap_or_else(|| "0.0.0.0".to_string());
    let addr = format!("{}:{}", bind_addr, config.inner.server_port)
        .parse()
        .map_err(|e| anyhow!("{:?}", e))?;
    let aggserver = AggServer::new();
    let tx = aggserver.tx.clone();
    let handle = tokio::spawn(async move {
        Server::builder()
            .add_service(OrderbookAggregatorServer::new(aggserver))
            .serve(addr)
            .await
            .map_err(|e| anyhow!("{}", e))
            .map(|_| ())
    });
    let market_fut = setup_marketdata(&config.inner.binance_pair, &config.inner.bitstamp_pair, tx);
    let fut_1 = handle.fuse();
    let fut_2 = market_fut.fuse();
    pin_mut!(fut_1, fut_2);

    select! {
        agg_killed = fut_1 => {
            error!("{:?}", agg_killed);
            agg_killed?

        }
        market_result = fut_2 => {
            error!("{:?}", market_result);
            market_result
        }
    }
}
