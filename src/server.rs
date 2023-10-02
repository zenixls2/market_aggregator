#![feature(btree_cursors)]

mod apitree;
mod config;
mod orderbook;
mod proto;
use crate::config::Config;
use crate::config::ExchangeSetting;
use anyhow::{anyhow, bail, Context, Result};
use clap::Parser;
use formatx::formatx;
use futures_util::stream::SplitStream;
use futures_util::{pin_mut, FutureExt, SinkExt, StreamExt};
use log::{debug, error, info};
use orderbook::{AggregatedOrderbook, Orderbook};
use proto::{AggServer, OrderbookAggregatorServer, Summary};
use std::collections::HashMap;
use std::string::String;
use std::vec::Vec;
use tokio::net::TcpStream;
use tokio::select;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tokio::time::{self, sleep, Duration};
use tokio_tungstenite::{
    connect_async, tungstenite::protocol::Message, MaybeTlsStream, WebSocketStream,
};
use tonic::{transport::Server, Code, Status};
use Message::*;

pub struct Exchange {
    name: String,
    level: u32,
    rx: Option<SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>>,
    utx: Option<UnboundedSender<Message>>,
    ws_api: bool,
    pairs: Vec<String>,
    wait_secs: u64,
}

impl Exchange {
    pub fn new(name: &str) -> Exchange {
        Exchange {
            name: name.to_string(),
            level: 10,
            ws_api: true,
            pairs: vec![],
            wait_secs: 0,
            rx: None,
            utx: None,
        }
    }

    pub async fn connect(&mut self, pairs: Vec<ExchangeSetting>) -> Result<()> {
        self.pairs = pairs.iter().map(|e| e.pair.clone()).collect();
        let default_setup = pairs
            .get(0)
            .with_context(|| format!("should have at least one pair setting"))?;
        self.wait_secs = if default_setup.wait_secs > 0 {
            default_setup.wait_secs
        } else {
            1_u64
        };
        self.ws_api = default_setup.ws_api;
        if !self.ws_api {
            return Ok(());
        }
        info!("start connecting {}", self.name);

        let api = apitree::ws(&self.name)?;
        let mut url = api.endpoint.to_string();
        if api.render_url {
            let p = self.pairs.join(",");
            info!("render Url: {}", p);
            url = formatx!(url, p).map_err(|e| anyhow!("{}", e))?;
        }
        info!("{}", url);

        let (ws_stream, result) = connect_async(url).await?;
        info!("{:?}", result);
        let (mut tx, rx) = ws_stream.split();
        self.rx = Some(rx);

        let (utx, mut urx) = unbounded_channel();
        let utx_hb = utx.clone();
        self.utx = Some(utx);

        tokio::spawn(async move {
            while let Some(msg) = urx.recv().await {
                if let Err(e) = tx.send(msg).await {
                    error!("{}", e);
                }
            }
        });

        let api = apitree::ws(&self.name)?;
        let (wait_secs, msg) = api.heartbeat.unwrap_or((0, ""));
        if wait_secs > 0 {
            let mut interval = time::interval(Duration::from_secs(wait_secs));
            let name = self.name.clone();
            tokio::spawn(async move {
                // sending heartbeats
                loop {
                    interval.tick().await;
                    info!("send heartbeat to {}", name);
                    if let Err(e) = utx_hb.send(Message::Binary(msg.into())) {
                        error!("heartbeat: {}", e);
                        break;
                    }
                }
            });
        }

        if !api.render_url {
            if let Some(utx) = self.utx.clone() {
                for pair in self.pairs.iter() {
                    let requests = api.subscribe_text(pair, 20)?;
                    info!("{:?}", requests);
                    for request in requests {
                        utx.send(Message::Text(request))?;
                    }
                }
            }
        }
        Ok(())
    }

    pub fn clear(&self) -> Result<()> {
        let api = apitree::ws(&self.name)?;
        (api.clear)();
        Ok(())
    }

    pub async fn next(&mut self) -> Result<Option<Orderbook>> {
        if !self.ws_api {
            let level = self.level;
            sleep(Duration::from_secs(self.wait_secs)).await;
            // only able to handle one pair
            if let Some(pair) = self.pairs.first() {
                return (apitree::rest(&self.name)?.orderbook)(pair.clone())
                    .await
                    .map(move |mut e| {
                        e.trim(level);
                        Some(e)
                    });
            } else {
                bail!("no pair assigned to the exchange");
            }
        }
        let result = &mut self
            .rx
            .as_mut()
            .with_context(|| "Not connect yet. Please run connect first")?;
        loop {
            if let Some(result) = result.next().await {
                let raw = match result? {
                    Text(msg) => msg,
                    Binary(msg) => std::str::from_utf8(&msg)?.to_string(),
                    Ping(_) | Pong(_) => return Ok(None),
                    Close(_) => {
                        error!("stream gets closed: {}", self.name);
                        return Err(anyhow!("close {}", self.name));
                    }
                    Frame(_) => {
                        unreachable!();
                    }
                };
                debug!("{}: {}", self.name, raw);

                if let Some(mut e) = (apitree::ws(&self.name)?.parse)(raw)? {
                    e.trim(self.level);
                    return Ok(Some(e));
                }
                // skip none
            } else {
                return Ok(None);
            }
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

async fn executor(
    exchange: String,
    pairs: Vec<ExchangeSetting>,
    tx: UnboundedSender<(String, Orderbook)>,
) -> Result<()> {
    let mut client = Exchange::new(&exchange);
    info!("start executor {}", exchange);
    client.connect(pairs.clone()).await?;
    info!("connect {}", exchange);
    // currently we only allow single subscription
    loop {
        match client.next().await {
            Ok(Some(orderbook)) => {
                tx.send((exchange.clone(), orderbook))?;
                continue;
            }
            Ok(None) => {
                error!("shutddown {}", exchange);
            }
            Err(e) => {
                error!("{}, reconnect...", e);
            }
        }
        if let Err(e) = client.clear() {
            error!("{}, clear error", e);
        }
        client = Exchange::new(&exchange);
        if let Err(e) = client.connect(pairs.clone()).await {
            error!("{} {} connect error", e, exchange);
        }
        error!("connect {}", exchange);
    }
}

async fn setup_marketdata(
    exchange_pairs: HashMap<String, Vec<ExchangeSetting>>,
    tx: UnboundedSender<Result<Summary, Status>>,
) -> Result<()> {
    let (itx, mut irx) = unbounded_channel::<(String, Orderbook)>();
    let mut exchange_cache = HashMap::<String, Orderbook>::new();
    let mut threads = vec![];
    for (exchange, settings) in exchange_pairs {
        info!("loading {}: {:?}", exchange, settings);
        let ltx = itx.clone();
        threads.push(tokio::spawn(async move {
            if let Err(e) = executor(exchange.clone(), settings.clone(), ltx).await {
                error!("exchange client spawn error: {}", e);
            }
        }));
    }
    while let Some((exchange, orderbook)) = irx.recv().await {
        let mut agg = AggregatedOrderbook::new();
        exchange_cache.remove(&exchange);
        exchange_cache.insert(exchange.clone(), orderbook);
        for (_key, ob) in exchange_cache.iter() {
            agg.merge(ob);
        }
        let summary = agg
            .finalize(10)
            .map_err(|e| Status::new(Code::InvalidArgument, format!("{:?}", e)));
        if let Err(e) = tx.send(summary) {
            error!("{:?}", e);
        }
    }
    threads.clear();
    Ok(())
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
    let server_port = config.inner.server_port;

    let aggserver = AggServer::new();
    let tx = aggserver.tx.clone();
    let handle = tokio::spawn(async move {
        Server::builder()
            .add_service(OrderbookAggregatorServer::new(aggserver))
            .serve(format!("{}:{}", bind_addr, server_port).parse()?)
            .await
            .map_err(|e| anyhow!("{}", e))
            .map(|_| ())
    });
    let market_fut = setup_marketdata(config.inner.exchange_pair_map, tx);
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
