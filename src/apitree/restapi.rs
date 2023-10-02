use crate::orderbook::{Orderbook, Side};
use anyhow::{anyhow, Result};
use bigdecimal::BigDecimal;
use futures_util::future::Future;
use log::info;
use phf::phf_map;
use serde::Deserialize;
use std::pin::Pin;
use std::str::FromStr;

type BoxFuture = Pin<Box<dyn Future<Output = Result<Orderbook>> + Send>>;

pub struct Api {
    pub endpoint: &'static str,
    pub orderbook: fn(String) -> BoxFuture,
}

pub static REST_APIMAP: phf::Map<&'static str, Api> = phf_map! {
    "btcmarkets" => Api {
        endpoint: "https://api.btcmarkets.net",
        orderbook: |s| Box::pin(btcmarkets_orderbook(s)),
    }
};

async fn btcmarkets_orderbook(pair: String) -> Result<Orderbook> {
    return Err(anyhow!("not implemented"));
}
