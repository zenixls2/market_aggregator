use anyhow::{anyhow, Result};
use formatx::formatx;
use phf::phf_map;

#[derive(Clone)]
pub struct Api {
    pub endpoint: &'static str,
    // (pair, level)
    pub subscribe_template: &'static str,
}

impl Api {
    pub fn subscribe_text(&self, pair: &str, level: u32) -> Result<String> {
        formatx!(self.subscribe_template, pair, level).map_err(|e| anyhow!("{:?}", e))
    }
}

pub static WS_APIMAP: phf::Map<&'static str, Api> = phf_map! {
    "binance" => Api {
        endpoint: "wss://stream.binance.com:9443/ws",
        subscribe_template: r#"{"id":1, "method":"SUBSCRIBE", "params": ["{}@depth{}@100ms"]}"#,
    },
    "binance_futures" => Api {
        endpoint: "wss://fstream.binance.com:9443/ws",
        subscribe_template: r#"{"id":1, "method":"SUBSCRIBE", "params": ["{}@depth{}@100ms"]}"#,
    },
    "bitstamp" => Api {
        endpoint: "wss://ws.bitstamp.net",
        subscribe_template: r#"{"event":"bts:subscribe","data":{"channel":"order_book_{}"}}"#,
    }
};
