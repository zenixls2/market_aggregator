use crate::orderbook::Orderbook;
use anyhow::{anyhow, Result};
use formatx::formatx;
use phf::phf_map;

type ParseFunc = fn(String) -> Result<Orderbook>;
#[derive(Clone)]
pub struct Api {
    pub endpoint: &'static str,
    // (pair, level)
    pub subscribe_template: &'static str,
    // raw String as input
    pub parse: ParseFunc,
}

impl Api {
    pub fn subscribe_text(&self, pair: &str, level: u32) -> Result<String> {
        formatx!(self.subscribe_template, pair, level).map_err(|e| anyhow!("{:?}", e))
    }
}

fn binance_parser(_raw: String) -> Result<Orderbook> {
    Ok(Orderbook::new("binance"))
}

fn bitstamp_parser(_raw: String) -> Result<Orderbook> {
    Ok(Orderbook::new("bitstamp"))
}

pub static WS_APIMAP: phf::Map<&'static str, Api> = phf_map! {
    "binance" => Api {
        endpoint: "wss://stream.binance.com:9443/ws",
        subscribe_template: r#"{{"id": 1, "method": "SUBSCRIBE", "params": ["{}@depth{}@100ms"]}}"#,
        parse: (binance_parser as ParseFunc),
    },
    "binance_futures" => Api {
        endpoint: "wss://fstream.binance.com:9443/ws",
        subscribe_template: r#"{{"id":1, "method":"SUBSCRIBE", "params": ["{}@depth{}@100ms"]}}"#,
        parse: (binance_parser as ParseFunc),
    },
    "bitstamp" => Api {
        endpoint: "wss://ws.bitstamp.net",
        subscribe_template: r#"{{"event":"bts:subscribe","data":{{"channel":"order_book_{}"}}}}"#,
        parse: (bitstamp_parser as ParseFunc),
    }
};

#[cfg(test)]
mod tests {
    #[test]
    fn test_subscribe_text() {
        let rendered = super::WS_APIMAP
            .get("binance")
            .unwrap()
            .subscribe_text("BTCUSDT", 20)
            .unwrap();
        assert_eq!(
            rendered,
            r#"{"id": 1, "method": "SUBSCRIBE", "params": ["BTCUSDT@depth20@100ms"]}"#
        );
    }
    #[test]
    fn test_parse() {
        let out = (super::WS_APIMAP.get("binance").unwrap().parse)("".to_string()).unwrap();
        assert_eq!(out, super::Orderbook::new("binance"));
    }
}
