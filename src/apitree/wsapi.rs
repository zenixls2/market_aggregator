use crate::orderbook::{Orderbook, Side};
use anyhow::bail;
use anyhow::{anyhow, Result};
use bigdecimal::BigDecimal;
use formatx::formatx;
use once_cell::sync::Lazy;
use phf::phf_map;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Mutex;

type ParseFunc = fn(String) -> Result<Option<Orderbook>>;
#[derive(Clone)]
pub struct Api {
    pub endpoint: &'static str,
    // (pair, level)
    pub subscribe_template: &'static [&'static str],
    // raw String as input
    pub parse: ParseFunc,
    // render url with data
    pub render_url: bool,
    // wait second, heartbeat message. None means no need to send heartbeat
    pub heartbeat: Option<(u64, &'static str)>,
    // cleanup function when error happens
    pub clear: fn() -> (),
}

impl Api {
    // utility to render the subscription text
    pub fn subscribe_text(&self, pair: &str, level: u32) -> Result<Vec<String>> {
        let mut result = vec![];
        for template in self.subscribe_template.iter() {
            result.push(formatx!(template.to_string(), pair, level)?);
        }
        Ok(result)
    }
}

static BINANCE: Lazy<Mutex<HashMap<String, Orderbook>>> = Lazy::new(|| Mutex::new(HashMap::new()));

fn binance_parser(raw: String) -> Result<Option<Orderbook>> {
    // TODO: the PartialBookDepth doesn't contain symbol.
    // Use PartialDiff packet to replace it.
    #[derive(Default, Deserialize, Debug)]
    #[serde(rename_all = "camelCase", default)]
    struct PartialBookDepth {
        last_update_id: u64,
        bids: Vec<[String; 2]>,
        asks: Vec<[String; 2]>,
        result: Value,
        id: u64,
    }
    #[derive(Default, Deserialize, Debug)]
    struct Ticker {
        #[serde(rename = "e")]
        event_type: String,
        #[serde(rename = "c")]
        close: String,
        #[serde(rename = "v")]
        volume: String,
        #[serde(rename = "s")]
        symbol: String,
    }
    let result: Value = serde_json::from_str(&raw)?;
    let mut tmp = BINANCE.lock().unwrap();

    // Since PartialBookDepth doesn't contain any key information,
    // Use a dummy one here
    let key: String = "dummy".to_string();
    let ob = if let Some(ob) = tmp.get_mut(&key) {
        ob
    } else {
        tmp.insert(key.clone(), Orderbook::new("binance"));
        tmp.get_mut(&key).unwrap()
    };

    if result["e"].as_str() == Some("24hrTicker") {
        let result: Ticker = serde_json::from_value(result)?;
        ob.last_price = BigDecimal::from_str(&result.close)?;
        ob.volume = BigDecimal::from_str(&result.volume)?;
        return Ok(Some(ob.clone()));
    } else {
        let result: PartialBookDepth = serde_json::from_value(result)?;
        // this is a subscription response
        if result.last_update_id == 0 && result.bids.is_empty() && result.asks.is_empty() {
            return Ok(None);
        }
        if result.result != Value::Null {
            bail!("result not empty");
        }
        ob.ask.clear();
        ob.bid.clear();

        for [price_str, quantity_str] in result.bids {
            let price = BigDecimal::from_str(&price_str)?;
            let quantity = BigDecimal::from_str(&quantity_str)?;
            ob.insert(Side::Bid, price, quantity);
        }
        for [price_str, quantity_str] in result.asks {
            let price = BigDecimal::from_str(&price_str)?;
            let quantity = BigDecimal::from_str(&quantity_str)?;
            ob.insert(Side::Ask, price, quantity);
        }
        Ok(Some(ob.clone()))
    }
}

fn bitstamp_parser(raw: String) -> Result<Option<Orderbook>> {
    #[derive(Deserialize, Debug)]
    struct LiveDetailOrderbook {
        bids: Vec<[String; 2]>,
        asks: Vec<[String; 2]>,
        #[serde(rename = "timestamp")]
        _timestamp: String,
        #[serde(rename = "microtimestamp")]
        _microtimestamp: String,
    }
    #[derive(Deserialize, Debug)]
    struct WsEvent {
        data: Value,
        event: String,
        channel: String,
    }
    let result: WsEvent = serde_json::from_str(&raw).map_err(|e| anyhow!("{:?}", e))?;
    if result.event != "data" {
        // reconnect
        return Ok(None);
    }
    if !result.channel.starts_with("order_book_") {
        bail!("non-orderbook signal passed it");
    }
    // LiveDetailOrderbook is the only subscription type
    // others should be categorized as error
    let result: LiveDetailOrderbook = serde_json::from_value(result.data)?;
    let mut ob = Orderbook::new("bitstamp");
    for [price_str, quantity_str] in result.bids {
        let price = BigDecimal::from_str(&price_str)?;
        let quantity = BigDecimal::from_str(&quantity_str)?;
        ob.insert(Side::Bid, price, quantity);
    }
    for [price_str, quantity_str] in result.asks {
        let price = BigDecimal::from_str(&price_str)?;
        let quantity = BigDecimal::from_str(&quantity_str)?;
        ob.insert(Side::Ask, price, quantity);
    }
    Ok(Some(ob))
}

static KRAKEN: Lazy<Mutex<HashMap<String, Orderbook>>> = Lazy::new(|| Mutex::new(HashMap::new()));

fn kraken_clear() {
    let mut tmp = KRAKEN.lock().unwrap();
    tmp.clear();
}

fn kraken_parser(raw: String) -> Result<Option<Orderbook>> {
    if raw.as_bytes()[0] as char == '{' {
        return Ok(None);
    }
    let result: Vec<Value> = serde_json::from_str(&raw).map_err(|e| anyhow!("{:?}", e))?;
    let channel_name: String =
        serde_json::from_value(result[2].clone()).map_err(|e| anyhow!("{:?}", e))?;
    let pair: String = serde_json::from_value(result[3].clone()).map_err(|e| anyhow!("{:?}", e))?;
    let key = &pair;
    if channel_name.starts_with("book") {
        #[derive(Deserialize, Debug)]
        struct Data {
            #[serde(default)]
            r#as: Vec<[String; 3]>,
            #[serde(default)]
            bs: Vec<[String; 3]>,
            #[serde(default)]
            a: Vec<Vec<String>>,
            #[serde(default)]
            b: Vec<Vec<String>>,
        }
        // channel_id: u64
        // data: object
        // - as: Vec<[String; 3]>
        // - bs: Vec<[String; 3]>
        // channel_name: String
        // pair: String
        let data: Data =
            serde_json::from_value(result[1].clone()).map_err(|e| anyhow!("{:?}", e))?;
        let mut tmp = KRAKEN.lock().unwrap();
        let ob = if let Some(ob) = tmp.get_mut(key) {
            ob
        } else {
            tmp.insert(key.clone(), Orderbook::new("kraken"));
            tmp.get_mut(key).unwrap()
        };
        if data.bs.len() > 0 || data.r#as.len() > 0 {
            ob.bid.clear();
            ob.ask.clear();
        }
        for [price_str, quantity_str, _timestamp] in data.bs {
            let price = BigDecimal::from_str(&price_str).map_err(|e| anyhow!("{:?}", e))?;
            let quantity = BigDecimal::from_str(&quantity_str).map_err(|e| anyhow!("{:?}", e))?;
            ob.insert(Side::Bid, price, quantity);
        }
        for v in data.b {
            let price_str: &str = &v[0];
            let quantity_str: &str = &v[1];
            let price = BigDecimal::from_str(price_str).map_err(|e| anyhow!("{:?}", e))?;
            let quantity = BigDecimal::from_str(quantity_str).map_err(|e| anyhow!("{:?}", e))?;
            ob.insert(Side::Bid, price, quantity);
        }
        for [price_str, quantity_str, _timestamp] in data.r#as {
            let price = BigDecimal::from_str(&price_str).map_err(|e| anyhow!("{:?}", e))?;
            let quantity = BigDecimal::from_str(&quantity_str).map_err(|e| anyhow!("{:?}", e))?;
            ob.insert(Side::Ask, price, quantity);
        }
        for v in data.a {
            let price_str: &str = &v[0];
            let quantity_str: &str = &v[1];
            let price = BigDecimal::from_str(price_str).map_err(|e| anyhow!("{:?}", e))?;
            let quantity = BigDecimal::from_str(quantity_str).map_err(|e| anyhow!("{:?}", e))?;
            ob.insert(Side::Ask, price, quantity);
        }
        // we're subscribing to book-25, so do cleanup here
        // the exchange/mod.rs side could only get the cloned item,
        // so the orderbook didn't explicitly trim the orderbook.
        ob.trim(25);
        return Ok(Some(ob.clone()));
    } else if channel_name == "ticker".to_string() {
        // data:
        // - a: best ask [3]
        // - b: best bid [3]
        // - c: close [2]
        // - v: volume [2] (today, last24hr)
        #[derive(Deserialize, Debug)]
        struct Data {
            #[serde(default)]
            c: [String; 2],
            #[serde(default)]
            v: [String; 2],
        }
        let data: Data =
            serde_json::from_value(result[1].clone()).map_err(|e| anyhow!("{:?}", e))?;
        let mut tmp = KRAKEN.lock().unwrap();
        let ob = if let Some(ob) = tmp.get_mut(key) {
            ob
        } else {
            tmp.insert(key.clone(), Orderbook::new("kraken"));
            tmp.get_mut(key).unwrap()
        };
        ob.volume = BigDecimal::from_str(&data.v[1]).map_err(|e| anyhow!("{:?}", e))?;
        ob.last_price = BigDecimal::from_str(&data.c[0]).map_err(|e| anyhow!("{:?}", e))?;
        return Ok(Some(ob.clone()));
    }
    Ok(None)
}

// The API Map compile-time static map that handles depth orderbook subscription and parsing
pub static WS_APIMAP: phf::Map<&'static str, Api> = phf_map! {
    "binance" => Api {
        endpoint: "wss://stream.binance.com:9443/ws",
        subscribe_template: &[
            r#"{{"id": 1, "method": "SUBSCRIBE", "params": ["{}@depth{}@100ms"]}}"#,
            r#"{{"id": 2, "method": "SUBSCRIBE", "params": ["{}@ticker"]}}"#,
        ],
        parse: (binance_parser as ParseFunc),
        render_url: false,
        heartbeat: None,
        clear: || {},
    },
    "binance_futures" => Api {
        endpoint: "wss://fstream.binance.com:9443/ws",
        subscribe_template: &[r#"{{"id":1, "method":"SUBSCRIBE", "params": ["{}@depth{}@100ms"]}}"#],
        parse: (binance_parser as ParseFunc),
        render_url: false,
        heartbeat: None,
        clear: || {},
    },
    "bitstamp" => Api {
        endpoint: "wss://ws.bitstamp.net",
        subscribe_template: &[r#"{{"event":"bts:subscribe","data":{{"channel":"order_book_{}"}}}}"#],
        parse: (bitstamp_parser as ParseFunc),
        render_url: false,
        heartbeat: None,
        clear: || {},
    },
    "kraken" => Api {
        endpoint: "wss://ws.kraken.com",
        subscribe_template: &[
            r#"{{"event":"subscribe","pair":["{}"], "subscription": {{"name":"book","depth":25}}}}"#,
            r#"{{"event":"subscribe","pair":["{}"], "subscription": {{"name":"ticker"}}}}"#],
        parse: (kraken_parser as ParseFunc),
        render_url: false,
        heartbeat: None,
        clear: kraken_clear,
    }
};

#[cfg(test)]
mod tests {
    use bigdecimal::BigDecimal;
    use std::str::FromStr;
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
    fn test_binance_parse() {
        // subscription response, return empty Orderbook
        let out = (super::WS_APIMAP.get("binance").unwrap().parse)(
            r#"{"id": 1, "result": null}"#.to_string(),
        )
        .unwrap();
        assert_eq!(out, super::Orderbook::new("binance"));

        // normal event
        let out = (super::WS_APIMAP.get("binance").unwrap().parse)(
            r#"{"lastUpdateId": 160, "bids":[["0.01", "0.2"]], "asks": []}"#.to_string(),
        )
        .unwrap();
        let mut ob = super::Orderbook::new("binance");
        ob.insert(
            super::Side::Bid,
            BigDecimal::from_str("0.01").unwrap(),
            BigDecimal::from_str("0.2").unwrap(),
        );
        if let Some(o) = out.as_ref() {
            ob.timestamp = o.timestamp;
        }
        assert_eq!(out, Some(ob));
    }
    #[test]
    fn test_bitstamp_parse() {
        // subscription response
        let out = (super::WS_APIMAP.get("bitstamp").unwrap().parse)(
            r#"{"event": "bts:subscription_succeeded", "channel": "order_book_btcusd", "data": {}}"#
                .to_string(),
        )
        .unwrap();
        assert_eq!(out, super::Orderbook::new("bitstamp"));

        // normal event
        let out = (super::WS_APIMAP.get("bitstamp").unwrap().parse)(
            r#"{"data":{
                "timestamp":"1691595437",
                "microtimestamp":"1691595437334962",
                "bids":[],
                "asks":[["29737","0.67548438"],["29738","0.67255217"]]
            },"channel":"order_book_btcusd","event":"data"}"#
                .to_string(),
        )
        .unwrap();
        let mut ob = super::Orderbook::new("bitstamp");
        ob.insert(
            super::Side::Ask,
            BigDecimal::from_str("29737").unwrap(),
            BigDecimal::from_str("0.67548438").unwrap(),
        );
        ob.insert(
            super::Side::Ask,
            BigDecimal::from_str("29738").unwrap(),
            BigDecimal::from_str("0.67255217").unwrap(),
        );
        if let Some(o) = out.as_ref {
            ob.timestamp = o.timestamp;
        }
        assert_eq!(out, Some(ob));
    }
}
