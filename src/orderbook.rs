use crate::proto::{Level, Summary};
use anyhow::{anyhow, Result};
use bigdecimal::{BigDecimal, ToPrimitive, Zero};
use std::collections::BTreeMap;
use std::ops::Bound;
use std::time::SystemTime;

#[derive(Clone, Copy)]
pub enum Side {
    Bid,
    Ask,
}

fn get_unixtime() -> u128 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis()
}

#[derive(Debug, PartialEq, Clone)]
pub struct Orderbook {
    pub(crate) name: String,
    pub(crate) bid: BTreeMap<BigDecimal, BigDecimal>,
    pub(crate) ask: BTreeMap<BigDecimal, BigDecimal>,
    pub(crate) volume: BigDecimal,
    pub(crate) last_price: BigDecimal,
    pub(crate) timestamp: u128,
}

impl Orderbook {
    pub fn clear(&mut self) {
        self.bid.clear();
        self.ask.clear();
    }
    pub fn insert(&mut self, side: Side, price: BigDecimal, volume: BigDecimal) {
        match side {
            Side::Bid => {
                self.bid.remove(&price);
                if !volume.is_zero() {
                    self.bid.insert(price, volume);
                }
            }
            Side::Ask => {
                self.ask.remove(&price);
                if !volume.is_zero() {
                    self.ask.insert(price, volume);
                }
            }
        };
    }
    pub fn new(name: &str) -> Orderbook {
        Orderbook {
            name: name.to_string(),
            bid: BTreeMap::new(),
            ask: BTreeMap::new(),
            timestamp: get_unixtime(),
            last_price: BigDecimal::zero(),
            volume: BigDecimal::zero(),
        }
    }
    // used to trim bid/ask to level numbers of price bars
    pub fn trim(&mut self, level: u32) {
        let l = self.bid.len();
        for _ in (level as usize)..l {
            self.bid.pop_first();
        }
        let l = self.ask.len();
        for _ in (level as usize)..l {
            self.ask.pop_last();
        }
    }
}

// AggregatedOrderbook works like this:
// new() -> merge(ob1) -> merge(ob2) -> ... -> merge(obN) -> finalize(max_level)
// max_level here is used to limit the depth of orderbook to reach in this call
#[derive(Debug)]
pub struct AggregatedOrderbook {
    pub spread: f64,
    pub bid: BTreeMap<BigDecimal, Vec<(String, BigDecimal)>>,
    pub ask: BTreeMap<BigDecimal, Vec<(String, BigDecimal)>>,
}

impl AggregatedOrderbook {
    // merge the content from one orderbook
    pub fn merge(&mut self, orderbook: &Orderbook) {
        let name = &orderbook.name;
        for (price, volume) in orderbook.bid.iter() {
            self.bid
                .entry(price.clone())
                .and_modify(|e| e.push((name.clone(), volume.clone())))
                .or_insert_with(|| vec![(name.clone(), volume.clone())]);
        }
        for (price, volume) in orderbook.ask.iter() {
            self.ask
                .entry(price.clone())
                .and_modify(|e| e.push((name.clone(), volume.clone())))
                .or_insert_with(|| vec![(name.clone(), volume.clone())]);
        }
        self.spread = 0.0;
    }
    pub fn new() -> AggregatedOrderbook {
        AggregatedOrderbook {
            spread: std::f64::NAN,
            bid: BTreeMap::new(),
            ask: BTreeMap::new(),
        }
    }
    // calculate the spread, output the stored price and volume data to grpc's Summary
    pub fn finalize(&mut self, level: u32) -> Result<Summary> {
        let mut cursor = self.bid.upper_bound(Bound::Unbounded);
        let mut counter = 0;
        let mut bids = vec![];
        'bid_outer: for _ in 0..level {
            if let Some((price, v)) = cursor.key_value() {
                for (exchange, volume) in v.iter() {
                    counter += 1;
                    bids.push(Level {
                        exchange: exchange.clone(),
                        price: price
                            .to_f64()
                            .ok_or_else(|| anyhow!("price conversion error: {:?}", price))?,
                        amount: volume
                            .to_f64()
                            .ok_or_else(|| anyhow!("volume conversion error: {:?}", volume))?,
                    });
                    if counter == 10 {
                        break 'bid_outer;
                    }
                }
                // notice move_prev is to move to the previous element in tree,
                // not the order of upper bound or lower bound.
                if cursor.peek_prev().is_some() {
                    cursor.move_prev();
                } else {
                    break;
                }
            } else {
                break;
            }
        }
        let mut cursor = self.ask.lower_bound(Bound::Unbounded);
        let mut counter = 0;
        let mut asks = vec![];
        'ask_outer: for _ in 0..level {
            if let Some((price, v)) = cursor.key_value() {
                for (exchange, volume) in v.iter() {
                    counter += 1;
                    asks.push(Level {
                        exchange: exchange.clone(),
                        price: price
                            .to_f64()
                            .ok_or_else(|| anyhow!("price conversion error: {:?}", price))?,
                        amount: volume
                            .to_f64()
                            .ok_or_else(|| anyhow!("volume conversion error: {:?}", volume))?,
                    });
                    if counter == 10 {
                        break 'ask_outer;
                    }
                }
                if cursor.peek_next().is_some() {
                    cursor.move_next();
                } else {
                    break;
                }
            } else {
                break;
            }
        }
        let best_bid = bids.first();
        let best_ask = asks.first();
        let spread = match (best_bid, best_ask) {
            (Some(v), Some(w)) => w.price - v.price,
            _ => 0.0,
        };
        Ok(Summary { spread, bids, asks })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_orderbook_trim() {
        let default_quantity: BigDecimal = BigDecimal::from_str("10").unwrap();
        let mut ob = Orderbook::new("");
        ob.insert(
            Side::Ask,
            BigDecimal::from_str("1").unwrap(),
            default_quantity.clone(),
        );
        ob.insert(
            Side::Ask,
            BigDecimal::from_str("2").unwrap(),
            default_quantity.clone(),
        );
        ob.trim(1);
        assert_eq!(ob.bid.len(), 0);
        assert_eq!(ob.ask.len(), 1);
        let one = BigDecimal::from_str("1").unwrap();
        assert_eq!(ob.ask.first_key_value(), Some((&one, &default_quantity)));
    }
    #[test]
    fn test_agg_merge() {
        let default_quantity: BigDecimal = BigDecimal::from_str("10").unwrap();
        let mut ob1 = Orderbook::new("A");
        ob1.insert(
            Side::Ask,
            BigDecimal::from_str("1").unwrap(),
            default_quantity.clone(),
        );
        ob1.insert(
            Side::Ask,
            BigDecimal::from_str("2").unwrap(),
            default_quantity.clone(),
        );
        let mut ob2 = Orderbook::new("B");
        ob2.insert(
            Side::Ask,
            BigDecimal::from_str("1").unwrap(),
            default_quantity.clone(),
        );
        ob2.insert(
            Side::Ask,
            BigDecimal::from_str("3").unwrap(),
            default_quantity.clone(),
        );
        let mut agg = AggregatedOrderbook::new();
        agg.merge(&ob1);
        agg.merge(&ob2);
        let summary = agg.finalize(4).unwrap();
        assert_eq!(summary.spread, 0.0);
        assert_eq!(
            summary.asks,
            vec![
                Level {
                    exchange: "A".to_string(),
                    price: 1.,
                    amount: 10.
                },
                Level {
                    exchange: "B".to_string(),
                    price: 1.,
                    amount: 10.
                },
                Level {
                    exchange: "A".to_string(),
                    price: 2.,
                    amount: 10.
                },
                Level {
                    exchange: "B".to_string(),
                    price: 3.,
                    amount: 10.
                },
            ]
        );
        assert_eq!(summary.bids.len(), 0);
    }
}
