use bigdecimal::{BigDecimal, ToPrimitive};
use std::collections::BTreeMap;

#[derive(Clone, Copy)]
pub enum Side {
    Bid,
    Ask,
}

#[derive(Debug, PartialEq)]
pub struct Orderbook {
    pub(self) name: String,
    pub(self) bid: BTreeMap<BigDecimal, BigDecimal>,
    pub(self) ask: BTreeMap<BigDecimal, BigDecimal>,
}

impl Orderbook {
    pub fn clear(&mut self) {
        self.bid.clear();
        self.ask.clear();
    }
    pub fn insert(&mut self, side: Side, price: BigDecimal, volume: BigDecimal) {
        match side {
            Side::Bid => self.bid.insert(price, volume),
            Side::Ask => self.ask.insert(price, volume),
        };
    }
    pub fn new(name: &str) -> Orderbook {
        Orderbook {
            name: name.to_string(),
            bid: BTreeMap::new(),
            ask: BTreeMap::new(),
        }
    }
}

pub struct AggregatedOrderbook {
    pub spread: f64,
    pub bid: BTreeMap<BigDecimal, Vec<(String, BigDecimal)>>,
    pub ask: BTreeMap<BigDecimal, Vec<(String, BigDecimal)>>,
}

impl AggregatedOrderbook {
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
}
