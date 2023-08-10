use bigdecimal::BigDecimal;
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

#[derive(Debug)]
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
    pub fn new() -> AggregatedOrderbook {
        AggregatedOrderbook {
            spread: std::f64::NAN,
            bid: BTreeMap::new(),
            ask: BTreeMap::new(),
        }
    }
    pub fn finalize(&mut self, level: u32) {
        // TODO: calculate spread, remove redundant levels, and output protobuf item
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    #[test]
    fn test_orderbook_trim() {
        let mut ob = Orderbook::new("");
        let default_quantity = BigDecimal::from_str("10").unwrap();
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
}
