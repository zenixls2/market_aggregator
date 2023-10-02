pub mod restapi;
pub mod wsapi;
use anyhow::{Context as _, Result};

pub fn ws(name: &str) -> Result<&'static wsapi::Api> {
    wsapi::WS_APIMAP
        .get(name)
        .with_context(|| format!("Exchange {} not supported", name))
}

pub fn rest(name: &str) -> Result<&'static restapi::Api> {
    restapi::REST_APIMAP
        .get(name)
        .with_context(|| format!("Exchange {} not supported", name))
}
