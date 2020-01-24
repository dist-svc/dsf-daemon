
use std::time::Duration;

use structopt::StructOpt;

use dsf_core::prelude::*;
use dsf_core::service::Publisher;

use kad::prelude::*;

use async_std::future::timeout;
use tracing::{Level, span};

use crate::core::peers::{Peer, PeerManager};
use crate::core::services::{ServiceManager};

use crate::io::Connector;
use crate::error::Error;

pub mod dht;
use dht::{Ctx, DhtAdaptor, dht_reducer};

pub mod net;

pub mod dsf;
pub use dsf::*;

//#[cfg(test)]
mod tests;

#[derive(Clone, Debug, PartialEq, StructOpt)]
pub struct Options {
    #[structopt(long = "database-dir", default_value = "/var/dsf/", env="DSF_DB_DIR")]
    /// [Legacy] database directory for storage by the daemon
    pub database_dir: String,

    #[structopt(flatten)]
    pub dht: DhtConfig,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            database_dir: "/var/dsf/".to_string(),
            dht: DhtConfig::default(),
        }
    }
}

