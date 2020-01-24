
use structopt::StructOpt;

use kad::prelude::*;

pub mod dht;

pub mod net;

pub mod dsf;
pub use dsf::*;

#[cfg(test)]
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

