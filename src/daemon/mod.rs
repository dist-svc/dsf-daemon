use structopt::StructOpt;

use kad::prelude::*;

pub mod dht;

pub mod net;

pub mod dsf;
pub use dsf::*;

#[cfg(test)]
mod tests;

/// DSF Instance Configuration Options
#[derive(Clone, Debug, PartialEq, StructOpt)]
pub struct Options {
    #[structopt(flatten)]
    pub dht: DhtConfig,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            dht: DhtConfig::default(),
        }
    }
}

/// DSF System Event
/// Used to prompt asynchronous system updates
#[derive(Clone, Debug, PartialEq)]
pub enum Event {

}



