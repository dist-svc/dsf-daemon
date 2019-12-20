

use std::net::{SocketAddr, IpAddr, Ipv4Addr};

use structopt::StructOpt;

use crate::error::Error;
use crate::io::*;

pub struct Daemon {
    net: Net,
    unix: Unix,
}

pub const DEFAULT_UNIX_SOCKET: &str = "/tmp/dsf.sock";

#[derive(StructOpt, Builder, Debug, Clone, PartialEq)]
#[builder(default)]
pub struct Options {
    #[structopt(short = "a", long = "bind-address", default_value = "0.0.0.0:10100")]
    /// Interface(s) to bind DSF daemon
    /// These may be reconfigured at runtime
    pub bind_addresses: Vec<SocketAddr>,

    #[structopt(short = "s", long = "daemon-socket", default_value = "/tmp/dsf.sock")]
    /// Unix socket for communication with the daemon
    pub daemon_socket: String,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            bind_addresses: vec![SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 10100)],
            daemon_socket: DEFAULT_UNIX_SOCKET.to_string(),
        }
    }
}

impl Daemon {
    pub async fn new(options: Options) -> Result<Self, Error> {
        // Create new network connector
        let mut net = Net::new();

        for addr in &options.bind_addresses {
            net.bind(NetKind::Udp, *addr).await?;
        }

        let mut unix = Unix::new(&options.daemon_socket).await?;

        Ok(Self{net, unix})
    }

    pub async fn run(&mut self) -> Result<(), Error> {
        loop {

        }
    }
}

