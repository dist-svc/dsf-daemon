

use std::net::{SocketAddr, IpAddr, Ipv4Addr};
use std::time::Duration;

use structopt::StructOpt;

use futures::prelude::*;
use futures::select;

use tracing::{span, Level};

use bytes::Bytes;

use dsf_core::types::Id;
use dsf_core::service::{Service, ServiceBuilder};
use dsf_rpc::{Request as RpcRequest, Response as RpcResponse, ResponseKind as RpcResponseKind};

use kad::{Config as DhtConfig};

use crate::error::Error;
use crate::io::*;
use crate::store::*;
use crate::daemon::*;

use crate::daemon::{Options as DaemonOptions};

pub struct Engine {
    dsf: Dsf<WireConnector>,

    net: Net,
    unix: Unix,
    
    wire: Wire,

    store: Store,
}

pub const DEFAULT_UNIX_SOCKET: &str = "/tmp/dsf.sock";
pub const DEFAULT_DATABASE: &str = "/tmp/dsf.db";
pub const DEFAULT_DATABASE_DIR: &str = "/tmp/dsf/";
pub const DEFAULT_SERVICE: &str = "/tmp/dsf.svc";

#[derive(StructOpt, Builder, Debug, Clone, PartialEq)]
#[builder(default)]
pub struct Options {
    #[structopt(short = "a", long = "bind-address", default_value = "0.0.0.0:10100")]
    /// Interface(s) to bind DSF daemon
    /// These may be reconfigured at runtime
    pub bind_addresses: Vec<SocketAddr>,

    #[structopt(long = "database-file", default_value = "/var/dsf/dsf.db", env="DSF_DB")]
    /// Database file for storage by the daemon
    pub database: String,

    #[structopt(long = "service-file", default_value = "/var/dsf/dsf.svc", env="DSF_SVC")]
    /// Service file for reading / writing peer service information
    pub service_file: String,

    #[structopt(short = "s", long = "daemon-socket", default_value = "/tmp/dsf.sock", env="DSF_SOCK")]
    /// Unix socket for communication with the daemon
    pub daemon_socket: String,

    #[structopt(flatten)]
    pub daemon_options: DaemonOptions,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            bind_addresses: vec![SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 10100)],
            daemon_socket: DEFAULT_UNIX_SOCKET.to_string(),
            database: DEFAULT_DATABASE.to_string(),
            service_file: DEFAULT_SERVICE.to_string(),
            daemon_options: DaemonOptions{
                database_dir: DEFAULT_DATABASE_DIR.to_string(),
                dht: DhtConfig::default(),
            }
        }
    }
}

impl Options {
    /// Helper constructor to run multiple instances alongside each other
    pub fn with_suffix(&self, suffix: usize) -> Self {
        Self {
            bind_addresses: vec![SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 10100 + suffix as u16)],
            daemon_socket: format!("{}.{}", self.daemon_socket, suffix),
            database: format!("{}.{}", self.database, suffix),
            service_file: format!("{}.{}", self.service_file, suffix),
            daemon_options: DaemonOptions {
                database_dir: format!("{}.{}", self.daemon_options.database_dir, suffix),
                dht: self.daemon_options.dht.clone(),
            }
        }
    }
}


impl Engine {
    /// Create a new daemon instance
    pub async fn new(options: Options) -> Result<Self, Error> {
        // Create or load service
        let s = ServiceBuilder::default().peer().build().unwrap();

        let span = span!(Level::DEBUG, "engine", "{}", s.id());
        let _enter = span.enter();

        info!("Creating new engine");

        // TODO: persist storage here

        // Create new network connector
        debug!("Creating network connector on addresses: {:?}", options.bind_addresses);
        let mut net = Net::new();
        for addr in &options.bind_addresses {
            net.bind(NetKind::Udp, *addr).await?;
        }

        // Create new unix socket connector
        let mut unix = Unix::new(&options.daemon_socket).await?;

        // Create new local data store
        let mut store = Store::new(&options.database)?;
        
        // Create new wire adaptor
        let mut wire = Wire::new(s.private_key().unwrap());

        // Create new DSF instance
        let mut dsf = Dsf::new(options.daemon_options, s, wire.connector())?;

        debug!("Engine created!");

        Ok(Self{dsf, net, unix, store, wire})
    }

    pub fn id(&self) -> Id {
        self.dsf.id()
    }

    /// Run a daemon instance
    /// 
    /// This blocks forever
    /// TODO: catch exit signal..?
    pub async fn run(&mut self) -> Result<(), Error> {
        let span = span!(Level::DEBUG, "engine", "{}", self.dsf.id());
        let _enter = span.enter();

        loop {

            select!{
                // Incoming network messages
                net_rx = self.net.next().fuse() => {
                    if let Some(m) = net_rx {
                        self.handle_net(m).await?;
                    }
                },
                // Outgoing network messages
                net_tx = self.wire.next().fuse() => {
                    if let Some(m) = net_tx {
                        self.net.send(m).await?;
                    }
                }
                // Incoming RPC messages, response is inline
                rpc_rx = self.unix.next().fuse() => {
                    if let Some(m) = rpc_rx {
                        self.handle_rpc(m).await?;
                    }
                },
                // TODO: periodic update

            }
            
        }
    }

    async fn handle_net(&mut self, msg: NetMessage) -> Result<(), Error> {
        let address = msg.address.clone();

        // Pass through wire module
        // This will route responses internally and return requests
        if let Some(req) = self.wire.handle(msg).await? {
            // Handle the request
            let resp = self.dsf.handle(address, req)?;
            // Send the response
            self.wire.respond(address, resp).await?;
        }

        Ok(())
    }

    async fn handle_rpc(&mut self, msg: UnixMessage) -> Result<(), Error> {
        // Parse out message
        let req: RpcRequest = serde_json::from_slice(&msg.data).unwrap();

        let span = span!(Level::TRACE, "rpc", id=req.req_id());
        let _enter = span.enter();
        
        // Handle RPC request
        let resp_kind = self.dsf.exec(req.kind()).await?;

        // Generate response
        let resp = RpcResponse::new(req.req_id(), resp_kind);

        // Encode response
        let enc = serde_json::to_vec(&resp).unwrap();

        // Send response via relevant unix connection
        let msg = UnixMessage {
            connection_id: msg.connection_id,
            data: Bytes::from(enc),
        };
        self.unix.send(msg).await?;

        Ok(())
    }
}

