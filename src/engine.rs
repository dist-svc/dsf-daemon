

use std::net::{SocketAddr, IpAddr, Ipv4Addr};
use std::sync::{Arc, Mutex};

use structopt::StructOpt;

use async_std::task;

use futures::prelude::*;
use futures::select;

use tracing::{span, Level};

use bytes::Bytes;

use dsf_core::types::Id;
use dsf_core::service::{ServiceBuilder};
use dsf_core::net::{Message as DsfMessage};

use dsf_rpc::{Request as RpcRequest, Response as RpcResponse};

use kad::{Config as DhtConfig};

use crate::error::Error;
use crate::io::*;
use crate::store::*;
use crate::daemon::*;


use crate::daemon::{Options as DaemonOptions};

pub struct Engine {
    dsf: Dsf<WireConnector>,

    unix: Unix,
    wire: Wire,
    net: Net,

}

pub const DEFAULT_UNIX_SOCKET: &str = "/tmp/dsf.sock";
pub const DEFAULT_DATABASE_FILE: &str = "/tmp/dsf.db";
pub const DEFAULT_SERVICE: &str = "/tmp/dsf.svc";

#[derive(StructOpt, Builder, Debug, Clone, PartialEq)]
#[builder(default)]
pub struct Options {
    #[structopt(short = "a", long = "bind-address", default_value = "0.0.0.0:10100")]
    /// Interface(s) to bind DSF daemon
    /// These may be reconfigured at runtime
    pub bind_addresses: Vec<SocketAddr>,

    #[structopt(long = "service-file", default_value = "/var/dsf/dsf.svc", env="DSF_SVC")]
    /// Service file for reading / writing peer service information
    pub service_file: String,

    #[structopt(long = "database-file", default_value = "/var/dsf/dsf.db", env="DSF_DB_FILE")]
    /// Database file for storage by the daemon
    pub database_file: String,

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
            service_file: DEFAULT_SERVICE.to_string(),
            database_file: DEFAULT_DATABASE_FILE.to_string(),
            daemon_options: DaemonOptions {
                
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
            service_file: format!("{}.{}", self.service_file, suffix),
            database_file: format!("{}.{}", self.database_file, suffix),
            daemon_options: DaemonOptions {
                dht: self.daemon_options.dht.clone(),
            }
        }
    }
}


impl Engine {
    /// Create a new daemon instance
    pub async fn new(options: Options) -> Result<Self, Error> {
        // Create or load service
        let service = ServiceBuilder::default().peer().build().unwrap();

        let span = span!(Level::DEBUG, "engine", "{}", service.id());
        let _enter = span.enter();

        info!("Creating new engine");

        // Create new network connector
        debug!("Creating network connector on addresses: {:?}", options.bind_addresses);
        let mut net = Net::new();
        for addr in &options.bind_addresses {
            net.bind(NetKind::Udp, *addr).await?;
        }

        // Create new unix socket connector
        let unix = Unix::new(&options.daemon_socket).await?;

        // Create new wire adaptor
        let wire = Wire::new(service.private_key().unwrap());

        // Create new local data store
        let store = Arc::new(Mutex::new(Store::new(&options.database_file)?));

        // Create new DSF instance
        let dsf = Dsf::new(options.daemon_options, service, store, wire.connector())?;

        debug!("Engine created!");

        Ok(Self{dsf, wire, net, unix})
    }

    pub fn id(&self) -> Id {
        self.dsf.id()
    }

    pub fn addrs(&self) -> Vec<SocketAddr> {
        self.net.list().drain(..).map(|info| info.addr ).collect()
    }

    // Run the DSF daemon
    pub async fn run(&mut self) -> Result<(), Error> {
        let span = span!(Level::DEBUG, "engine", "{}", self.dsf.id());
        let _enter = span.enter();

        loop {

            select!{
                // Incoming network messages
                net_rx = self.net.next().fuse() => {
                    if let Some(m) = net_rx {
                        Self::handle_net_rx(&mut self.dsf, &mut self.net, &mut self.wire, m).await?;
                    }
                },
                // Outgoing network messages
                net_tx = self.wire.next().fuse() => {
                    if let Some(m) = net_tx {
                        Self::handle_net_tx(&mut self.dsf, &mut self.net, &mut self.wire, m).await?;
                    }
                }
                // Incoming RPC messages, response is inline
                rpc_rx = self.unix.next().fuse() => {
                    if let Some(m) = rpc_rx {
                        let mut dsf = self.dsf.clone();

                        // RPC tasks can take some time and thus must be independent threads
                        // To avoid blocking network operations
                        task::spawn(async move {
                            Self::handle_rpc(&mut dsf, m).await.unwrap();
                        });
                    }
                },
                // TODO: periodic update

            }
            
        }
    }

    async fn handle_net_tx(_dsf: &mut Dsf<WireConnector>, net: &mut Net, _wire: &mut Wire, m: NetMessage) -> Result<(), Error> {
        net.send(m).await.unwrap();
        Ok(())
    }

    async fn handle_net_rx(dsf: &mut Dsf<WireConnector>, net: &mut Net, wire: &mut Wire, m: NetMessage) -> Result<(), Error> {
        // Pass through wire module
        // This will route responses internally and return requests
        let address = m.address.clone();

        if let Some(req) = wire.handle(m).await.unwrap() {
            trace!("Engine request: {:?}", req);
            // Handle the request
            let resp = dsf.handle(address, req).unwrap();

            trace!("Engine response: {:?}", resp);

            let net_tx = wire.handle_outgoing(address, DsfMessage::Response(resp)).unwrap();

            // Send the response
            net.send(net_tx).await.unwrap();
        }

        Ok(())
    }

    
    async fn handle_rpc(dsf: &mut Dsf<WireConnector>, unix_req: UnixMessage) -> Result<(), Error> {
        // Parse out message
        let req: RpcRequest = serde_json::from_slice(&unix_req.data).unwrap();

        let span = span!(Level::TRACE, "rpc", id=req.req_id());
        let _enter = span.enter();
        
        // Handle RPC request
        let resp_kind = dsf.exec(req.kind()).await?;

        // Generate response
        let resp = RpcResponse::new(req.req_id(), resp_kind);

        // Encode response
        let enc = serde_json::to_vec(&resp).unwrap();

        // Send response via relevant unix connection
        let unix_resp = unix_req.response(Bytes::from(enc));

        unix_resp.send().await?;

        Ok(())
    }
}

