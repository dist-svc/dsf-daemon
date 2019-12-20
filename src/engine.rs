

use std::net::{SocketAddr, IpAddr, Ipv4Addr};

use structopt::StructOpt;

use futures::prelude::*;
use futures::select;

use tracing::{span, Level};

use bytes::Bytes;

use dsf_rpc::{Request as RpcRequest, Response as RpcResponse, ResponseKind as RpcResponseKind};

use crate::error::Error;
use crate::io::*;
use crate::store::*;

pub struct Engine {
    net: Net,
    unix: Unix,

    store: Store,
}

pub const DEFAULT_UNIX_SOCKET: &str = "/tmp/dsf.sock";
pub const DEFAULT_DATABASE: &str = "/tmp/dsf.sock";

#[derive(StructOpt, Builder, Debug, Clone, PartialEq)]
#[builder(default)]
pub struct Options {
    #[structopt(short = "a", long = "bind-address", default_value = "0.0.0.0:10100")]
    /// Interface(s) to bind DSF daemon
    /// These may be reconfigured at runtime
    pub bind_addresses: Vec<SocketAddr>,

    #[structopt(short = "d", default_value = "/var/dsf/dsf.db", env="DSF_DB")]
    /// Unix socket for communication with the daemon
    pub database: String,

    #[structopt(short = "s", long = "daemon-socket", default_value = "/tmp/dsf.sock", env="DSF_SOCK")]
    /// Unix socket for communication with the daemon
    pub daemon_socket: String,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            bind_addresses: vec![SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 10100)],
            daemon_socket: DEFAULT_UNIX_SOCKET.to_string(),
            database: DEFAULT_DATABASE.to_string(),
        }
    }
}

impl Engine {
    /// Create a new daemon instance
    pub async fn new(options: Options) -> Result<Self, Error> {
        debug!("new daemon, options: {:?}", options);

        // Create new network connector
        let mut net = Net::new();
        for addr in &options.bind_addresses {
            net.bind(NetKind::Udp, *addr).await?;
        }

        // Create new unix socket connector
        let mut unix = Unix::new(&options.daemon_socket).await?;

        // Create new local data store
        let mut store = Store::new(&options.database)?;

        Ok(Self{net, unix, store})
    }

    /// Run a daemon instance
    /// 
    /// This blocks forever
    pub async fn run(&mut self) -> Result<(), Error> {

        loop {

            select!{
                // Incoming network messages
                net_rx = self.net.next().fuse() => {
                    if let Some(m) = net_rx {
                        self.handle_net(m).await?;
                    }
                },
                // Incoming RPC messages
                rpc_rx = self.unix.next().fuse() => {
                    if let Some(m) = rpc_rx {
                        self.handle_rpc(m).await?;
                    }
                },
            }
            
        }
    }

    async fn handle_net(&mut self, msg: NetMessage) -> Result<(), Error> {
        unimplemented!()
    }

    async fn handle_rpc(&mut self, msg: UnixMessage) -> Result<(), Error> {
        // Parse out message
        let req: RpcRequest = serde_json::from_slice(&msg.data).unwrap();

        let span = span!(Level::TRACE, "rpc", id=req.req_id());
        let _enter = span.enter();
        
        // TODO: handle RPC request
        let resp = RpcResponse::new(req.req_id(), RpcResponseKind::None);

        // Encode and send response
        let enc = serde_json::to_vec(&resp).unwrap();

        let msg = UnixMessage {
            connection_id: msg.connection_id,
            data: Bytes::from(enc),
        };
        self.unix.send(msg).await?;

        Ok(())
    }
}

