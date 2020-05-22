use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use structopt::StructOpt;

use async_std::stream;
use async_std::task;

use futures::prelude::*;
use futures::select;

use tracing::{span, Level};

use bytes::Bytes;

use dsf_core::net::Message as DsfMessage;
use dsf_core::service::{Publisher, ServiceBuilder};
use dsf_core::types::Id;

use dsf_rpc::{Request as RpcRequest, Response as RpcResponse};

use kad::Config as DhtConfig;

use crate::daemon::*;
use crate::error::Error;
use crate::io::*;
use crate::store::*;

use crate::daemon::Options as DaemonOptions;

pub struct Engine {
    dsf: Dsf<WireConnector>,

    unix: Unix,
    wire: Wire,
    net: Net,

    options: Options,
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

    #[structopt(
        long = "database-file",
        default_value = "/var/dsfd/dsf.db",
        env = "DSF_DB_FILE"
    )]
    /// Database file for storage by the daemon
    pub database_file: String,

    #[structopt(
        short = "s",
        long = "daemon-socket",
        default_value = "/var/run/dsfd/dsf.sock",
        env = "DSF_SOCK"
    )]
    /// Unix socket for communication with the daemon
    pub daemon_socket: String,

    #[structopt(long = "no-bootstrap")]
    /// Disable automatic bootstrapping
    pub no_bootstrap: bool,

    #[structopt(flatten)]
    pub daemon_options: DaemonOptions,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            bind_addresses: vec![SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
                10100,
            )],
            daemon_socket: DEFAULT_UNIX_SOCKET.to_string(),
            database_file: DEFAULT_DATABASE_FILE.to_string(),
            no_bootstrap: false,
            daemon_options: DaemonOptions {
                dht: DhtConfig::default(),
            },
        }
    }
}

impl Options {
    /// Helper constructor to run multiple instances alongside each other
    pub fn with_suffix(&self, suffix: usize) -> Self {
        Self {
            bind_addresses: vec![SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
                10100 + suffix as u16,
            )],
            daemon_socket: format!("{}.{}", self.daemon_socket, suffix),
            database_file: format!("{}.{}", self.database_file, suffix),
            no_bootstrap: self.no_bootstrap,
            daemon_options: DaemonOptions {
                dht: self.daemon_options.dht.clone(),
            },
        }
    }
}

impl Engine {
    /// Create a new daemon instance
    pub async fn new(options: Options) -> Result<Self, Error> {
        // Create new local data store
        let store = Store::new(&options.database_file)?;

        // Fetch or create new peer service
        let mut service = match store.load_peer_service()? {
            Some(s) => {
                info!("Loaded existing peer service: {}", s.id());
                s
            }
            None => {
                let s = ServiceBuilder::default().peer().build().unwrap();
                info!("Created new peer service: {}", s.id());
                s
            }
        };

        // Generate updated peer page
        let mut buff = vec![0u8; 1025];
        let (n, mut page) = service.publish_primary(&mut buff)?;
        page.raw = Some(buff[..n].to_vec());

        // Store service and page
        store.set_peer_service(&service, &page)?;

        let span = span!(Level::DEBUG, "engine", "{}", service.id());
        let _enter = span.enter();

        info!("Creating new engine");

        // Create new network connector
        info!(
            "Creating network connector on addresses: {:?}",
            options.bind_addresses
        );
        let mut net = Net::new();
        for addr in &options.bind_addresses {
            net.bind(NetKind::Udp, *addr).await?;
        }

        // Create new unix socket connector
        info!("Creating unix socket: {}", options.daemon_socket);
        let unix = Unix::new(&options.daemon_socket).await?;

        // Create new wire adaptor
        let wire = Wire::new(service.private_key().unwrap());

        // Create new DSF instance
        let dsf = Dsf::new(
            options.daemon_options.clone(),
            service,
            Arc::new(Mutex::new(store)),
            wire.connector(),
        )?;

        debug!("Engine created!");

        Ok(Self {
            dsf,
            wire,
            net,
            unix,
            options,
        })
    }

    pub fn id(&self) -> Id {
        self.dsf.id()
    }

    pub fn addrs(&self) -> Vec<SocketAddr> {
        self.net.list().drain(..).map(|info| info.addr).collect()
    }

    // Run the DSF daemon
    pub async fn run(&mut self, running: Arc<AtomicBool>) -> Result<(), Error> {
        let span = span!(Level::DEBUG, "engine", "{}", self.dsf.id());
        let _enter = span.enter();

        if !self.options.no_bootstrap {
            let mut d = self.dsf.clone();
            // Create future bootstrap event
            task::spawn(async move {
                task::sleep(Duration::from_secs(2)).await;
                let _ = d.bootstrap().await;
            });
        }

        // Create periodic timer
        let mut update_timer = stream::interval(Duration::from_secs(30));
        let mut tick_timer = stream::interval(Duration::from_secs(1));

        while running.load(Ordering::SeqCst) {
            #[cfg(feature = "profile")]
            let _fg = ::flame::start_guard("engine::tick");

            select! {
                // Incoming network messages
                net_rx = self.net.next().fuse() => {
                    trace!("engine::net_rx");

                    if let Some(m) = net_rx {
                        Self::handle_net_rx(&mut self.dsf, &mut self.net, &mut self.wire, m).await?;
                    }
                },
                // Outgoing network messages
                // TODO: this is one of the hot spots..?
                net_tx = self.wire.next().fuse() => {
                    trace!("engine::net_tx");

                    if let Some(m) = net_tx {
                        Self::handle_net_tx(&mut self.dsf, &mut self.net, &mut self.wire, m).await?;
                    }
                }
                // Incoming RPC messages, response is inline
                rpc_rx = self.unix.next().fuse() => {
                    trace!("engine::unix_rx");

                    if let Some(m) = rpc_rx {
                        let mut dsf = self.dsf.clone();
                        //let mut unix = self.unix.clone();

                        // RPC tasks can take some time and thus must be independent threads
                        // To avoid blocking network operations
                        task::spawn(async move {
                            Self::handle_rpc(&mut dsf, m).await.unwrap();
                        });
                    }
                },
                // TODO: periodic update
                interval = update_timer.next().fuse() => {
                    trace!("engine::tick");

                    if let Some(_i) = interval {

                    }
                },
                // Tick timer for process reactivity
                tick = tick_timer.next().fuse() => {},
            }
        }

        Ok(())
    }

    async fn handle_net_tx(
        _dsf: &mut Dsf<WireConnector>,
        net: &mut Net,
        _wire: &mut Wire,
        m: NetMessage,
    ) -> Result<(), Error> {
        net.send(m).await.unwrap();
        Ok(())
    }

    async fn handle_net_rx(
        dsf: &mut Dsf<WireConnector>,
        net: &mut Net,
        wire: &mut Wire,
        m: NetMessage,
    ) -> Result<(), Error> {
        // Pass through wire module
        // This will route responses internally and return requests
        let address = m.address.clone();

        if let Some(req) = wire.handle(m, |id| dsf.find_public_key(id) ).await.unwrap() {
            trace!("Engine request: {:?}", req);
            // Handle the request
            let resp = dsf.handle(address, req).unwrap();

            trace!("Engine response: {:?}", resp);

            let net_tx = wire
                .handle_outgoing(address, DsfMessage::Response(resp))
                .unwrap();

            // Send the response
            net.send(net_tx).await.unwrap();
        }

        Ok(())
    }

    async fn handle_rpc(dsf: &mut Dsf<WireConnector>, unix_req: UnixMessage) -> Result<(), Error> {
        // Parse out message
        let req: RpcRequest = serde_json::from_slice(&unix_req.data).unwrap();

        let span = span!(Level::TRACE, "rpc", id = req.req_id());
        let _enter = span.enter();

        // Handle RPC request
        let resp_kind = dsf.exec(req.kind()).await?;

        // Generate response
        let resp = RpcResponse::new(req.req_id(), resp_kind);

        // Encode response
        let enc = serde_json::to_vec(&resp).unwrap();

        // Generate response with required socket info
        let unix_resp = unix_req.response(Bytes::from(enc));

        // Send response
        if let Err(e) = unix_resp.send().await {
            error!("Error sending RPC response: {:?}", e);
        }

        Ok(())
    }
}
