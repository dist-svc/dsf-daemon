use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicBool, Ordering};

use std::time::Duration;

use structopt::StructOpt;

use async_std::stream;
use async_std::sync::channel;
use async_std::task;

use futures::prelude::*;
use futures::select;

use tracing::{span, Level};

use bytes::Bytes;

use dsf_core::net::Message as DsfMessage;
use dsf_core::service::{Publisher, ServiceBuilder};
use dsf_core::types::{Address, Id};

use dsf_rpc::{Request as RpcRequest, Response as RpcResponse};

use kad::Config as DhtConfig;

use crate::sync::{Arc, Mutex};
use crate::daemon::*;
use crate::error::Error;
use crate::io::*;
use crate::store::*;

use crate::daemon::Options as DaemonOptions;

pub struct Engine {
    dsf: Dsf<WireConnector>,

    unix: Option<Unix>,
    wire: Option<Wire>,
    net: Option<Net>,

    options: Options,
}

pub const DEFAULT_UNIX_SOCKET: &str = "/tmp/dsf.sock";
pub const DEFAULT_DATABASE_FILE: &str = "/tmp/dsf.db";
pub const DEFAULT_SERVICE: &str = "/tmp/dsf.svc";

#[derive(StructOpt, Debug, Clone, PartialEq)]
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
        let bind_addresses = self
            .bind_addresses
            .iter()
            .map(|a| {
                let mut a = a.clone();
                a.set_port(a.port() + suffix as u16);
                a
            })
            .collect();

        Self {
            bind_addresses,
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
                let s = ServiceBuilder::peer().build().unwrap();
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
            if let Err(e) = net.bind(NetKind::Udp, *addr).await {
                error!("Error binding interface: {:?}", addr);
                return Err(e.into());
            }
        }

        // Create new unix socket connector
        info!("Creating unix socket: {}", options.daemon_socket);
        let unix = match Unix::new(&options.daemon_socket).await {
            Ok(u) => u,
            Err(e) => {
                error!("Error binding unix socket: {}", options.daemon_socket);
                return Err(e.into());
            }
        };

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
            wire: Some(wire),
            net: Some(net),
            unix: Some(unix),
            options,
        })
    }

    pub fn id(&self) -> Id {
        self.dsf.id()
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

        let (net_in_tx, mut net_in_rx) = channel(1000);
        let (net_out_tx, mut net_out_rx) = channel(1000);

        // Setup network IO task

        let (mut wire, mut net) = (self.wire.take().unwrap(), self.net.take().unwrap());
        let r = running.clone();
        let net_dsf = self.dsf.clone();

        let _net_handle = task::spawn(async move {
            while r.load(Ordering::SeqCst) {
                select! {
                    // Incoming network messages
                    net_rx = net.next().fuse() => {
                        
                        if let Some(m) = net_rx {
                            trace!("engine::net_rx {:?}", m);

                            let address = m.address.clone();

                            // Decode message via wire module
                            let message = match wire.handle_incoming(&m, |id| net_dsf.find_public_key(id) ).await {
                                // Incoming request
                                Ok(Some(v)) => v,
                                // Incoming response, routed internally
                                Ok(None) => continue,
                                // Decoding error
                                Err(e) => {
                                    error!("error decoding network message from: {:?}", address);
                                    continue;
                                }
                            };

                            // Forward to DSF for execution
                            net_in_tx.send((address, message)).await;
                        }
                    },
                    net_tx = net_out_rx.next().fuse() => {

                        if let Some((address, message)) = net_tx {
                            trace!("engine::net_tx {:?} {:?}", address, message);

                            let net_tx = match wire.handle_outgoing(Address::from(address), message) {
                                Ok(v) => v,
                                Err(e) => {
                                    error!("error encoding network message: {:?}", e);
                                    continue;
                                }
                            };

                            if let Err(e) = net.send(net_tx).await {
                                error!("error sending network message: {:?}", e);
                                return Err(e)
                            }
                        }
                    }
                    // Outgoing network messages
                    net_tx = wire.next().fuse() => {
                        
                        if let Some(m) = net_tx {
                            trace!("engine::wire_tx {:?}", m);

                            net.send(m).await.unwrap();
                        }
                    }
                }
            }

            error!("Exiting network handler");

            Ok(())
        });

        let mut unix = self.unix.take().unwrap();
        let r = running.clone();
        let engine_dsf = self.dsf.clone();

        let _engine_handle = task::spawn(async move {

            while r.load(Ordering::SeqCst) {
                select! {
                    // Incoming network _requests_
                    net_rx = net_in_rx.next().fuse() => {

                        if let Some((address, req)) = net_rx {
                            let mut dsf = engine_dsf.clone();
                            let mut tx = net_out_tx.clone();

                            task::spawn(async move {

                                // Handle request via DSF
                                let resp = match dsf.handle_net(address, req.clone()).await {
                                    Ok(v) => v,
                                    Err(e) => {
                                        error!("error handling DSF request: {:?}", e);
                                        return;
                                    }
                                };

                                trace!("engine::handle_net rx: {:?} tx: {:?} for {:?}", req, resp, address);

                                // Return response
                                tx.send((address, DsfMessage::Response(resp))).await;
                            });
                        }
                    },
                    // Incoming RPC messages, response is inline
                    rpc_rx = unix.next().fuse() => {
                        trace!("engine::unix_rx");

                        if let Some(m) = rpc_rx {
                            let mut dsf = engine_dsf.clone();
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
                            // TODO: do something
                        }
                    },
                    // Tick timer for process reactivity
                    tick = tick_timer.next().fuse() => {},
                }
            }

            Ok(())
        });

        // TODO: join on net handle

        futures::try_join!(_engine_handle, _net_handle)?;

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
