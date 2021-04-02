use std::task::{Context, Poll};

use log::{debug, error, info};
use tracing::{span, Level};

use futures::channel::mpsc;
use futures::prelude::*;

use dsf_core::prelude::*;
use dsf_rpc::*;

use crate::daemon::Dsf;
use crate::error::{CoreError, Error};

// Generic / shared operation types
pub mod ops;
use ops::*;

// Connect to an existing peer
pub mod connect;

// Lookup a peer in the database
pub mod lookup;

// Create and register new service
pub mod create;

// Register an existing service
pub mod register;

// Publish data for a given service
pub mod publish;

// Push DSF data for a given service
pub mod push;

// Locate an existing service
pub mod locate;

// Query for data from a service
pub mod query;

// Subscribe to a service
pub mod subscribe;

// Bootstrap daemon connectivity
pub mod bootstrap;

// Debug commands
pub mod debug;

impl Dsf {
    // Create a new RPC operation
    pub fn start_rpc(&mut self, req: Request, done: RpcSender) -> Result<(), Error> {
        let req_id = req.req_id();

        // Respond to non-async RPC requests immediately
        let resp = match req.kind() {
            RequestKind::Status => {
                let i = StatusInfo {
                    id: self.id(),
                    peers: self.peers().count(),
                    services: self.services().count(),
                };

                Some(ResponseKind::Status(i))
            }
            RequestKind::Peer(PeerCommands::List(_options)) => {
                let peers = self
                    .peers()
                    .list()
                    .drain(..)
                    .map(|(id, p)| (id, p.info()))
                    .collect();

                Some(ResponseKind::Peers(peers))
            }
            RequestKind::Peer(PeerCommands::Remove(options)) => {
                match self.resolve_peer_identifier(&options) {
                    Ok(id) => {
                        let p = self.peers().remove(&id);
                        match p {
                            Some(p) => Some(ResponseKind::Peer(p)),
                            None => Some(ResponseKind::None),
                        }
                    }
                    Err(_e) => Some(ResponseKind::None),
                }
            }
            RequestKind::Service(ServiceCommands::List(_options)) => {
                let s = self.services().list();
                Some(ResponseKind::Services(s))
            }
            RequestKind::Service(ServiceCommands::Info(options)) => {
                match self.resolve_identifier(&options.service) {
                    Ok(id) => Some(
                        self.services()
                            .find(&id)
                            .map(|i| ResponseKind::Service(i))
                            .unwrap_or(ResponseKind::None),
                    ),
                    Err(_e) => Some(ResponseKind::None),
                }
            }
            RequestKind::Service(ServiceCommands::SetKey(options)) => {
                match self.resolve_identifier(&options.service) {
                    Ok(id) => {
                        // TODO: Ehh?
                        let s = self.services().update_inst(&id, |s| {
                            s.service.set_secret_key(options.secret_key.clone());
                        });

                        match s {
                            Some(s) => Some(ResponseKind::Services(vec![s])),
                            None => Some(ResponseKind::None),
                        }
                    }
                    Err(_e) => Some(ResponseKind::None),
                }
            }
            RequestKind::Service(ServiceCommands::Remove(options)) => {
                match self.resolve_identifier(&options.service) {
                    Ok(id) => {
                        let s = self.services().remove(&id)?;

                        match s {
                            Some(s) => Some(ResponseKind::Service(s)),
                            None => Some(ResponseKind::None),
                        }
                    }
                    Err(e) => Some(ResponseKind::None),
                }
            }
            RequestKind::Data(DataCommands::List(data::ListOptions { service, bounds })) => {
                match self.resolve_identifier(&service) {
                    Ok(id) => {
                        let d = self.data().fetch_data(&id, bounds.count.unwrap_or(100))?;
                        let i = d.iter().map(|i| i.info.clone()).collect();

                        Some(ResponseKind::Data(i))
                    }
                    Err(_e) => Some(ResponseKind::None),
                }
            }

            _ => None,
        };

        if let Some(k) = resp {
            let r = Response::new(req_id, k);
            done.clone().try_send(r).unwrap();
            return Ok(());
        }

        // Otherwise queue up request for async execution
        let kind = match req.kind() {
            RequestKind::Peer(PeerCommands::Connect(opts)) => RpcKind::connect(opts),
            RequestKind::Peer(PeerCommands::Search(opts)) => RpcKind::lookup(opts),
            RequestKind::Service(ServiceCommands::Create(opts)) => RpcKind::create(opts),
            RequestKind::Service(ServiceCommands::Register(opts)) => RpcKind::register(opts),
            RequestKind::Service(ServiceCommands::Locate(opts)) => RpcKind::locate(opts),
            RequestKind::Service(ServiceCommands::Subscribe(opts)) => RpcKind::subscribe(opts),
            RequestKind::Data(DataCommands::Publish(opts)) => RpcKind::publish(opts),
            //RequestKind::Data(DataCommands::Query(options)) => unimplemented!(),
            //RequestKind::Debug(DebugCommands::Update) => self.update(true).await.map(|_| ResponseKind::None)?,
            RequestKind::Debug(DebugCommands::Bootstrap) => RpcKind::bootstrap(()),
            _ => {
                error!("RPC operation {:?} unimplemented", req.kind());
                return Ok(());
            }
        };

        let op = RpcOperation { req_id, kind, done };

        // TODO: check we're not overwriting anything here

        debug!("Adding RPC op {} to tracking", req_id);
        self.rpc_ops.insert(req_id, op);

        Ok(())
    }

    // Poll on pending RPC operations
    // Context must be propagated through here to keep the waker happy
    pub fn poll_rpc(&mut self, ctx: &mut Context) -> Result<(), Error> {
        // Take RPC operations so we can continue using `&mut self`
        let mut rpc_ops: Vec<_> = self.rpc_ops.drain().collect();
        let mut ops_done = vec![];

        // Iterate through and update each operation
        for (_req_id, op) in &mut rpc_ops {
            let RpcOperation { kind, done, req_id } = op;

            let complete = match kind {
                RpcKind::Connect(connect) => {
                    self.poll_rpc_connect(*req_id, connect, ctx, done.clone())?
                }
                RpcKind::Locate(locate) => {
                    self.poll_rpc_locate(*req_id, locate, ctx, done.clone())?
                }
                RpcKind::Register(register) => {
                    self.poll_rpc_register(*req_id, register, ctx, done.clone())?
                }
                RpcKind::Create(create) => {
                    self.poll_rpc_create(*req_id, create, ctx, done.clone())?
                }
                RpcKind::Lookup(lookup) => {
                    self.poll_rpc_lookup(*req_id, lookup, ctx, done.clone())?
                }
                RpcKind::Bootstrap(bootstrap) => {
                    self.poll_rpc_bootstrap(*req_id, bootstrap, ctx, done.clone())?
                }
                RpcKind::Publish(publish) => {
                    self.poll_rpc_publish(*req_id, publish, ctx, done.clone())?
                }
                RpcKind::Subscribe(subscribe) => {
                    self.poll_rpc_subscribe(*req_id, subscribe, ctx, done.clone())?
                }
                _ => {
                    error!("Unsuported async RPC: {}", kind);
                    return Ok(());
                }
            };

            if complete {
                ops_done.push(req_id.clone());
            }
        }

        // Re-add updated operations
        for (req_id, op) in rpc_ops {
            self.rpc_ops.insert(req_id, op);
        }

        // Remove completed operations
        for d in ops_done {
            self.rpc_ops.remove(&d);
        }

        Ok(())
    }

    pub(super) fn resolve_identifier(
        &mut self,
        identifier: &ServiceIdentifier,
    ) -> Result<Id, Error> {
        // Short circuit if ID specified or error if none
        let index = match (&identifier.id, identifier.index) {
            (Some(id), _) => return Ok(id.clone()),
            (None, None) => {
                error!("service id or index must be specified");
                return Err(Error::UnknownService);
            }
            (_, Some(index)) => index,
        };

        match self.services().index_to_id(index) {
            Some(id) => Ok(id),
            None => {
                error!("no service matching index: {}", index);
                Err(Error::UnknownService)
            }
        }
    }

    pub(super) fn resolve_peer_identifier(
        &mut self,
        identifier: &ServiceIdentifier,
    ) -> Result<Id, Error> {
        // Short circuit if ID specified or error if none
        let index = match (&identifier.id, identifier.index) {
            (Some(id), _) => return Ok(id.clone()),
            (None, None) => {
                error!("service id or index must be specified");
                return Err(Error::Core(CoreError::NoPeerId));
            }
            (_, Some(index)) => index,
        };

        match self.peers().index_to_id(index) {
            Some(id) => Ok(id),
            None => {
                error!("no peer matching index: {}", index);
                Err(Error::Core(CoreError::UnknownPeer))
            }
        }
    }
}
