use std::task::{Poll, Context};

use tracing::{span, Level};
use log::{debug, info, error};

use futures::prelude::*;
use futures::channel::mpsc;

use dsf_core::prelude::*;
use dsf_rpc::{self as rpc, ServiceIdentifier};

use crate::daemon::Dsf;
use crate::error::{CoreError, Error};

// Generic / shared operation types
pub mod ops;
use ops::*;

// Connect to an existing peer
pub mod connect;

// Create and register new service
pub mod create;

// Register an existing service
pub mod register;

// Publish data for a given service
pub mod publish;

// Locate an existing service
pub mod locate;

// Query for data from a service
pub mod query;

// Subscribe to a service
pub mod subscribe;

// Debug commands
pub mod debug;

impl Dsf {
    /// Execute an RPC command
    // TODO: Actually execute RPC commands
    pub async fn exec(&mut self, req: rpc::RequestKind) -> Result<rpc::ResponseKind, Error> {
        let span = span!(Level::DEBUG, "rpc", "{}", self.id());
        let _enter = span.enter();

        use rpc::*;

        debug!("Handling request: {:?}", req);

        let res = match req {
            RequestKind::Status => Ok(ResponseKind::Status(self.status())),

            RequestKind::Peer(options) => self.exec_peer(options).await,
            RequestKind::Service(options) => self.exec_service(options).await,
            RequestKind::Data(options) => self.exec_data(options).await,
            RequestKind::Debug(options) => self.exec_debug(options).await,

            _ => {
                error!("Unrecognized RPC request: {:?}", req);
                Ok(ResponseKind::Unrecognised)
            }
        };

        debug!("Result: {:?}", res);

        match res {
            Ok(v) => Ok(v),
            Err(Error::Core(e)) => Ok(ResponseKind::Error(e)),
            Err(Error::Timeout) => Ok(ResponseKind::Error(CoreError::Timeout)),
            Err(e) => {
                error!("Unsupported RPC error: {:?}", e);
                Ok(ResponseKind::Error(CoreError::Unknown))
            }
        }
    }

    // Create a new RPC operation
    pub fn start_rpc(&mut self, req: rpc::Request, done: RpcSender) -> Result<(), Error> {

        let req_id = req.req_id();

        let kind = match req.kind() {
            rpc::RequestKind::Status => RpcKind::Status,
            rpc::RequestKind::Peer(rpc::PeerCommands::Connect(opts)) => RpcKind::connect(opts),
            _ => {
                error!("RPC start {:?} unimplemented", req.kind());
                return Ok(());
            },
        };

        let op = RpcOperation {
            req_id,
            kind,
            done,
        };

        // TODO: check we're not overwriting anything here

        debug!("Adding RPC op {} to tracking", req_id);
        self.rpc_ops.as_mut().unwrap().insert(req_id, op);

        Ok(())
    }

    // Poll on pending RPC operations
    // Context must be propagated through here to keep the waker happy
    pub fn poll_rpc(&mut self, ctx: &mut Context) -> Result<(), Error> {

        // Take RPC operations so we can continue using `&mut self`
        let mut rpc_ops = self.rpc_ops.take().unwrap();
        let mut ops_done = vec![];
        
        // Iterate through and update each operation
        for (req_id, mut op) in rpc_ops.iter_mut() {

            let RpcOperation{kind, done, req_id} = op;

            let complete = match kind {
                RpcKind::Status => {
                    let resp = rpc::Response::new(*req_id,  rpc::ResponseKind::Status(self.status()));

                    done.clone().try_send(resp).unwrap();
                    true
                },
                RpcKind::Connect(connect) => self.poll_rpc_connect(*req_id, connect, ctx, done.clone())?,
                _ => unimplemented!(),
            };

            if complete {
                ops_done.push(req_id.clone());
            }
        }

        // Remove completed operations
        for d in ops_done {
            rpc_ops.remove(&d);
        }

        // Return updated RPC operations
        self.rpc_ops = Some(rpc_ops);

        Ok(())
    }

    pub(crate) fn status(&self) -> rpc::StatusInfo {
        rpc::StatusInfo {
            id: self.id(),
            peers: self.peers().count(),
            services: self.services().count(),
        }
    }

    async fn exec_debug(&mut self, req: rpc::DebugCommands) -> Result<rpc::ResponseKind, Error> {
        use rpc::*;

        let res = match req {
            DebugCommands::Datastore(_opts) => {
                //println!("{:?}", self.datastore());
                ResponseKind::None
            }
            DebugCommands::Dht(DhtCommands::Data(service)) => {
                let id = self.resolve_identifier(&service)?;

                let pages = self.search(&id).await?;

                ResponseKind::Pages(pages)
            }
            DebugCommands::Dht(DhtCommands::Peer(id)) => {
                let id = self.resolve_peer_identifier(&id)?;

                let peer = self.lookup(&id).await?;

                ResponseKind::Peers(vec![(id, peer.info())])
            }
            DebugCommands::Update => self.update(true).await.map(|_| ResponseKind::None)?,
            DebugCommands::Bootstrap => self.bootstrap().await.map(|_| ResponseKind::None)?,
        };

        Ok(res)
    }

    async fn exec_peer(&mut self, req: rpc::PeerCommands) -> Result<rpc::ResponseKind, Error> {
        use rpc::*;

        let res = match req {
            PeerCommands::List(_options) => ResponseKind::Peers(self.peer_info().await),
            PeerCommands::Connect(options) => {
                self.connect(options)?.await.map(ResponseKind::Connected)?
            }
            PeerCommands::Search(options) => {
                // TODO: pass timeout here
                self.lookup(&options.id)
                    .await
                    .map(|p| ResponseKind::Peers(vec![(p.id(), p.info())]))?
            }
            PeerCommands::Remove(options) => {
                let id = self.resolve_peer_identifier(&options)?;

                let p = self.peers().remove(&id);

                match p {
                    Some(p) => ResponseKind::Peer(p),
                    None => ResponseKind::None,
                }
            }
            _ => ResponseKind::Unrecognised,
        };

        Ok(res)
    }

    async fn exec_data(&mut self, req: rpc::DataCommands) -> Result<rpc::ResponseKind, Error> {
        use rpc::*;

        let res = match req {
            DataCommands::List(data::ListOptions { service, bounds }) => {
                let id = self.resolve_identifier(&service)?;
                self.get_data(&id, bounds.count.unwrap_or(100))
                    .await
                    .map(ResponseKind::Data)?
            }
            DataCommands::Publish(options) => {
                self.publish(options).await.map(ResponseKind::Published)?
            }
            _ => ResponseKind::Unrecognised,
        };

        Ok(res)
    }

    async fn exec_service(
        &mut self,
        req: rpc::ServiceCommands,
    ) -> Result<rpc::ResponseKind, Error> {
        use rpc::*;

        let res = match req {
            ServiceCommands::List(_options) => ResponseKind::Services(self.get_services()),
            ServiceCommands::Create(options) => {
                self.create(options).await.map(ResponseKind::Service)?
            }
            ServiceCommands::Register(options) => {
                self.register(options)?.await.map(ResponseKind::Registered)?
            }
            ServiceCommands::Locate(options) => {
                self.locate(options).await.map(ResponseKind::Located)?
            }
            ServiceCommands::Info(options) => {
                let id = self.resolve_identifier(&options.service)?;

                self.services()
                    .find(&id)
                    .map(|i| ResponseKind::Service(i))
                    .unwrap_or(ResponseKind::None)
            }
            ServiceCommands::Subscribe(options) => self
                .subscribe(options)
                .await
                .map(ResponseKind::Subscribed)?,
            ServiceCommands::SetKey(options) => {
                let id = self.resolve_identifier(&options.service)?;

                // TODO: Ehh?
                let s = self.services().update_inst(&id, |s| {
                    s.service.set_secret_key(options.secret_key.clone());
                });

                match s {
                    Some(s) => ResponseKind::Services(vec![s]),
                    None => ResponseKind::None,
                }
            }
            ServiceCommands::Remove(options) => {
                let id = self.resolve_identifier(&options.service)?;

                let s = self.services().remove(&id)?;

                match s {
                    Some(s) => ResponseKind::Service(s),
                    None => ResponseKind::None,
                }
            }
            _ => ResponseKind::Unrecognised,
        };

        Ok(res)
    }

    pub(super) async fn peer_info(&mut self) -> Vec<(Id, rpc::PeerInfo)> {
        self.peers()
            .list()
            .drain(..)
            .map(|(id, p)| (id, p.info()))
            .collect()
    }

    pub(super) async fn get_data(
        &mut self,
        id: &Id,
        count: usize,
    ) -> Result<Vec<dsf_rpc::DataInfo>, Error> {
        let d = self.data().fetch_data(id, count)?;
        Ok(d.iter().map(|i| i.info.clone()).collect())
    }

    pub(super) fn get_services(&mut self) -> Vec<dsf_rpc::ServiceInfo> {
        self.services().list()
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
