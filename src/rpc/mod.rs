

use tracing::{span, Level};


use dsf_core::prelude::*;

use dsf_rpc::{self as rpc, ServiceIdentifier};

use crate::error::{Error, CoreError};
use crate::io::Connector;
use crate::daemon::Dsf;

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


impl <C> Dsf <C> where C: Connector + Clone + Sync + Send + 'static
{
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
            },
        };

        debug!("Result: {:?}", res);

        match res {
            Ok(v) => Ok(v),
            Err(Error::Core(e)) => Ok(ResponseKind::Error(e)),
            Err(e) => {
                error!("Unsupported RPC error: {:?}", e);
                Ok(ResponseKind::Error(CoreError::Unknown))
            }
        }
    }

    pub(crate) fn status(&mut self) -> rpc::StatusInfo {
        rpc::StatusInfo{
            id: self.id(),
            peers: self.peers().count(),
            services: self.services().count(),
        }
    }

    async fn exec_debug(&mut self, req: rpc::DebugCommands) -> Result<rpc::ResponseKind, Error> {
        use rpc::*;

        let res = match req {
            DebugCommands::Datastore => {
                println!("{:?}", self.datastore());
                ResponseKind::None
            },
            DebugCommands::Update => {
                self.update(true).await.map(|_| { ResponseKind::None })?
            },
            DebugCommands::Bootstrap => {
                self.bootstrap().await.map(|_| { ResponseKind::None })?
            },
        };

        Ok(res)
    }

    async fn exec_peer(&mut self, req: rpc::PeerCommands) -> Result<rpc::ResponseKind, Error> {
        use rpc::*;

        let mut s = self.clone();

        let res = match req {
            PeerCommands::List(_options) => ResponseKind::Peers(self.peer_info()),
            PeerCommands::Connect(options) => {
                s.connect(options).await.map(ResponseKind::Connected)?
            },
            _ => ResponseKind::Unrecognised,
        };

        Ok(res)
    }

    async fn exec_data(&mut self, req: rpc::DataCommands) -> Result<rpc::ResponseKind, Error> {
        use rpc::*;

        let res = match req {
            DataCommands::List(ListOptions{service, n}) => {
                let id = self.resolve_identifier(&service)?;
                self.get_data(&id, n).map(ResponseKind::Data)?
            },
            DataCommands::Publish(options) => {
                self.publish(options).await.map(ResponseKind::Published)?
            },
            _ => {
                ResponseKind::Unrecognised
            }
        };

        Ok(res)
    }

    async fn exec_service(&mut self, req: rpc::ServiceCommands) -> Result<rpc::ResponseKind, Error> {
        use rpc::*;

        let res = match req {
            ServiceCommands::List(_options) => {
                ResponseKind::Services(self.get_services())
            },
            ServiceCommands::Create(options) => {
                self.create(options).await.map(ResponseKind::Created)?
            },
            ServiceCommands::Register(options) => {
                self.register(options).await.map(ResponseKind::Registered)?
            },
            ServiceCommands::Search(options) => {
                self.locate(options).await.map(ResponseKind::Located)?
            },
            ServiceCommands::Subscribe(options) => {
                self.subscribe(options).await.map(ResponseKind::Subscribed)?
            },
            ServiceCommands::SetKey(options) => {
                let id = self.resolve_identifier(&options.service)?;

                // Ehh?
                let s = self.services().update_inst(&id, |s| {
                    s.service.set_secret_key(options.secret_key);
                });

                match s {
                    Some(s) => ResponseKind::Services(vec![s]),
                    None => ResponseKind::None,
                }
                
            }
            _ => ResponseKind::Unrecognised,
        };

        Ok(res)
    }

    pub(super) fn peer_info(&mut self) -> Vec<(Id, rpc::PeerInfo)> {
        self.peers().list().drain(..).map(|(id, p)| (id, p.info()) ).collect()
    }

    pub(super) fn get_data(&mut self, id: &Id, count: usize) -> Result<Vec<dsf_rpc::DataInfo>, Error> {
        let d = self.services().data(id, count)?;
        Ok(d)
    }

    pub(super) fn get_services(&mut self) -> Vec<dsf_rpc::ServiceInfo> {
        self.services().list()
    }

    pub(super) fn resolve_identifier(&mut self, identifier: &ServiceIdentifier) -> Result<Id, Error> {
        // Short circuit if ID specified or error if none
        let index = match (identifier.id, identifier.index) {
            (Some(id), _) => return Ok(id),
            (None, None) => {
                error!("service id or index must be specified");
                return Err(Error::UnknownService)
            },
            (_, Some(index)) => index
        };

        match self.services().index_to_id(index) {
            Some(id) => Ok(id),
            None => {
                error!("no service matching index: {}", index);
                Err(Error::UnknownService)
            }
        }
    }
}
