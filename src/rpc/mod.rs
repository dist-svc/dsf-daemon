

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
//pub mod subscribe;
impl <C> Dsf <C> where C: Connector + Clone + Sync + Send + 'static
{
    /// Execute an RPC command
    // TODO: Actually execute RPC commands
    pub async fn exec(&mut self, req: rpc::RequestKind) -> Result<rpc::ResponseKind, Error> {
        use rpc::*;

        debug!("Handling request: {:?}", req);

        let res = match req {
            RequestKind::Status => Ok(ResponseKind::Status(self.status())),

            RequestKind::Peer(PeerCommands::List(_options)) => Ok(ResponseKind::Peers(self.peer_info())),
            RequestKind::Peer(PeerCommands::Connect(options)) => self.connect(options).await.map(|i| ResponseKind::Connected(i)),

            _ => Ok(ResponseKind::Unrecognised),
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

    pub(crate) fn peer_info(&self) -> Vec<(Id, rpc::PeerInfo)> {
        self.peers().list().drain(..).map(|(id, p)| (id, p.info()) ).collect()
    }

    pub(crate) fn resolve_identifier(&mut self, identifier: &ServiceIdentifier) -> Result<Id, Error> {
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
