



use futures::prelude::*;

use dsf_core::prelude::*;
use dsf_core::net;
use dsf_rpc::ServiceIdentifier;

use crate::error::Error;
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
    pub async fn exec() -> Result<(), Error> {
        unimplemented!()
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
