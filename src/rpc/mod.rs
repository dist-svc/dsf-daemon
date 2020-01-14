
// Connect to an existing peer
//pub mod connect;

// Create and register new service
//pub mod create;

// Register an existing service
//pub mod register;

// Publish data for a given service
//pub mod publish;

// Locate an existing service
//pub mod locate;

// Query for data from a service
//pub mod query;

// Subscribe to a service
//pub mod subscribe;


use futures::prelude::*;

use dsf_core::prelude::*;
use dsf_core::net;

use crate::error::Error;
use crate::io::Connector;
use crate::daemon::Dsf;

pub mod connect;

impl <C> Dsf <C> where C: Connector + Clone + Sync + Send + 'static
{
    /// Execute an RPC command
    pub async fn exec() -> Result<(), Error> {
        unimplemented!()
    }
}
