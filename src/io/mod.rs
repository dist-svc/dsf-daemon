
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::pin::Pin;


use futures::prelude::*;
use futures::channel::mpsc;
use futures::task::{Poll, Context};

use async_std::future::timeout;
use async_std::task::{self, JoinHandle};

pub mod net;
pub use net::{Net, NetError, NetMessage, NetKind};

pub mod unix;
pub use unix::{Unix, UnixError, UnixMessage};

pub mod wire;
pub use wire::{Wire, WireConnector};

pub mod mock;

use dsf_core::prelude::*;

use crate::error::Error;

#[async_trait]
pub trait Connector {
    // Send a request and receive a response or error at some time in the future
    async fn request(
        &self, req_id: RequestId, target: Address, req: NetRequest, timeout: Duration,
    ) -> Result<NetResponse, Error>;

    // Send a response message
    async fn respond(
        &self, req_id: RequestId, target: Address, resp: NetResponse,
    ) -> Result<(), Error>;
}
