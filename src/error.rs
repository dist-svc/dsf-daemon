use async_std::future::TimeoutError;
use futures::channel::mpsc::SendError;

pub use dsf_core::error::Error as CoreError;

pub use crate::io::{NetError, UnixError};
pub use crate::store::StoreError;

#[derive(Debug, PartialEq)]
pub enum Error {
    Net(NetError),
    Unix(UnixError),
    Store(StoreError),
    Channel(SendError),
    Core(CoreError),

    Timeout,
    Unknown,
    Unimplemented,
    NotFound,
    UnknownService,
    NoReplicasFound,
    NoPrivateKey,
    Closed,
}

impl From<NetError> for Error {
    fn from(e: NetError) -> Self {
        Self::Net(e)
    }
}

impl From<UnixError> for Error {
    fn from(e: UnixError) -> Self {
        Self::Unix(e)
    }
}

impl From<StoreError> for Error {
    fn from(e: StoreError) -> Self {
        Self::Store(e)
    }
}

impl From<SendError> for Error {
    fn from(e: SendError) -> Self {
        Self::Channel(e)
    }
}

impl From<CoreError> for Error {
    fn from(e: CoreError) -> Self {
        Self::Core(e)
    }
}

impl From<TimeoutError> for Error {
    fn from(_e: TimeoutError) -> Self {
        Self::Timeout
    }
}
