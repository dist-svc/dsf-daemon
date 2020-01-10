
use futures::channel::mpsc::SendError;

use dsf_core::types::{Error as CoreError};
use dsf_core::base::BaseError;

use crate::io::{NetError, UnixError};
use crate::store::StoreError;

#[derive(Debug)]
pub enum Error {
    Net(NetError),
    Unix(UnixError),
    Store(StoreError),
    Channel(SendError),
    Core(CoreError),
    Base(BaseError),

    Timeout,
    Unknown,
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

impl From<BaseError> for Error {
    fn from(e: BaseError) -> Self {
        Self::Base(e)
    }
}
