
use crate::io::{NetError, UnixError};
use crate::store::StoreError;

#[derive(Debug)]
pub enum Error {
    Net(NetError),
    Unix(UnixError),
    Store(StoreError),
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