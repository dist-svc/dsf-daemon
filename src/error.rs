
use crate::io::{NetError, UnixError};

#[derive(Debug)]
pub enum Error {
    Net(NetError),
    Unix(UnixError),
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