
use std::net::AddrParseError;
use diesel::ConnectionError;
use diesel::result::{Error as DieselError};
use strum::{ParseError as StrumError};
use base64::{DecodeError as B64Error};

#[derive(Debug, PartialEq)]
pub enum StoreError {
    Connection(ConnectionError),
    Diesel(DieselError),
    Strum(StrumError),
    B64(B64Error),
    Addr(AddrParseError),
    MissingSignature,
}

impl From<ConnectionError> for StoreError {
    fn from(e: ConnectionError) -> Self {
        Self::Connection(e)
    }
}

impl From<DieselError> for StoreError {
    fn from(e: DieselError) -> Self {
        Self::Diesel(e)
    }
}

impl From<StrumError> for StoreError {
    fn from(e: StrumError) -> Self {
        Self::Strum(e)
    }
}

impl From<B64Error> for StoreError {
    fn from(e: B64Error) -> Self {
        Self::B64(e)
    }
}

impl From<AddrParseError> for StoreError {
    fn from(e: AddrParseError) -> Self {
        Self::Addr(e)
    }
}