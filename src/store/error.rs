
use std::net::AddrParseError;
use strum::ParseError as StrumError;

#[derive(Debug, PartialEq)]
pub enum StoreError {
    Connection(ConnectionError),
    Strum(StrumError),
    B64(base64::DecodeError),
    Addr(AddrParseError),
    MissingSignature,
    MissingRawData,
    NotFound,
}

impl From<StrumError> for StoreError {
    fn from(e: StrumError) -> Self {
        Self::Strum(e)
    }
}

impl From<base64::DecodeError> for StoreError {
    fn from(e: base64::DecodeError) -> Self {
        Self::B64(e)
    }
}

impl From<AddrParseError> for StoreError {
    fn from(e: AddrParseError) -> Self {
        Self::Addr(e)
    }
}
