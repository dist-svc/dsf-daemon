
use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;

pub struct Store {
    conn: SqliteConnection,
}

#[derive(Debug)]
pub enum StoreError {
    Connection(ConnectionError),
}

impl From<ConnectionError> for StoreError {
    fn from(e: ConnectionError) -> Self {
        Self::Connection(e)
    }
}

impl Store {
    pub fn new(path: &str) -> Result<Self, StoreError> {
        let conn = SqliteConnection::establish(path)?;

        Ok(Self{conn})
    }
}