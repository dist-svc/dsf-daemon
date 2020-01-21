
use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;

use crate::core::services::{ServiceInst};

use dsf_core::prelude::*;

pub struct Store {
    conn: SqliteConnection,
}

#[derive(Debug, PartialEq)]
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


    pub fn get_service_inst(&mut self, id: Id) -> Result<ServiceInst, StoreError> {
        unimplemented!()
    }


}


#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn store_service_inst() {
        let mut store = Store::new("/tmp/dsf-test-1.db")
            .expect("Error opening store");


        
    }

}