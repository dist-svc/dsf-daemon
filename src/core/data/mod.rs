

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{SystemTime, Duration};


use dsf_core::prelude::*;

pub use dsf_rpc::data::DataInfo;

use crate::error::Error;
use crate::store::Store;

pub struct DataManager {
    store: Arc<Mutex<Store>>,
}

impl DataManager {
    pub fn new(store: Arc<Mutex<Store>>) -> Self {
        Self{store}
    }

    /// Fetch data for a given service
    // TODO: add paging?
    pub fn fetch_data(&self, service_id: &Id, n: usize) -> Result<Vec<DataInfo>, Error> {
        let d = self.store.lock().unwrap().load_service_data(service_id)?;

        Ok(d)
    }

    /// Store data for a given service
    pub fn store_data(&self, info: &DataInfo, page: &Page) -> Result<(), Error> {
        // Store data object
        self.store.lock().unwrap().save_data(info)?;

        // Store backing raw object

        Ok(())
    }
}
