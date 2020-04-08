

use std::sync::{Arc, Mutex};

use dsf_core::{types::Id, page::Page};

pub use dsf_rpc::data::DataInfo;

use crate::error::Error;
use crate::store::Store;

pub struct DataManager {
    store: Arc<Mutex<Store>>,
}

pub struct DataInst {
    info: DataInfo,
    page: Page,
}

impl From<Page> for DataInst {
    fn from(page: Page) -> Self {
        let info = DataInfo {
            service: page.id.clone(),
            index: page.version(),
            body: page.body().clone(),
            previous: page.previous_sig.clone(),
            signature: page.signature.unwrap().clone(),
        };

        DataInst{info, page}
    }
}

impl DataManager {
    pub fn new(store: Arc<Mutex<Store>>) -> Self {
        Self{store}
    }

    /// Fetch data for a given service
    // TODO: add paging?
    pub fn fetch_data(&self, service_id: &Id) -> Result<Vec<DataInst>, Error> {
        let store = self.store.lock().unwrap();

        // Load data info
        let info = store.load_service_data(service_id)?;

        // Load associated raw pages
        let mut data = Vec::with_capacity(info.len());
        for i in info {
            let p = store.load_page(&i.signature)?;
            data.push(DataInst{info: i, page: p.unwrap()})
        }

        Ok(data)
    }

    /// Store data for a given service
    pub fn store_data(&self, info: &DataInfo, page: &Page) -> Result<(), Error> {
        let store = self.store.lock().unwrap();

        // Store data object
        store.save_data(info)?;

        // Store backing raw object
        store.save_page(page)?;

        Ok(())
    }
}
