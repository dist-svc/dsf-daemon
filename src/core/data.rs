

use std::sync::{Arc, Mutex};
use std::convert::TryFrom;

use dsf_core::{types::Id, page::Page};

pub use dsf_rpc::data::DataInfo;

use crate::error::Error;
use crate::store::{Store, Base as _};

#[derive(Clone)]
pub struct DataManager {
    store: Arc<Mutex<Store>>,
}

pub struct DataInst {
    pub info: DataInfo,
    pub page: Page,
}

impl From<Page> for DataInst {
    fn from(page: Page) -> Self {
        let info = DataInfo::try_from(&page).unwrap();
        DataInst{info, page}
    }
}

impl DataManager {
    pub fn new(store: Arc<Mutex<Store>>) -> Self {
        Self{store}
    }

    /// Fetch data for a given service
    // TODO: add paging?
    pub fn fetch_data(&self, service_id: &Id, _limit: usize) -> Result<Vec<DataInst>, Error> {
        let store = self.store.lock().unwrap();

        // Load data info
        let info: Vec<DataInfo> = store.find(service_id)?;

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
        store.save(info)?;

        // Store backing raw object
        store.save(page)?;

        Ok(())
    }
}
