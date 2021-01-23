use crate::sync::{Arc, Mutex};
use std::convert::TryFrom;

use log::{error, info, trace};

use dsf_core::{page::Page, types::Id};

pub use dsf_rpc::data::DataInfo;

use crate::error::Error;
use crate::store::Store;

pub struct DataManager {
    store: Store,
}

pub struct DataInst {
    pub info: DataInfo,
    pub page: Page,
}

impl From<Page> for DataInst {
    fn from(page: Page) -> Self {
        let info = DataInfo::try_from(&page).unwrap();
        DataInst { info, page }
    }
}

impl DataManager {
    pub fn new(store: Store) -> Self {
        Self { store }
    }

    /// Fetch data for a given service
    // TODO: add paging?
    pub fn fetch_data(&self, service_id: &Id, _limit: usize) -> Result<Vec<DataInst>, Error> {
        // Load service info
        let service = match self.store.find_service(service_id)? {
            Some(s) => s,
            None => return Err(Error::NotFound),
        };

        // Load data info
        let info: Vec<DataInfo> = self.store.find_data(service_id)?;

        info!("Loaded data info: {:?}", info);

        // Load associated raw pages
        let mut data = Vec::with_capacity(info.len());
        for i in info {
            info!("Fetching raw page for: {}", i.signature);

            let p = self
                .store
                .load_page(&i.signature, Some(service.public_key.clone()))?;
            data.push(DataInst {
                info: i,
                page: p.unwrap(),
            })
        }

        Ok(data)
    }

    /// Store data for a given service
    pub fn store_data(&self, info: &DataInfo, page: &Page) -> Result<(), Error> {
        // Store data object
        self.store.save_data(info)?;

        // Store backing raw object
        self.store.save_page(page)?;

        Ok(())
    }
}
