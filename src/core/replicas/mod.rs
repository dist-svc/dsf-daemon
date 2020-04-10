
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use dsf_core::prelude::*;
use dsf_rpc::replica::ReplicaInfo;

use crate::store::Store;


#[derive(Clone, Debug)]
pub struct ReplicaInst {
    pub info: ReplicaInfo,
    pub page: Page,
}

impl From<Page> for ReplicaInst {
    fn from(page: Page) -> Self {

        // Replica pages are _always_ secondary types
        let peer_id = match page.info() {
            PageInfo::Secondary(s) => s.peer_id,
            _ => unimplemented!()
        };

        let info = ReplicaInfo{
            peer_id,

            version: page.version(),
            page_id: page.id.clone(),

            //peer: None,
            issued: page.issued(),
            updated: SystemTime::now(),
            expiry: page.expiry(),

            active: false,
        };

        Self{page, info}
    }
}

#[derive(Clone)]
pub struct ReplicaManager {
    store: Arc<Mutex<Store>>
}

impl ReplicaManager {
    pub fn new(store: Arc<Mutex<Store>>) -> Self {
        // Create replica manager instance
        let mut m = ReplicaManager{
            store,
        };

        // Load existing replicas from database
        m.load();

        // Return replica manager
        m
    }


    // Find replicas for a given service
    pub fn find(&self, _service_id: &Id) -> Vec<ReplicaInst> {
        unimplemented!()
    }

    // Update a specified replica
    pub fn update(&self, replica: &ReplicaInst) -> Result<Self, ()> {
        unimplemented!()
    }

    pub fn update_fn<F: Fn(&mut ReplicaInst)>(&self, service_id: &Id, peer_id: &Id, f: F) -> Result<(), ()> {
        unimplemented!()
    }

    // Remove a specified replica
    pub fn remove(&self, replica: &ReplicaInst) -> Result<Self, ()> {
        unimplemented!()
    }

    // Sync replicas for a given service
    pub fn sync(&self, _service_id: &Id, replicas: &[ReplicaInst]) {
        unimplemented!()
    }


    fn load(&mut self) {

    }
}

