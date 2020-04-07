
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use dsf_core::prelude::*;
use dsf_rpc::replica::ReplicaInfo;

use crate::store::Store;


#[derive(Clone, Debug)]
pub struct Replica {
    info: ReplicaInfo,
    page: Page,
}

impl From<Page> for Replica {
    fn from(page: Page) -> Self {

        let info = ReplicaInfo{
            peer_id: page.info().peer_id(),

            version: page.version(),
            page_id: page.id.clone(),

            //peer: None,
            issued: page.issued(),
            updated: SystemTime::now(),
            expiry: page.expiry(),
            active: false,
        };

        Replica{page, info}
    }
}

pub struct ReplicaManager {
    store: Arc<Mutex<Store>>
}

impl ReplicaManager {
    pub fn new(store: Arc<Mutex<Store>>) -> Result<Self, ()> {
        // Create replica manager instance
        let m = ReplicaManager{
            store,
        };

        // TODO: load known replicas
        


        // Return replica manager
        Ok(m)
    }

    // Update a specified replica
    pub fn update(replica: &Replica) -> Result<Self, ()> {
        unimplemented!()
    }

    // Remove a specified replica
    pub fn remove(replica: &Replica) -> Result<Self, ()> {
        unimplemented!()
    }

    // Find replicas for a given service
    pub fn find(_ud: Id) -> Vec<Replica> {
        unimplemented!()
    }

    fn load() {

    }
}

