use crate::sync::{Arc, Mutex};
use std::collections::HashMap;
use std::time::SystemTime;

use log::{debug, error, trace};

use dsf_core::prelude::*;
use dsf_rpc::replica::ReplicaInfo;

#[derive(Clone, Debug)]
pub struct ReplicaInst {
    pub info: ReplicaInfo,
    pub page: Page,
}

impl From<Page> for ReplicaInst {
    fn from(page: Page) -> Self {
        // Replica pages are _always_ secondary types
        let peer_id = match page.info() {
            PageInfo::Secondary(s) => s.peer_id.clone(),
            _ => unimplemented!(),
        };

        let info = ReplicaInfo {
            peer_id,

            version: page.header.index(),
            page_id: page.id.clone(),

            //peer: None,
            issued: page.issued().unwrap().into(),
            expiry: page.expiry().map(|v| v.into()),
            updated: SystemTime::now(),

            active: false,
        };

        Self { page, info }
    }
}

pub struct ReplicaManager {
    replicas: HashMap<Id, Vec<ReplicaInst>>,
}

impl ReplicaManager {
    /// Create a new replica manager
    pub fn new() -> Self {
        ReplicaManager {
            replicas: HashMap::new(),
        }
    }

    /// Find replicas for a given service
    pub fn find(&self, service_id: &Id) -> Vec<ReplicaInst> {
        let v = self.replicas.get(service_id);

        v.map(|v| v.clone()).unwrap_or(vec![])
    }

    /// Create or update a given replica instance
    pub fn create_or_update(&mut self, service_id: &Id, peer_id: &Id, page: &Page) {
        let replicas = self.replicas.entry(service_id.clone()).or_insert(vec![]);
        let replica = replicas.iter_mut().find(|r| &r.info.peer_id == peer_id);

        match replica {
            Some(r) => *r = ReplicaInst::from(page.clone()),
            None => {
                let r = ReplicaInst::from(page.clone());
                replicas.push(r);
            }
        }
    }

    /// Update a specified replica
    pub fn update_replica<F: Fn(&mut ReplicaInst)>(
        &mut self,
        service_id: &Id,
        peer_id: &Id,
        f: F,
    ) -> Result<(), ()> {
        let replicas = self.replicas.entry(service_id.clone()).or_insert(vec![]);
        let replica = replicas.iter_mut().find(|r| &r.info.peer_id == peer_id);

        if let Some(mut r) = replica {
            f(&mut r);
        }

        Ok(())
    }

    /// Remove a specified replica
    pub fn remove(&self, _service_id: &Id, _peer_id: &Id) -> Result<Self, ()> {
        unimplemented!()
    }
}
