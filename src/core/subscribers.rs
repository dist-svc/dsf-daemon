

use std::sync::{Arc, Mutex};
use std::collections::HashMap;

use dsf_core::types::Id;

pub use dsf_rpc::SubscriptionInfo;

use crate::error::Error;
use crate::store::Store;

use crate::core::peers::Peer;

/// Subscribers are those connected to a service hosted by this daemon
#[derive(Clone)]
pub struct SubscriberInst {
    pub peer: Peer,

    pub info: SubscriptionInfo,
}

/// Subscriber Manager manages local, delegated, and RPC service Subscribers
#[derive(Clone)]
pub struct SubscriberManager {
    store: Arc<Mutex<HashMap<Id, Vec<SubscriberInst>>>>,
}

impl SubscriberManager {
    pub fn new() -> Self {
        Self{ store: Arc::new(Mutex::new(HashMap::new())) }
    }

    pub fn find(&self, service_id: &Id) -> Result<Vec<SubscriberInst>, Error> {
        let s = self.store.lock().unwrap();
        
        match s.get(service_id) {
            Some(v) => Ok(v.clone()),
            None => Ok(vec![]),
        }
    }

    pub fn update_fn<F: Fn(&mut SubscriberInst)>(&mut self, service_id: &Id, peer_id: &Id, f: F) -> Result<(), Error> {
        unimplemented!()
    }

    fn load() {

    }
    
}
