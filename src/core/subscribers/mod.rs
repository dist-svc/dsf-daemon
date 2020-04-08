

use std::sync::{Arc, Mutex};
use std::collections::HashMap;

use dsf_core::types::Id;

pub use dsf_rpc::SubscribeInfo;

use crate::store::Store;

pub struct SubscriberInst {
    pub info: SubscribeInfo,
}

/// Subscriber Manager manages local, delegated, and RPC service Subscribers
pub struct SubscriberManager {
    store: Arc<Mutex<Store>>,
}

impl SubscriberManager {
    pub fn new(store: Arc<Mutex<Store>>) -> Self {
        Self{store}
    }

    pub fn find(&self, id: &Id) -> Result<HashMap<Id, SubscriberInst>, ()> {
        unimplemented!()
    }

    pub fn update(&mut self, id: &Id, Subscriber: SubscriberInst) -> Result<(), ()> {
        unimplemented!()
    }

    fn load() {

    }
    
}
