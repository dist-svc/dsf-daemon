

use std::sync::{Arc, Mutex};
use std::collections::HashMap;

use dsf_core::types::Id;

pub use dsf_rpc::SubscribeInfo;

use crate::store::Store;

pub struct SubscriptionInst {
    pub info: SubscribeInfo,
}

/// Subscription Manager manages local, delegated, and RPC service Subscriptions
pub struct SubscriptionManager {
    store: Arc<Mutex<Store>>,
}

impl SubscriptionManager {
    pub fn new(store: Arc<Mutex<Store>>) -> Self {
        Self{store}
    }

    pub fn find(&self, id: &Id) -> Result<HashMap<Id, SubscriptionInst>, ()> {
        unimplemented!()
    }

    pub fn update(&mut self, id: &Id, Subscription: SubscriptionInst) -> Result<(), ()> {
        unimplemented!()
    }

    fn load() {

    }
    
}
