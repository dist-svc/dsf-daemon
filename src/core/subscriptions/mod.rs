

use std::sync::{Arc, Mutex};
use std::collections::HashMap;

use dsf_core::types::Id;

pub use dsf_rpc::SubscribeInfo;

use crate::core::peers::Peer;
use crate::store::Store;

/// Subscriptions are connections from the daemon to other services
pub struct SubscriptionInst {
    // TODO: info
    pub info: (),

    // TODO: peers
    pub peer_id: Id,
}

/// Subscription Manager manages local, delegated, and RPC service Subscriptions
#[derive(Clone)]
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

    pub fn update_fn<F: Fn(&mut SubscriptionInst)>(&mut self, service_id: &Id, peer_id: &Id, f: F) -> Result<(), ()> {
        unimplemented!()
    }

    fn load() {

    }
    
}
