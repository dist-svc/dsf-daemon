

use std::sync::{Arc, Mutex};


//use dsf_core::prelude::*;

pub use dsf_rpc::SubscribeInfo;

use crate::store::Store;

/// Subscription Manager manages local, delegated, and RPC service subscriptions
pub struct SubscriptionManager {
    store: Arc<Mutex<Store>>,
}

impl SubscriptionManager {
    pub fn new(store: Arc<Mutex<Store>>) -> Self {
        Self{store}
    }


    
}
