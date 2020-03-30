

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{SystemTime, Duration};


use dsf_core::prelude::*;

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
