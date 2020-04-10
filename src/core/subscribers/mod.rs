

use std::sync::{Arc, Mutex};
use std::collections::HashMap;

use dsf_core::types::Id;

pub use dsf_rpc::SubscribeInfo;

use crate::error::Error;
use crate::store::Store;

use crate::core::peers::Peer;

/// Subscribers are those connected to a service hosted by this daemon
pub struct SubscriberInst {
    pub peer: Peer,

    // SubscriberInfo
}

/// Subscriber Manager manages local, delegated, and RPC service Subscribers
#[derive(Clone)]
pub struct SubscriberManager {
    store: Arc<Mutex<Store>>,
}

impl SubscriberManager {
    pub fn new(store: Arc<Mutex<Store>>) -> Self {
        Self{store}
    }

    pub fn find(&self, id: &Id) -> Result<HashMap<Id, SubscriberInst>, Error> {
        unimplemented!()
    }

    pub fn update(&mut self, id: &Id, Subscriber: SubscriberInst) -> Result<(), Error> {
        unimplemented!()
    }

    fn load() {

    }
    
}
