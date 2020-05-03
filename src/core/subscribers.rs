use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use futures::channel::mpsc;

use dsf_core::types::Id;

pub use dsf_rpc::{SubscriptionInfo, SubscriptionKind};

use crate::error::Error;

// TODO: isolate unixmessage from core subscriber somehow
use crate::io::unix::UnixMessage;

/// Subscribers are those connected to a service hosted by this daemon
#[derive(Clone)]
pub struct SubscriberInst {
    pub info: SubscriptionInfo,
}

#[derive(Clone, Debug)]
pub struct UnixSubscriber {
    pub connection_id: u32,
    pub sender: mpsc::Sender<UnixMessage>,
}

/// Subscriber Manager manages local, delegated, and RPC service Subscribers
#[derive(Clone)]
pub struct SubscriberManager {
    store: Arc<Mutex<HashMap<Id, Vec<SubscriberInst>>>>,
}

impl SubscriberManager {
    pub fn new() -> Self {
        Self {
            store: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Fetch subscribers for a given service
    pub fn find(&self, service_id: &Id) -> Result<Vec<SubscriberInst>, Error> {
        let s = self.store.lock().unwrap();

        match s.get(service_id) {
            Some(v) => Ok(v.clone()),
            None => Ok(vec![]),
        }
    }

    /// Update a specified peer subscription
    pub fn update_peer<F: Fn(&mut SubscriberInst)>(
        &mut self,
        service_id: &Id,
        peer_id: &Id,
        f: F,
    ) -> Result<(), Error> {
        let mut store = self.store.lock().unwrap();

        let subscribers = store.entry(service_id.clone()).or_insert(vec![]);

        // Find subscriber in list
        let mut subscriber = subscribers.iter_mut().find(|s| {
            if let SubscriptionKind::Peer(i) = &s.info.kind {
                return i == peer_id;
            }
            return false;
        });

        // Create new subscriber if not found
        if let None = subscriber {
            let s = SubscriberInst {
                info: SubscriptionInfo {
                    service_id: service_id.clone(),
                    kind: SubscriptionKind::Peer(peer_id.clone()),

                    updated: Some(SystemTime::now()),
                    expiry: None,
                },
            };

            subscribers.push(s);

            let n = subscribers.len();
            subscriber = Some(&mut subscribers[n - 1]);
        }

        // Call update function
        if let Some(mut s) = subscriber {
            f(&mut s);
        }

        Ok(())
    }

    /// Update a specified socket subscription
    pub fn update_socket<F: Fn(&mut SubscriberInst)>(
        &mut self,
        service_id: &Id,
        socket_id: u32,
        f: F,
    ) -> Result<(), Error> {
        let mut store = self.store.lock().unwrap();

        let subscribers = store.entry(service_id.clone()).or_insert(vec![]);

        let mut subscriber = subscribers.iter_mut().find(|s| {
            if let SubscriptionKind::Socket(i) = &s.info.kind {
                return *i == socket_id;
            }
            return false;
        });

        // Create new subscriber if not found
        if let None = subscriber {
            let s = SubscriberInst {
                info: SubscriptionInfo {
                    service_id: service_id.clone(),
                    kind: SubscriptionKind::Socket(socket_id),

                    updated: Some(SystemTime::now()),
                    expiry: None,
                },
            };

            subscribers.push(s);

            let n = subscribers.len();
            subscriber = Some(&mut subscribers[n - 1]);
        }

        if let Some(mut s) = subscriber {
            f(&mut s);
        }

        Ok(())
    }
}

// TODO: write tests for this
