
//! Peer model, information, and map
//! This module is used to provide a single map of PeerManager peers for sharing between DSF components

use std::sync::{Arc, Mutex, RwLock};

use std::collections::HashMap;


use dsf_core::prelude::*;

use crate::store::Store;
use super::store::FileStore;

pub mod info;
pub use info::{Peer, PeerInfo, PeerAddress, PeerState};

/// PeerManager allows the creation of and provides storage for peer objects.
/// This insures that one shared peer object exists for each PeerManager id
#[derive(Clone)]
pub struct PeerManager {
    peers: Arc<Mutex<HashMap<Id, Peer>>>,
    store: Arc<Mutex<Store>>,
}

impl PeerManager {
    pub fn new(store: Arc<Mutex<Store>>) -> Self {
        let peers = HashMap::new();

        let mut s = Self{
            peers: Arc::new(Mutex::new(peers)), 
            store,
        };

        s.load();

        s
    }

    pub fn find(&self, id: &Id) -> Option<Peer> {
        let peers = self.peers.lock().unwrap();
        peers.get(id).map(|p| p.clone() )
    }

    pub fn find_or_create(&mut self, id: Id, address: PeerAddress, key: Option<PublicKey>) -> Peer {
        let mut peers = self.peers.lock().unwrap();
        let store = self.store.lock().unwrap();

        peers.entry(id.clone()).or_insert_with(|| {
            debug!("Creating new peer instance id: ({:?} addr: {:?}, key: {:?})", id, address, key);
            
            let state = match key {
                Some(k) => PeerState::Known(k),
                None => PeerState::Unknown,
            };

            let info = PeerInfo::new(id, address, state, None);

            if let Err(e) = store.save_peer(&info) {
                error!("Error writing peer {} to db: {:?}", id, e);
            }

            Peer{ info: Arc::new(RwLock::new(info)) }
        }).clone()
    }

    pub fn count(&self) -> usize {
        let peers = self.peers.lock().unwrap();
        peers.len()
    }

    pub fn info(&self, id: &Id) -> Option<PeerInfo> {
        self.find(id).map(|p| p.info() )
    }

    pub fn list(&self) -> Vec<(Id, Peer)> {
        let peers = self.peers.lock().unwrap();
        peers.iter().map(|(id, p)| (id.clone(), p.clone()) ).collect()
    }

    pub fn sync(&self) {
        let peers = self.peers.lock().unwrap();
        let mut store = self.store.lock().unwrap();

        for (id, inst) in peers.iter() {
            let info = inst.info();

            if let Err(e) = store.save_peer(&info) {
                error!("Error writing peer {} to db: {:?}", info.id, e);
            }
        }
    }

    pub fn load(&mut self) {
        let mut store = self.store.lock().unwrap();
        let mut peers = self.peers.lock().unwrap();

        let peer_info = match store.load_peers() {
            Ok(v) => v,
            Err(e) => {
                error!("Error listing files: {:?}", e);
                return
            }
        };

        for p in peer_info {
            peers.entry(p.id).or_insert(Peer{info: Arc::new(RwLock::new(p))});
        }
    }
}
