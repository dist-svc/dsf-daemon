
//! Peer model, information, and map
//! This module is used to provide a single map of PeerManager peers for sharing between DSF components

use std::sync::{Arc, Mutex, RwLock};

use std::collections::HashMap;


use dsf_core::prelude::*;

use super::store::FileStore;

pub mod info;
pub use info::{Peer, PeerInfo, PeerAddress, PeerState};

/// PeerManager allows the creation of and provides storage for peer objects.
/// This insures that one shared peer object exists for each PeerManager id
#[derive(Clone)]
pub struct PeerManager {
    peers: Arc<Mutex<HashMap<Id, Peer>>>,
    store: Arc<Mutex<FileStore>>,
}

impl PeerManager {
    pub fn new(database_path: &str) -> Self {
        let peers = HashMap::new();
        let store = FileStore::new(database_path);

        let _ = store.create_dir();

        let mut s = Self{
            peers: Arc::new(Mutex::new(peers)), 
            store: Arc::new(Mutex::new(store)),
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
        let store = self.store.clone();

        peers.entry(id.clone()).or_insert_with(|| {
            debug!("Creating new peer instance id: ({:?} addr: {:?}, key: {:?})", id, address, key);
            
            let state = match key {
                Some(k) => PeerState::Known(k),
                None => PeerState::Unknown,
            };

            let info = PeerInfo::new(id, address, state, None);

            let name = format!("{}.json", id);
            trace!("Writing to peer file: {}", &name);
            if let Err(e) = store.lock().unwrap().store(&name, &info) {
                error!("Error writing peer file: {}, {:?}", name, e);
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

            let name = format!("{}.json", id);
            info!("Writing to peer file: {}", &name);
            if let Err(e) = store.store(&name, &info) {
                error!("Error writing peer file: {}, {:?}", name, e);
            }
        }
    }

    pub fn load(&mut self) {
        let mut store = self.store.lock().unwrap();
        let mut peers = self.peers.lock().unwrap();

        let files = match store.list() {
            Ok(v) => v,
            Err(e) => {
                error!("Error listing files: {:?}", e);
                return
            }
        };

        for f in &files {
            match store.load::<PeerInfo, _, _>(f) {
                Ok(v) => {
                    peers.entry(v.id).or_insert(Peer{info: Arc::new(RwLock::new(v))});
                },
                Err(e) => {
                    error!("Error loading service file: {:?}", e);
                }
            }
        }
    }
}
