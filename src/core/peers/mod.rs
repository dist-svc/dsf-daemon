//! Peer model, information, and map
//! This module is used to provide a single map of PeerManager peers for sharing between DSF components

use crate::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};

use std::collections::HashMap;

use log::{debug, error, info, trace, warn};

use dsf_core::prelude::*;

use crate::store::Store;

pub mod info;
pub use info::{Peer, PeerAddress, PeerInfo, PeerState};

/// PeerManager allows the creation of and provides storage for peer objects.
/// This insures that one shared peer object exists for each PeerManager id
pub struct PeerManager {
    peers: HashMap<Id, Peer>,
    store: Arc<Mutex<Store>>,

    index: usize,
}

impl PeerManager {
    pub fn new(store: Arc<Mutex<Store>>) -> Self {
        let peers = HashMap::new();

        let mut s = Self {
            peers,
            store,
            index: 0,
        };

        s.load();

        s
    }

    pub fn find(&self, id: &Id) -> Option<Peer> {
        self.peers.get(id).map(|p| p.clone())
    }

    pub fn find_or_create(&mut self, id: Id, address: PeerAddress, key: Option<PublicKey>) -> Peer {

        // Update and return existing peer
        if let Some(p) = self.peers.get_mut(&id) {
    
            p.info.update_address(address);
            
            if let Some(k) = key {
                p.info.set_state(PeerState::Known(k))
            }
    
            return p.clone()
        }
    
        // Create new peer
        
        let state = match key {
            Some(k) => PeerState::Known(k),
            None => PeerState::Unknown,
        };

        debug!(
            "Creating new peer instance id: ({:?} addr: {:?}, state: {:?})",
            id, address, state
        );
    
        let index = self.index;
        self.index += 1;
    
        let info = PeerInfo::new(id.clone(), address, state, index, None);
        let peer = Peer { info };
    
    
        // Write to store
        //        let store = self.store.lock().unwrap();
        //        if let Err(e) = store.save_peer(&peer.info) {
        //            error!("Error writing peer {} to db: {:?}", id, e);
        //        }
    
        peer
    }

    pub fn remove(&mut self, id: &Id) -> Option<PeerInfo> {
        trace!("remove peer lock");

        let peer = self.peers.remove(id);

        if let Some(p) = peer {
            let info = p.info();

            trace!("update peer (store) lock");
            if let Err(e) = self.store.lock().unwrap().delete_peer(&info) {
                error!("Error removing peer from db: {:?}", e);
            }

            Some(info)
        } else {
            None
        }
    }

    pub fn count(&self) -> usize {
        trace!("count peer lock");

        self.peers.len()
    }

    pub fn info(&self, id: &Id) -> Option<PeerInfo> {
        self.find(id).map(|p| p.info())
    }

    pub fn list(&self) -> Vec<(Id, Peer)> {
        trace!("list peer lock");

        self.peers
            .iter()
            .map(|(id, p)| (id.clone(), p.clone()))
            .collect()
    }

    pub fn index_to_id(&self, index: usize) -> Option<Id> {
        trace!("index to id peer lock");

        self.peers
            .iter()
            .find(|(_id, p)| p.info.index == index)
            .map(|(id, _s)| id.clone())
    }

    /// Update a peer instance (if found)
    pub fn update<F>(&mut self, id: &Id, mut f: F) -> Option<PeerInfo>
    where
        F: FnMut(&mut Peer),
    {
        match self.peers.get_mut(id) {
            Some(p) => {
                (f)(p);
                Some(p.info())
            }
            None => None,
        }
    }

    /// Fetch a field from a service instance
    pub fn filter<F, R>(&mut self, id: &Id, f: F) -> Option<R>
    where
        F: Fn(&Peer) -> R,
    {
        match self.peers.get(id) {
            Some(p) => Some((f)(&p)),
            None => None,
        }
    }

    pub fn sync(&self) {
        for (id, inst) in self.peers.iter() {
            let info = inst.info();

            if let Err(e) = self.store.lock().unwrap().save_peer(&info) {
                error!("Error writing peer {} to db: {:?}", id, e);
            }
        }
    }

    // Load all peers from store
    fn load(&mut self) {
        trace!("load peers lock");

        trace!("take store lock");
        let store = self.store.lock().unwrap();

        let peer_info: Vec<PeerInfo> = match store.load_peers() {
            Ok(v) => v,
            Err(e) => {
                error!("Error listing files: {:?}", e);
                return;
            }
        };

        for mut info in peer_info {
            info.index = self.index;
            self.index += 1;

            self.peers.entry(info.id.clone()).or_insert(Peer { info });
        }
    }
}
