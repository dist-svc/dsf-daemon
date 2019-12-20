
use std::sync::{Arc, RwLock};
use std::time::{SystemTime};
use std::fmt;

use colored::*;

use dsf_core::prelude::*;

/// Peer object used to store peer related state and information
/// This is a global singleton representing a known peer and may be shared
/// between components of the system
#[derive(Clone, Debug)]
pub struct Peer {
    pub(super) info: Arc<RwLock<PeerInfo>>,
}

impl PartialEq for Peer {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.info, &other.info)
    }
}

unsafe impl Send for Peer {}

impl Peer {
    pub fn id(&self) -> Id {
        let info = &self.info.read().unwrap();
        info.id.clone()
    }

    pub fn address(&self) -> Address {
        let info = &self.info.read().unwrap();
        info.address().clone()
    }

    pub fn state(&self) -> PeerState {
        let info = &self.info.read().unwrap();
        info.state().clone()
    }

    pub fn seen(&self) -> Option<SystemTime> {
        let info = self.info.read().unwrap();
        info.seen().clone()
    }

    pub fn set_seen(&self, seen: SystemTime) {
        let mut info = self.info.write().unwrap();
        info.set_seen(seen);
    }

    pub fn info(&self) -> PeerInfo {
        let info = self.info.read().unwrap();
        info.clone()
    }

    pub fn update<F>(&mut self, mut f: F) 
    where F: FnMut(&mut PeerInfo)
    {
        let mut info = &mut self.info.write().unwrap();

        f(&mut info);
    }

    pub fn pub_key(&self) -> Option<PublicKey> {
        let info = &self.info.write().unwrap();
        match info.state() {
            PeerState::Known(pk) => Some(pk.clone()),
            _ => None,
        }
    }
}


/// PeerInfo object for storage and exchange of peer information
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PeerInfo {
    pub id: Id,
    pub address: PeerAddress,
    pub state: PeerState,
    pub seen: Option<SystemTime>,

    pub sent: u64,
    pub received: u64,
}


/// PeerState defines the state of a peer
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PeerState {
    /// A peer that has not been contacted exists in the Unknown state
    Unknown,
    /// Once public keys have been exchanged this moces to the Known state
    Known(PublicKey),
    
    //Peered(Service),
}

/// PeerState defines the state of a peer
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PeerAddress {
    /// Implicit address
    Implicit(Address),
    /// Explicit / requested address
    Explicit(Address),
}

impl PeerInfo {
    pub fn new(id: Id, address: PeerAddress, state: PeerState, seen: Option<SystemTime>) -> Self {
        Self{id, address, state, seen, sent: 0, received: 0}
    }

    /// Fetch the address of a peer
    pub fn address(&self) -> &Address {
        match &self.address {
            PeerAddress::Explicit(e) => e,
            PeerAddress::Implicit(i) => i
        }
    }

    pub fn update_address(&mut self, addr: PeerAddress){
        use PeerAddress::*;

        match (&self.address, &addr) {
            (_, Explicit(_)) => self.address = addr,
            (Implicit(_), Implicit(_)) => self.address = addr,
            _ => (),
        }
    }

    /// Fetch the state of a peer
    pub fn state(&self) -> &PeerState {
        &self.state
    }

    // Set the state of a peer
    pub fn set_state(&mut self, state: PeerState) {
        self.state = state;
    }

    pub fn seen(&self) -> Option<SystemTime> {
        self.seen
    }

    pub fn set_seen(&mut self, seen: SystemTime) {
        self.seen = Some(seen);
    }
}

impl fmt::Display for PeerInfo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {

        if f.sign_plus() {
            write!(f, "id: {}", self.id)?;
        } else {
            write!(f, ", {}", self.id)?;
        }

        if f.sign_plus() {
            write!(f, "\n  - address: {}", self.address())?;
        } else {
            write!(f, "{}", self.address())?;
        }
        
        if f.sign_plus() {
            write!(f, "\n  - state: {}", self.state)?;
        } else {
            write!(f, ", {}", self.state)?;
        }

        if let Some(seen) = self.seen {
            let dt: chrono::DateTime<chrono::Local> = chrono::DateTime::from(seen);
            let ht = chrono_humanize::HumanTime::from(dt);

            if f.sign_plus() {
                write!(f, "\n  - last seen: {}", ht)?;
            } else {
                write!(f, ", {}", ht)?;
            }
        }

        if f.sign_plus() {
            write!(f, "\n  - sent: {}, received: {}", self.sent, self.received)?;
        } else {
            write!(f, ", {}, {}", self.sent, self.received)?;
        }

        Ok(())
    }
}


impl fmt::Display for PeerState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if f.sign_plus() {
            write!(f, "state: ")?;
        }

        let s = match self {
            PeerState::Unknown => "unknown".red(),
            PeerState::Known(_) =>   "known  ".green(),
        };

        write!(f, "{}", s)?;

        Ok(())
    }
}

