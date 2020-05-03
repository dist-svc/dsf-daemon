use std::sync::{Arc, RwLock};
use std::time::SystemTime;

use dsf_core::prelude::*;
pub use dsf_rpc::peer::{PeerAddress, PeerInfo, PeerState};

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
    where
        F: FnMut(&mut PeerInfo),
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

#[cfg(disabled)]
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

#[cfg(disabled)]
impl fmt::Display for PeerState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if f.sign_plus() {
            write!(f, "state: ")?;
        }

        let s = match self {
            PeerState::Unknown => "unknown".red(),
            PeerState::Known(_) => "known  ".green(),
        };

        write!(f, "{}", s)?;

        Ok(())
    }
}
