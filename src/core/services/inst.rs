use std::collections::HashMap;

use std::time::{SystemTime, Duration};
use std::ops::Add;

use dsf_core::prelude::*;
use dsf_core::service::{Subscriber, Publisher};
use dsf_core::service::publisher::SecondaryOptionsBuilder;
use dsf_core::options::Options;

use dsf_rpc::service::{ServiceInfo, ServiceState};


use crate::core::peers::Peer;
use crate::core::replicas::Replica;

use super::SubscriptionInfo;
use super::data::Data;

#[derive(Debug, Serialize, Deserialize, Queryable)]
pub struct ServiceInst {
    pub(crate) service: Service,
    pub(crate) state: ServiceState,
    pub(crate) index: usize,
    pub(crate) last_updated: Option<SystemTime>,

    // TODO: this isn't really optional / should always exist?
    #[serde(skip)]
    pub(crate) primary_page: Option<Page>,

    #[serde(skip)]
    pub(crate) replica_page: Option<Page>,

    #[serde(skip)]
    pub (crate) replicas: HashMap<Id, Replica>,

    #[serde(skip)]
    pub(crate) subscribers: HashMap<Id, SubscriptionInfo>,

    pub(crate) data: Vec<Data>,

    #[serde(skip)]
    pub(crate) changed: bool,
}


impl ServiceInst {
    pub(crate) fn id(&self) -> Id {
        self.service.id()
    }

    pub(crate) fn info(&self) -> ServiceInfo {
        let service = &self.service;

        ServiceInfo {
            id: service.id(),
            index: self.index,
            state: self.state,
            last_updated: self.last_updated,
            primary_page: self.primary_page.as_ref().map(|v| v.signature()).flatten(),
            replica_page: self.replica_page.as_ref().map(|v| v.signature()).flatten(),
            public_key: service.public_key(),
            private_key: service.private_key(),
            secret_key: service.secret_key(),
            replicas: self.replicas.len(),
            subscribers: self.subscribers.len(),
            origin: service.is_origin(),
        }
    }

    #[cfg(nope)]
    pub(crate) fn add_data(&mut self, page: &Page) -> Result<(), DsfError> {
        // Validate page (assuming sig has been pre-verified)
        self.service.validate_page(page)?;

        if let Some(raw) = page.raw() {
            debug!("Using raw data object");
            self.data.push(Data::from(&raw[..]));

        } else {
            debug!("Encoding data object");
            // Encode base object for storage
            let mut b: Base = page.into();
            let mut d = vec![0u8; 4096];
            let n = b.encode(None, None, &mut d)?;

            self.data.push(Data::from(&d[..n]));
        }

        self.changed = true;

        Ok(())
    }

    #[cfg(nope)]
    pub(crate) fn get_data(&self, n: usize) -> impl Iterator<Item=Page> + '_ {
        let public_key = self.service.public_key();
        let secret_key = self.service.secret_key();

        (&self.data).iter().rev().take(n)
            .filter_map(move |v| {
            let (b, _n) = match Base::parse(&v.0, |_id| Some(public_key), |_id| secret_key ) {
                Ok(b) => b,
                Err(e) => {
                    error!("Error fetching data object: {:?}", e);
                    return None
                }
            };

            let p: Page = match b.try_into() {
                Ok(p) => p,
                Err(e) => {
                    error!("Error converting data object: {:?}", e);
                    return None
                }
            };

            Some(p)
        })
    }

    pub(crate) fn publish(&mut self, force_update: bool) -> Result<Page, DsfError> {
        // Check the private key exists for signing the primary page
        let _private_key = match self.service.private_key() {
            Some(s) => s,
            None => {
                error!("no service private key (id: {})", self.service.id());
                return Err(DsfError::NoPrivateKey)
            }
        };

        // Check if there's an existing page
        if let Some(page) = &self.primary_page {
            // Fetch expiry time
            let expired = match page.expiry() {
                Some(expiry) => SystemTime::now() > expiry,
                None => SystemTime::now() > page.issued().add(Duration::from_secs(3600)),
            };

            // If it hasn't expired, use this one
            if !expired && !force_update {
                debug!("Using existing service page");
                return Ok(page.clone())
            }
        }

        // Generate actual page
        debug!("Generating new service page");
        let mut buff = vec![0u8; 1024];
        let (n, mut primary_page) = self.service.publish_primary(&mut buff).unwrap();
        primary_page.raw = Some(buff[..n].to_vec());
        
        // Update local page version
        self.primary_page = Some(primary_page.clone());
        self.changed = true;

        Ok(primary_page)
    }

    pub(crate) fn replicate(&mut self, peer_service: &mut Service, force_update: bool) -> Result<Page, DsfError> {
        let mut version = 0;

        // Check if there's an existing page
        if let Some(page) = &self.replica_page {
            // Fetch expiry time
            let expired = match page.expiry() {
                Some(expiry) => SystemTime::now() < expiry,
                None => SystemTime::now() < page.issued().add(Duration::from_secs(3600)),
            };
            // If it hasn't expired, use this one
            if !expired && !force_update {
                return Ok(page.clone());
            }

            version = page.version();
        }

        let mut opts = SecondaryOptionsBuilder::default();
        opts.id(self.service.id());
        opts.page_kind(PageKind::Replica.into());
        opts.version(version);
        opts.public_options(vec![Options::public_key(peer_service.public_key())]);
        let opts = opts.build().unwrap();

        let mut buff = vec![0u8; 1024];
        let (n, mut replica_page) = peer_service.publish_secondary(opts, &mut buff).unwrap();
        replica_page.raw = Some(buff[..n].to_vec());

        // Update local replica page
        self.replica_page = Some(replica_page.clone());
        self.changed = true;

        Ok(replica_page)
    }

    pub(crate) fn sync_replicas(&mut self, replicas: &[Replica]) {
        use std::collections::hash_map::Entry::{Vacant, Occupied};
        info!("syncing {} replicas", replicas.len());

        for r in replicas {
            match self.replicas.entry(r.id) {
                Vacant(v) => {
                    v.insert(r.clone());
                },
                Occupied(mut o) => {
                    let o = o.get_mut();
                    if o.version < r.version {
                        o.version = r.version;
                        o.issued = r.issued;
                        o.expiry = r.expiry;
                        o.active = r.active;
                    }
                }
            }
        }
    }

    pub fn update<F>(&mut self, f: F)
    where F: Fn(&mut ServiceInst) {
        (f)(self);
        self.changed = true;
    }

    pub(crate) fn update_replica<F>(&mut self, id: Id, f: F)
    where F: Fn(&mut Replica) {
        use std::collections::hash_map::Entry::{Occupied};

        if let Occupied(mut o) = self.replicas.entry(id) {
            f(o.get_mut());
        }

    }

    pub(crate) fn update_subscription(&mut self, _id: Id, peer: Peer, updated: SystemTime, expiry: SystemTime) {
        use std::collections::hash_map::Entry::{Vacant, Occupied};

        match self.subscribers.entry(peer.id()) {
            Vacant(v) => {
                v.insert(SubscriptionInfo{ peer, updated, expiry });
            },
            Occupied(mut o) => {
                let mut s = o.get_mut();
                // TODO: check bounds here
                s.expiry = expiry;
                s.updated = updated;
            }
        }

        self.changed = true;
    }

    pub(crate) fn update_required(&self, state: ServiceState, update_interval: Duration, force: bool) -> bool {
        // Filter for the specified service state
        if self.state != state {
            return false
        }

        // Skip checking further if force is set
        if force {
            return true
        }

        // If we've never updated them, definitely required
        let updated = match self.last_updated {
            Some(u) => u,
            None => return true,
        };

        // Otherwise, only if update time has expired
        if updated.add(update_interval) < SystemTime::now() {
            return true
        }
        
        return false
    }
}