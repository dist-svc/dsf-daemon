use std::ops::Add;
use std::time::{Duration, SystemTime};

use log::{debug, error, warn};

use diesel::Queryable;
use serde::{Deserialize, Serialize};

use dsf_core::options::Options;
use dsf_core::prelude::*;
use dsf_core::service::{Publisher, Subscriber, SecondaryOptions};

use dsf_rpc::service::{ServiceInfo, ServiceState};

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
    pub(crate) changed: bool,
}

impl ServiceInst {
    pub(crate) fn id(&self) -> Id {
        self.service.id()
    }

    pub(crate) fn service(&mut self) -> &mut Service {
        &mut self.service
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
            // TODO: fix replica / subscriber info (split objects?)
            replicas: 0,
            subscribers: 0,
            origin: service.is_origin(),
            subscribed: false,
        }
    }

    /// Publish a service, creating a new primary page
    pub(crate) fn publish(&mut self, force_update: bool) -> Result<Page, DsfError> {
        // Check if there's an existing page
        if let Some(page) = &self.primary_page {
            let (issued, expiry): (Option<SystemTime>, Option<SystemTime>) = (
                page.issued().map(|v| v.into()),
                page.expiry().map(|v| v.into()),
            );

            // Fetch expiry time
            let expired = match (expiry, issued) {
                (Some(expiry), _) => SystemTime::now() > expiry,
                (_, Some(issued)) => SystemTime::now() > issued.add(Duration::from_secs(3600)),
                _ => {
                    warn!("Page does not contain expiry or issued fields");
                    // TODO: fault out here
                    false
                }
            };

            // If it hasn't expired, use this one
            if !expired && !force_update {
                debug!("Using existing service page");
                return Ok(page.clone());
            }
        }

        // Check the private key exists for signing the primary page
        let _private_key = match self.service.private_key() {
            Some(s) => s,
            None => {
                error!("no service private key (id: {})", self.service.id());
                return Err(DsfError::NoPrivateKey);
            }
        };

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

    /// Replicate a service, creating a new replica page
    pub(crate) fn replicate(
        &mut self,
        peer_service: &mut Service,
        force_update: bool,
    ) -> Result<Page, DsfError> {
        let mut version = 0;

        // Check if there's an existing page
        if let Some(page) = &self.replica_page {
            let (issued, expiry): (Option<SystemTime>, Option<SystemTime>) = (
                page.issued().map(|v| v.into()),
                page.expiry().map(|v| v.into()),
            );

            // Fetch expiry time
            let expired = match (issued, expiry) {
                (_, Some(expiry)) => SystemTime::now() < expiry,
                (Some(issued), None) => SystemTime::now() < issued.add(Duration::from_secs(3600)),
                _ => false,
            };
            // If it hasn't expired, use this one
            if !expired && !force_update {
                return Ok(page.clone());
            }

            version = page.header().index();
        }

        let opts = SecondaryOptions {
            page_kind: PageKind::Replica.into(),
            version: version,
            public_options: &[Options::public_key(peer_service.public_key())],
            ..Default::default()
        };

        let mut buff = vec![0u8; 1024];
        let (n, mut replica_page) = peer_service
            .publish_secondary(&self.service.id(), opts, &mut buff)
            .unwrap();
        replica_page.raw = Some(buff[..n].to_vec());

        // Update local replica page
        self.replica_page = Some(replica_page.clone());
        self.changed = true;

        Ok(replica_page)
    }

    /// Apply an updated service page
    pub(crate) fn apply_update(&mut self, page: &Page) -> Result<bool, DsfError> {
        let changed = self.service.apply_primary(page)?;

        // TODO: mark update required

        Ok(changed)
    }

    pub fn update<F>(&mut self, f: F)
    where
        F: Fn(&mut ServiceInst),
    {
        (f)(self);
        self.changed = true;
    }

    pub(crate) fn update_required(
        &self,
        state: ServiceState,
        update_interval: Duration,
        force: bool,
    ) -> bool {
        // Filter for the specified service state
        if self.state != state {
            return false;
        }

        // Skip checking further if force is set
        if force {
            return true;
        }

        // If we've never updated them, definitely required
        let updated = match self.last_updated {
            Some(u) => u,
            None => return true,
        };

        // Otherwise, only if update time has expired
        if updated.add(update_interval) < SystemTime::now() {
            return true;
        }

        false
    }
}
