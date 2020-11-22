//! Services module maintains the service database
//!
//!

use std::collections::HashMap;
use crate::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use dsf_core::prelude::*;
use dsf_core::service::Subscriber;

pub use dsf_rpc::service::{ServiceInfo, ServiceState};

use crate::store::Store;

pub mod inst;
pub use inst::ServiceInst;

pub mod data;
pub use data::Data;

/// ServiceManager keeps track of local and remote services
#[derive(Clone)]
pub struct ServiceManager {
    pub(crate) services: Arc<Mutex<HashMap<Id, ServiceInst>>>,
    store: Arc<Mutex<Store>>,
}

impl ServiceManager {
    /// Create a new service manager instance
    pub fn new(store: Arc<Mutex<Store>>) -> Self {
        let services = HashMap::new();

        let s = Self {
            services: Arc::new(Mutex::new(services)),
            store,
        };

        s.load();

        s
    }

    /// Register a local service for tracking
    pub fn register(
        &mut self,
        service: Service,
        primary_page: &Page,
        state: ServiceState,
        updated: Option<SystemTime>,
    ) -> Result<ServiceInfo, DsfError> {
        let services = self.services.clone();
        trace!("service register lock");
        let mut services = services.lock().unwrap();

        let id = service.id();

        // Create a service instance wrapper
        let inst = ServiceInst {
            service,
            state,
            index: services.len(),
            last_updated: updated,
            primary_page: Some(primary_page.clone()),
            replica_page: None,
            changed: true,
        };

        let info = inst.info();

        // Write new instance to disk
        self.sync_inst(&inst);

        // Insert into storage
        services.insert(id, inst);

        trace!("service register done");

        Ok(info)
    }

    /// Determine whether a service is known in the database
    pub fn known(&self, id: &Id) -> bool {
        trace!("services known lock");
        let services = self.services.lock().unwrap();
        services.contains_key(id)
    }

    /// Fetch service information for a given service id from the manager
    pub fn find(&self, id: &Id) -> Option<ServiceInfo> {
        trace!("services find lock");
        let services = self.services.lock().unwrap();

        let service = match services.get(id) {
            Some(v) => v,
            None => return None,
        };

        Some(service.info())
    }

    /// Fetch a service by local index
    pub fn index(&self, index: usize) -> Option<ServiceInfo> {
        let services = self.services.clone();
        trace!("services index lock");
        let services = services.lock().unwrap();
        services
            .iter()
            .find(|(_id, s)| s.index == index )
            .map(|(_id, s)| s.info() )
    }

    pub fn index_to_id(&self, index: usize) -> Option<Id> {
        let services = self.services.clone();
        trace!("services index to id lock");
        let services = services.lock().unwrap();
        services
            .iter()
            .find(|(_id, s)| s.index == index)
            .map(|(id, _s)| id.clone())
    }

    pub fn remove(&self, id: &Id) -> Result<Option<ServiceInfo>, DsfError> {
        // Remove from memory
        trace!("services remove lock");
        let service = { self.services.lock().unwrap().remove(id) };

        // Remove from database
        if let Some(s) = service {
            let info = s.info();
            // DEADLOCK?
            //let _ = self.store.lock().unwrap().delete_service(&info);

            return Ok(Some(info));
        };

        return Ok(None);
    }

    /// Fetch data for a given service
    #[cfg(nope)]
    pub fn data(&self, id: &Id, n: usize) -> Result<Vec<DataInfo>, DsfError> {
        let service = match self.find(id) {
            Some(s) => s,
            None => return Err(DsfError::UnknownService),
        };

        let s = service;

        let d = s.get_data(n).map(|d| DataInfo {
            service: id.clone(),
            index: d.version(),
            body: d.body().clone(),
            previous: None,
            signature: d.signature().unwrap(),
        });

        Ok(d.collect())
    }

    /// Update a service instance (if found)
    pub fn update_inst<F>(&mut self, id: &Id, mut f: F) -> Option<ServiceInfo>
    where
        F: FnMut(&mut ServiceInst),
    {
        trace!("services update inst");
        let mut services = self.services.lock().unwrap();

        match services.get_mut(id) {
            Some(svc) => {
                (f)(svc);
                svc.changed = true;
                Some(svc.info())
            }
            None => None,
        }
    }

    pub fn validate_pages(&mut self, id: &Id, pages: &[Page]) -> Result<(), DsfError> {
        let mut services = self.services.lock().unwrap();
        let service_inst = match services.get_mut(id) {
            Some(s) => s,
            None => return Err(DsfError::UnknownService),
        };

        for p in pages {
            service_inst.service().validate_page(p)?;
        }

        Ok(())
    }

    /// Fetch a field from a service instance
    pub fn filter<F, R>(&mut self, id: &Id, f: F) -> Option<R> 
    where
        F: Fn(&ServiceInst) -> R,
    {
        trace!("services filter inst");
        let services = self.services.lock().unwrap();

        match services.get(id) {
            Some(s) => {
                let svc = s;
                Some((f)(&svc))
            }
            None => None,
        }
    }

    /// Fetch services with a matching state requiring update
    pub fn updates_required(
        &mut self,
        state: ServiceState,
        interval: Duration,
        force: bool,
    ) -> Vec<ServiceInfo> {
        let services = self.services.lock().unwrap();

        let updates: Vec<_> = services
            .iter()
            .filter_map(|(_id, svc)| {
                let i = svc;
                match i.update_required(state, interval, force) {
                    true => Some(i.info()),
                    false => None,
                }
            })
            .collect();

        trace!("updates required (state: {:?}): {:?}", state, updates);

        updates
    }

    /// Sync a service instance to disk
    pub(crate) fn sync_inst(&mut self, inst: &ServiceInst) {
        trace!("service sync inst");

        let store = self.store.lock().unwrap();

        if let Err(e) = store.save_service(&inst.info()) {
            error!("Error writing service instance {}: {:?}", inst.id(), e);
        }

        if let Some(p) = &inst.primary_page {
            store.save_page(p).unwrap();
        }

        if let Some(p) = &inst.replica_page {
            store.save_page(p).unwrap();
        }
    }

    /// Fetch a list of information for known services
    pub fn list(&self) -> Vec<ServiceInfo> {
        let services = self.services.lock().unwrap();

        services
            .iter()
            .map(|(_k, v)| v.info())
            .collect()
    }

    /// Fetch the number of known services
    pub fn count(&self) -> usize {
        let services = self.services.lock().unwrap();
        services.len()
    }

    /// Sync the service database to disk
    pub fn sync(&self) {
        trace!("services sync");

        let mut services = self.services.lock().unwrap();
        let store = self.store.lock().unwrap();

        for (id, inst) in services.iter_mut() {
            // Skip unchanged instances
            if !inst.changed {
                continue;
            }

            if let Err(e) = store.save_service(&inst.info()) {
                error!("Error writing service instance {}: {:?}", id, e);
            }

            if let Some(p) = &inst.primary_page {
                store.save_page(p).unwrap();
            }

            if let Some(p) = &inst.replica_page {
                store.save_page(p).unwrap();
            }

            inst.changed = false;
        }
    }

    /// Load the service database from disk
    pub fn load(&self) {
        trace!("services load");
        
        let store = self.store.lock().unwrap();
        let mut services = self.services.lock().unwrap();

        let service_info = store.load_services().unwrap();

        debug!("Loading {} services from database", service_info.len());

        for i in service_info {
            let public_key = i.public_key.clone();

            let primary_page = match i.primary_page {
                Some(p) => store
                    .load_page(&p, Some(public_key.clone()))
                    .unwrap()
                    .unwrap(),
                None => {
                    trace!("No primary page for service: {:?}", i);
                    continue;
                }
            };

            let replica_page = i
                .replica_page
                .map(|s| store.load_page(&s, Some(public_key.clone())).unwrap())
                .flatten();

            let mut service = Service::load(&primary_page).unwrap();

            service.set_private_key(i.private_key);
            service.set_secret_key(i.secret_key);

            // URGENT TODO: rehydrate other components here
            // TODO: maybe replicas and subscribers should not be stored against the service inst but in separate core modules?
            let s = ServiceInst {
                service,
                state: i.state,
                index: i.index,

                last_updated: i.last_updated,
                primary_page: Some(primary_page),
                replica_page,

                changed: false,
            };

            services.entry(i.id).or_insert(s);
        }
    }
}

impl Drop for ServiceManager {
    fn drop(&mut self) {
        // TODO: sync..?
    }
}
