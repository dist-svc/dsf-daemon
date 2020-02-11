//! Services module maintains the service database
//! 
//! 

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{SystemTime, Duration};


use dsf_core::prelude::*;
pub use dsf_rpc::service::{ServiceInfo, ServiceState};

use super::peers::Peer;
use super::data::DataInfo;

use crate::store::Store;

pub mod inst;
pub use inst::{ServiceInst};

pub mod data;
pub use data::Data;

/// ServiceManager keeps track of local and remote services
#[derive(Clone)]
pub struct ServiceManager {
    pub(crate) services: Arc<Mutex<HashMap<Id, Arc<RwLock<ServiceInst>>>>>,
    store: Arc<Mutex<Store>>,
}

#[derive(Clone, Debug)]
pub struct SubscriptionInfo {
    pub(crate) peer: Peer,

    pub(crate) updated: SystemTime,
    pub(crate) expiry: SystemTime,
}

impl ServiceManager {
    /// Create a new service manager instance
    pub fn new(store: Arc<Mutex<Store>>) -> Self {
        let services = HashMap::new();

        let s = Self{
            services: Arc::new(Mutex::new(services)), 
            store,
        };

        s.load();

        s
    }

    /// Register a local service for tracking
    pub fn register(&mut self, service: Service, page: &Page, state: ServiceState, updated: Option<SystemTime>) -> Result<Arc<RwLock<ServiceInst>>, DsfError> {
        let services = self.services.clone();
        let mut services = services.lock().unwrap();

        let id = service.id();

        // Create a service instance wrapper
        let mut inst = ServiceInst{service, state,
            index: services.len(),
            last_updated: updated,
            primary_page: Some(page.clone()),
            replica_page: None,
            data: Vec::new(),
            replicas: HashMap::new(),
            subscribers: HashMap::new(),
            changed: true,
        };

        inst.add_data(page)?;

        // Write new instance to disk
        self.sync_inst(&inst);

        let inst = Arc::new(RwLock::new(inst));
        services.insert(id, inst.clone());

        Ok(inst)
    }

    /// Determine whether a service is known in the database
    pub fn known(&self, id: &Id) -> bool {
        let services = self.services.clone();
        let services = services.lock().unwrap();
        services.contains_key(id)
    }

    /// Fetch service information for a given service id from the manager
    pub fn find(&self, id: &Id) -> Option<Arc<RwLock<ServiceInst>>> {
        let services = self.services.clone();
        let services = services.lock().unwrap();

        let service = match services.get(id) {
            Some(v) => v,
            None => return None,
        };

        let lockable = match service.try_write() {
            Ok(_l) => true,
            Err(_e) => false,
        };

        trace!("FIND id: {:?} lockable: {:?}", id, lockable);

        Some(service.clone())
    }

    /// Fetch a service by local index
    pub fn index(&self, index: usize) -> Option<Arc<RwLock<ServiceInst>>> {
        let services = self.services.clone();
        let services = services.lock().unwrap();
        services.iter().find(|(_id, s)| {
            s.read().unwrap().index == index
        }).map(|(_id, s)| s.clone() )
    }

    pub fn index_to_id(&self, index: usize) -> Option<Id> {
        let services = self.services.clone();
        let services = services.lock().unwrap();
        services.iter().find(|(_id, s)| {
            s.read().unwrap().index == index
        }).map(|(id, _s)| id.clone() )
    }

    /// Fetch info for a given service
    pub fn info(&self, id: &Id) -> Option<ServiceInfo> {
        match self.find(id) {
            Some(s) => {
                Some(s.read().unwrap().info())
            },
            None => None,
        }
    }

    /// Fetch data for a given service
    pub fn data(&self, id: &Id, n: usize) -> Result<Vec<DataInfo>, DsfError> {
        let service = match self.find(id) {
            Some(s) => s,
            None => return Err(DsfError::UnknownService),
        };

        let s = service.read().unwrap();

        let d = s.get_data(n).map(|d| {
            DataInfo{
                service: id.clone(),
                index: d.version(),
                body: d.body().clone(),
                previous: None,
                signature: d.signature().unwrap(),
            }
        });

        Ok(d.collect())
    }

    /// Update a service instance (if found)
    pub fn update_inst<F>(&mut self, id: &Id, f: F) -> Option<ServiceInfo>
    where F: Fn(&mut ServiceInst) {
        match self.find(id) {
            Some(s) => {
                let mut svc = s.write().unwrap();
                (f)(&mut svc);
                svc.changed = true;
                Some(svc.info())
            },
            None => None,
        }
    }

    /// Fetch services with a matching state requiring update
    pub fn updates_required(&mut self, state: ServiceState, interval: Duration, force: bool) -> Vec<ServiceInfo> {
        let services = self.services.lock().unwrap();

        let updates: Vec<_>  = services.iter().filter_map(|(_id, svc)| {
            let i = svc.read().unwrap();
            match i.update_required(state, interval, force) {
                true => Some(i.info()),
                false => None,
            }
        }).collect();

        trace!("updates required (state: {:?}): {:?}", state, updates);

        updates
    }

    /// Sync a service instance to disk
    pub(crate) fn sync_inst(&mut self, inst: &ServiceInst) {
        let store = self.store.lock().unwrap();

        if let Err(e) = store.save_service(&inst.info()) {
            error!("Error writing service instance {}: {:?}", inst.id(), e);
        }
    }

    /// Fetch a list of information for known services
    pub fn list(&self) -> Vec<ServiceInfo> {
        let services = self.services.lock().unwrap();
        
        services.iter().map(|(_k, v)| {
            v.read().unwrap().info()
        }).collect()
    }

    /// Fetch the number of known services
    pub fn count(&self) -> usize {
        let services = self.services.lock().unwrap();
        services.len()
    }

    /// Sync the service database to disk
    pub fn sync(&self) {
        let services = self.services.lock().unwrap();
        let store = self.store.lock().unwrap();

        for (id, inst) in services.iter() {
            let mut inst = inst.write().unwrap();
            let i: &mut ServiceInst = &mut inst;
            // Skip unchanged instances
            if !i.changed {
                continue
            }

            if let Err(e) = store.save_service(&i.info()) {
                error!("Error writing service instance {}: {:?}", id, e);
            }

            
    

            i.changed = false;
        }
    }

    /// Load the service database from disk
    pub fn load(&self) {
        let store = self.store.lock().unwrap();
        let services = self.services.lock().unwrap();

        let service_info = store.load_services().unwrap();

        for _s in service_info {
            // URGENT TODO: rehydrate ServiceInst here
            //services.entry(s.id).or_insert(Arc::new(RwLock::new(v)));
        }
    }
}

impl Drop for ServiceManager {
    fn drop(&mut self) {
        // TODO: sync..?
    }
}