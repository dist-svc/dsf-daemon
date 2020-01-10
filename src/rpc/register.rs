
use std::time::SystemTime;

use futures::prelude::*;
use futures::future::{err};

use rr_mux::Connector;



use dsf_core::prelude::*;

use dsf_core::net;



use crate::core::services::ServiceState;

use crate::core::ctx::Ctx;
use crate::daemon::dsf::Dsf;
use crate::rpc::{RegisterOptions, RegisterInfo};

#[derive(Debug, Clone)]
pub enum RegisterError {
    UnknownService,
    NoPrivateKey,
    Inner(DsfError),
}

impl <C> Dsf <C> 
where
    C: Connector<RequestId, Address, net::Request, net::Response, DsfError, Ctx> + Clone + Sync + Send + 'static,
{
    /// Register a locally known service
    pub fn register(&mut self, options: RegisterOptions) -> Box<dyn Future<Item=RegisterInfo, Error=DsfError> + Send> {
        info!("[DSF ({:?})] Register: {:?}", self.id, &options.service);

        let id = match self.resolve_identifier(&options.service) {
            Ok(id) => id,
            Err(e) => return Box::new(err(e)),
        };

        let mut services = self.services.clone();

        // Fetch the known service from the service list
        let service = match self.services.find(&id) {
            Some(s) => s,
            None => {
                // Only known services can be registered
                error!("unknown service (id: {})", id);
                return Box::new(err(DsfError::UnknownService))
            }
        };
        let mut s = service.try_write().unwrap();

        debug!("Generating service page");
        let primary_page = match s.publish(false) {
            Ok(v) => v,
            Err(e) => return Box::new(err(e)),
        };
        drop(s);

        let mut info = RegisterInfo {
            page_version: primary_page.version(),
            replica_version: None,
        };

        let mut pages = vec![primary_page];

        // Generate replica page unless disabled
        if !options.no_replica {
            debug!("Generating replica page");

            // Generate a replica page
            let mut s = service.try_write().unwrap();
            let peer_service = self.service.clone();
            let mut peer_service = peer_service.write().unwrap();

            let replica_page = match s.replicate(&mut peer_service, false) {
                Ok(v) => v,
                Err(e) => return Box::new(err(e)),
            };

            info.replica_version = Some(replica_page.version());

            pages.push(replica_page);

            drop(peer_service);
            drop(s);
        }
        
        
        // Drop to ensure mutex is closed out prior to later update
        drop(service);

        info!("Registering service");
        trace!("Pages: {:?}", pages);
        Box::new(self.store(&id, pages, Ctx::default()).map(move |_n| {
            services.update_inst(&id, |s| {
                s.state = ServiceState::Registered;
                s.last_updated = Some(SystemTime::now());
            });
            info
        } ))
    }

}