
use std::time::SystemTime;

use tracing::{span, Level};

use dsf_core::prelude::*;
use dsf_rpc::{RegisterCommand, RegisterInfo};

use crate::core::services::ServiceState;

use crate::daemon::Dsf;
use crate::error::Error;
use crate::io;

#[derive(Debug, Clone)]
pub enum RegisterError {
    UnknownService,
    NoPrivateKey,
    Inner(DsfError),
}


impl <C> Dsf <C> where C: io::Connector + Clone + Sync + Send + 'static {
    /// Register a locally known service
    pub async fn register(&mut self, command: RegisterCommand) -> Result<RegisterInfo, Error> {
        let span = span!(Level::DEBUG, "register");
        let _enter = span.enter();

        info!("Register: {:?}", &command.service);

        let id = self.resolve_identifier(&command.service)?;

        let mut services = self.services();

        // Generate pages
        // This needs to be in a scope so the generator doesn't try moving the
        // RwLockWriteGuart<'_, ServiceInst> over yeild points
        let (info, pages) = {
            // Fetch the known service from the service list
            let service = match services.find(&id) {
                Some(s) => s,
                None => {
                    // Only known services can be registered
                    error!("unknown service (id: {})", id);
                    return Err(Error::UnknownService.into())
                }
            };
            let mut s = service.try_write().unwrap();

            debug!("Generating service page");
            let primary_page = match s.publish(false) {
                Ok(v) => v,
                Err(e) => return Err(e.into()),
            };
            drop(s);

            let mut info = RegisterInfo {
                page_version: primary_page.version(),
                replica_version: None,
                peers: 0,
            };
    
            let mut pages = vec![primary_page];
    
            // Generate replica page unless disabled
            if !command.options.no_replica {
                debug!("Generating replica page");
    
                // Generate a replica page
                let mut s = service.try_write().unwrap();
    
                let replica_page = match s.replicate(self.service(), false) {
                    Ok(v) => v,
                    Err(e) => return Err(e.into()),
                };
    
                info.replica_version = Some(replica_page.version());
    
                pages.push(replica_page);
    
                drop(s);
            }

            (info, pages)
        };


        debug!("Registering service");
        trace!("Pages: {:?}", pages);

        // Store pages
        // TODO: get store info / number of peers storing
        self.store(&id, pages).await?;
        
        // Update local storage info
        services.update_inst(&id, |s| {
            s.state = ServiceState::Registered;
            s.last_updated = Some(SystemTime::now());
        });

        Ok(info)
    }
}