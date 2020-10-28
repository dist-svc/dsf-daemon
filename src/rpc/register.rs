use std::time::SystemTime;

use tracing::{span, Level};

use dsf_core::prelude::*;
use dsf_rpc::{RegisterInfo, RegisterOptions};

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

impl<C> Dsf<C>
where
    C: io::Connector + Clone + Sync + Send + 'static,
{
    /// Register a locally known service
    pub async fn register(&mut self, options: RegisterOptions) -> Result<RegisterInfo, Error> {
        let span = span!(Level::DEBUG, "register");
        let _enter = span.enter();

        info!("Register: {:?}", &options.service);

        let id = self.resolve_identifier(&options.service)?;

        let mut services = self.services();

        // Generate pages

        // Fetch the known service from the service list
        let service_info = match services.find(&id) {
            Some(s) => s,
            None => {
                // Only known services can be registered
                error!("unknown service (id: {})", id);
                return Err(Error::UnknownService.into());
            }
        };

        let mut pages = vec![];
        let mut page_version = 0u16;
        let mut replica_version = None;

        // Update service instance
        let service_info = services.update_inst(&id, |s| {

            debug!("Generating service page");
            let primary_page = match s.publish(false) {
                Ok(v) => v,
                Err(e) => {
                    error!("Error generating primary page: {:?}", e);
                    return
                },
            };

            page_version = primary_page.header().index();

            pages.push(primary_page);

            // Generate replica page unless disabled
            if !options.no_replica {
                debug!("Generating replica page");

                // Generate a replica page
                let replica_page = match s.replicate(self.service(), false) {
                    Ok(v) => v,
                    Err(e) => {
                        error!("Error generating replica page: {:?}", e);
                        return
                    },
                };

                replica_version = Some(replica_page.header().index());

                pages.push(replica_page);
            }
        });


        debug!("Registering service");
        trace!("Pages: {:?}", pages);

        // Store pages
        // TODO: get store info / number of peers storing
        let peers = self.store(&id, pages).await?;

        // Update local storage info
        services.update_inst(&id, |s| {
            s.state = ServiceState::Registered;
            s.last_updated = Some(SystemTime::now());
        });

        Ok(RegisterInfo{
            peers,
            page_version,
            replica_version,
        })
    }
}
