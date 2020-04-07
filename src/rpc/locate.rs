
use std::time::SystemTime;

use tracing::{span, Level};

use dsf_core::prelude::*;
use dsf_core::service::Subscriber;
use dsf_rpc::{LocateOptions, LocateInfo, ReplicaInfo};

use crate::core::services::ServiceState;
use crate::core::replicas::Replica;

use crate::daemon::Dsf;
use crate::error::Error;
use crate::io;


impl <C> Dsf <C> where C: io::Connector + Clone + Sync + Send + 'static {

    pub async fn locate(&mut self, options: LocateOptions) -> Result<LocateInfo, Error> {
        let span = span!(Level::DEBUG, "locate");
        let _enter = span.enter();

        let mut services = self.services();

        // Skip search for owned services...
        if let Some(service) = services.info(&options.id) {
            if service.origin {
                return Ok(LocateInfo{origin: true, updated: false})
            }
        }

        // Search for associated service pages
        let mut pages = self.search(&options.id).await?;

        //let services = services.clone();

        debug!("locate, found {} pages", pages.len());
        // Fetch primary page
        let primary_page = match pages.iter().find(|p| p.kind().is_page() && !p.flags().contains(Flags::SECONDARY) && p.id() == &options.id ) {
            Some(p) => p.clone(),
            None => return Err(Error::NotFound),
        };

        // Fetch replica pages
        let replicas: Vec<Replica> = pages.drain(..).filter(|p| p.kind().is_page() && p.flags().contains(Flags::SECONDARY) 
        && p.application_id() == 0 && p.kind() == PageKind::Replica.into() )
        .filter_map(|p| {
            let peer_id = match p.info().peer_id() {
                    Some(v) => v,
                _ => return None,
            };

            Some(Replica::from(p))
        }).collect();

        info!("locate, found {} replicas", replicas.len());

        if services.known(&options.id) {
            // Update a known service
            info!("updating existing service");

            services.update_inst(&options.id, |inst| {
                // Apply page update
                let updated = match inst.service.apply_primary(&primary_page) {
                    Ok(r) => r,
                    Err(e) => {
                        error!("error updating service: {:?}", e);
                        return
                    },
                };

                // Update internal last updated time
                inst.last_updated = Some(SystemTime::now());

                // Add updated service page to
                if updated {
                    inst.primary_page = Some(primary_page.clone());
                }

                inst.sync_replicas(&replicas);
            } );

            debug!("locate done");

            Ok(LocateInfo{origin: false, updated: true})
        } else {
            // Create a new entry for unknown service
            info!("creating new service entry");

            let service = match Service::load(&primary_page) {
                Ok(s) => s,
                Err(e) => return Err(e.into()),
            };

            let inst = services.register(service, &primary_page, ServiceState::Located, Some(SystemTime::now())).unwrap();

            inst.write().unwrap().sync_replicas(&replicas);

            debug!("locate done");

            Ok(LocateInfo{origin: false, updated: true})
        }

    }
}