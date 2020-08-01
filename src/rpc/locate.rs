use std::time::SystemTime;

use tracing::{span, Level};

use dsf_core::prelude::*;
use dsf_core::service::Subscriber;
use dsf_rpc::{LocateInfo, LocateOptions};

use crate::core::services::ServiceState;

use crate::daemon::Dsf;
use crate::error::Error;
use crate::io;

impl<C> Dsf<C>
where
    C: io::Connector + Clone + Sync + Send + 'static,
{
    pub async fn locate(&mut self, options: LocateOptions) -> Result<LocateInfo, Error> {
        let span = span!(Level::DEBUG, "locate");
        let _enter = span.enter();

        let mut services = self.services();
        let replica_manager = self.replicas();

        // Skip search for owned services...
        if let Some(service) = services.info(&options.id) {
            if service.origin {
                return Ok(LocateInfo {
                    origin: true,
                    updated: false,
                });
            }
        }

        // Search for associated service pages
        let mut pages = self.search(&options.id).await?;

        //let services = services.clone();

        debug!("locate, found {} pages", pages.len());
        // Fetch primary page
        let primary_page = match pages.iter().find(|p| {
            let h = p.header();
            h.kind().is_page() && !h.flags().contains(Flags::SECONDARY) && p.id() == &options.id
        }) {
            Some(p) => p.clone(),
            None => return Err(Error::NotFound),
        };

        // Fetch replica pages
        let replicas: Vec<(Id, Page)> = pages
            .drain(..)
            .filter(|p| {
                let h = p.header();
                h.kind().is_page()
                    && h.flags().contains(Flags::SECONDARY)
                    && h.application_id() == 0
                    && h.kind() == PageKind::Replica.into()
            })
            .filter_map(|p| {
                let peer_id = match p.info().peer_id() {
                    Some(v) => v,
                    _ => return None,
                };

                Some((peer_id.clone(), p))
            })
            .collect();

        info!("locate, found {} replicas", replicas.len());

        // Fetch service instance
        let service_inst = match services.find(&options.id) {
            Some(s) => {
                info!("updating existing service");
                s
            }
            None => {
                info!("creating new service entry");
                let service = match Service::load(&primary_page) {
                    Ok(s) => s,
                    Err(e) => return Err(e.into()),
                };

                services
                    .register(
                        service,
                        &primary_page,
                        ServiceState::Located,
                        Some(SystemTime::now()),
                    )
                    .unwrap()
            }
        };

        // Update service
        let mut inst = service_inst.write().unwrap();

        // Apply page update
        let updated = match inst.service().apply_primary(&primary_page) {
            Ok(r) => r,
            Err(e) => {
                error!("error updating service: {:?}", e);
                return Err(e.into());
            }
        };

        // Add updated instance information
        if updated {
            inst.primary_page = Some(primary_page.clone());
            inst.last_updated = Some(SystemTime::now());
        }

        // Update replicas
        for (peer_id, page) in &replicas {
            replica_manager.create_or_update(&options.id, peer_id, page);
        }

        Ok(LocateInfo {
            origin: false,
            updated: true,
        })
    }
}
