
use std::time::SystemTime;

use futures::prelude::*;
use futures::future::{ok, err, Either::A, Either::B};

use rr_mux::Connector;
use dsf_core::prelude::*;

use dsf_core::net;
use dsf_core::service::Subscriber;


use crate::core::services::ServiceState;
use crate::core::replicas::Replica;
use crate::core::ctx::Ctx;
use crate::rpc::{LocateOptions, LocateInfo};
use crate::daemon::dsf::Dsf;


impl <C> Dsf <C> 
where
    C: Connector<RequestId, Address, net::Request, net::Response, DsfError, Ctx> + Clone + Sync + Send + 'static,
{

    pub fn locate(&mut self, options: LocateOptions) -> impl Future<Item=LocateInfo, Error=DsfError> {
        info!("[DSF ({:?})] Locate: {:?}", self.id, &options.id);
        let mut services = self.services.clone();

        // Skip search for owned services...
        if let Some(service) = services.info(&options.id) {
            if service.origin {
                return A(ok(LocateInfo{origin: true, updated: false}))
            }
        }

        B(self.find(&options.id)
        .and_then(move |pages| {
            //let services = services.clone();

            debug!("locate, found {} pages", pages.len());
            // Fetch primary page
            let page = match pages.iter().find(|p| p.kind().is_page() && !p.flags().contains(Flags::SECONDARY) && p.id() == &options.id ) {
                Some(p) => p,
                None => return err(DsfError::NotFound),
            };

            // Fetch replica pages
            let replicas: Vec<Replica> = pages.iter().filter(|p| p.kind().is_page() && p.flags().contains(Flags::SECONDARY) 
            && p.application_id() == 0 && p.kind() == PageKind::Replica.into() )
            .filter_map(|p| {
                let peer_id = match p.info().peer_id() {
                        Some(v) => v,
                    _ => return None,
                };

                trace!("Processing replica page: {:?}", p);
                
                Some(Replica{
                    id: peer_id.clone(),
                    version: p.version(),
                    peer: None,
                    issued: p.issued(),
                    expiry: p.expiry().unwrap(),
                    active: false,
                })
            }).collect();

            info!("locate, found {} replicas", replicas.len());

            // Update service if existing
            if services.known(&options.id) {
                info!("updating existing service");

                services.update_inst(&options.id, |inst| {
                    // Apply page update
                    let updated = match inst.service.apply_primary(page) {
                        Ok(r) => r,
                        Err(e) => {
                            error!("error updating service: {:?}", e);
                            return
                        },
                    };

                    // Update internal last updated time
                    inst.last_updated = Some(SystemTime::now());

                    // Add updated service page to stored data
                    if updated {
                        inst.add_data(page).unwrap();
                    }

                    inst.sync_replicas(&replicas);

                } );

                debug!("locate done");

                ok(LocateInfo{origin: false, updated: true})
            } else {
                info!("creating new service entry");

                let service = match Service::load(&page) {
                    Ok(s) => s,
                    Err(e) => return err(e),
                };

                let _inst = services.register(service, page, ServiceState::Located, Some(SystemTime::now())).unwrap();

                //inst.write().unwrap().sync_replicas(&replicas);

                debug!("locate done");

                ok(LocateInfo{origin: false, updated: false})
            }
        }))
    }
}