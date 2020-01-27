


use futures::future::join_all;

use tracing::{span, Level};

use dsf_core::prelude::*;
use dsf_core::net;
use dsf_rpc::{SubscribeOptions, SubscribeInfo};

use crate::error::Error;
use crate::core::services::ServiceState;
use crate::daemon::Dsf;
use crate::io;


impl <C> Dsf <C> where C: io::Connector + Clone + Sync + Send + 'static {

    // Subscribe to data from a given service
    pub async fn subscribe(&mut self, options: SubscribeOptions) -> Result<SubscribeInfo, Error> { 

        let span = span!(Level::DEBUG, "subscribe");
        let _enter = span.enter();

        info!("Subscribe: {:?}", &options.service);

        let id = match self.resolve_identifier(&options.service) {
            Ok(id) => id,
            Err(e) => return Err(e)
        };

        let own_id = self.id();
        
        let (service_id, searches) = {
            // Fetch the known service from the service list
            let service_arc = match self.services().find(&id) {
                Some(s) => s,
                None => {
                    // Only known services can be registered
                    error!("unknown service (id: {})", id);
                    return Err(Error::UnknownService)
                }
            };

            let service_inst = service_arc.read().unwrap();
            let service_id = service_inst.id();

            debug!("Service: {:?}", service_inst);

            // Build search across listed replicas
            let mut searches = Vec::with_capacity(service_inst.replicas.len());
            for (id, _r) in service_inst.replicas.iter() {
                let mut s = self.clone();
                let id = id.clone();

                searches.push(async move {
                    s.lookup(&id).await
                });
            }

            drop(service_inst);

            (service_id, searches)
        };

        info!("Searching for viable replicas");

        // Execute searches
        let mut search_responses = join_all(searches).await;

        // Filter successful responses
        let ok: Vec<_> = search_responses.drain(..).filter_map(|r| r.ok() ).collect();
        info!("Searches complete, found {} viable replicas", ok.len());

        // Fetch addresses from viable replicas
        // TODO: limited subset of replicas
        let addrs: Vec<_> = ok.iter().filter(|_v| true).map(|v| v.address() ).collect();
        
        // Issue subscription requests                
        let req = net::Request::new(self.id(), net::RequestKind::Subscribe(service_id), Flags::default());
        info!("Sending subscribe messages to {} peers", addrs.len());
        
        let subscribe_responses = self.request_all(&addrs, req).await;

        // Fetch the known service from the service list
        let service_arc = self.services().find(&id).unwrap();
        let mut service = service_arc.write().unwrap();
        let mut count = 0;
        
        for r in &subscribe_responses {
            
            let response = match r {
                Ok(v) => v,
                Err(e) => {
                    warn!("[DSF ({:?})] Subscribe request error: {:?}", own_id, e);
                    continue
                }
            };

            match response.data {
                net::ResponseKind::Status(net::Status::Ok) => {
                    debug!("[DSF ({:?})] Subscription ack from: {:?}", own_id, response.from);

                    service.update_replica(response.from, |mut r| {
                        r.active = true;
                    });

                    count += 1;
                },
                net::ResponseKind::Status(net::Status::InvalidRequest) => {
                    debug!("[DSF ({:?})] Subscription denied from: {:?}", own_id, response.from);
                },
                _ => {
                    warn!("[DSF ({:?})] Unhandled response from: {:?}", own_id, response.from);
                }
            }
        }

        if count > 0 {
            info!("[DSF ({:?})] Subscription complete, updating service state", own_id);
            service.update(|s| {
                if s.state == ServiceState::Located {
                    s.state = ServiceState::Subscribed;
                }
            });
        } else {
            warn!("[DSF ({:?})] Subscription failed, no viable replicas found", own_id);
            return Err(Error::NoReplicasFound)
        }

        Ok(SubscribeInfo{count})

    }
}