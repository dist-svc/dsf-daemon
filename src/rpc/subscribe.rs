


use futures::prelude::*;
use futures::future::{ok, err, join_all};

use rr_mux::Connector;



use dsf_core::prelude::*;
use dsf_core::net;


use crate::core::{ctx::Ctx, services::ServiceState};
use crate::daemon::dsf::Dsf;
use crate::rpc::{SubscribeOptions, SubscribeInfo};


impl <C> Dsf <C> 
where
    C: Connector<RequestId, Address, net::Request, net::Response, DsfError, Ctx> + Clone + Sync + Send + 'static,
{
    // Subscribe to data from a given service
    pub fn subscribe(&mut self, options: SubscribeOptions) -> Box<dyn Future<Item=SubscribeInfo, Error=DsfError> + Send> { 
        info!("[DSF ({:?})] Subscribe: {:?}", self.id, &options.service);

        let id = match self.resolve_identifier(&options.service) {
            Ok(id) => id,
            Err(e) => return Box::new(err(e)),
        };

        let own_id = self.id.clone();

        // Fetch the known service from the service list
        let service_arc = match self.services.find(&id) {
            Some(s) => s,
            None => {
                // Only known services can be registered
                error!("[DSF ({:?})]unknown service (id: {})", self.id, id);
                return Box::new(err(DsfError::UnknownService))
            }
        };

        let service_inst = service_arc.read().unwrap();
        let service_id = service_inst.id().clone();

        debug!("Service: {:?}", service_inst);

        // Lookup possible replicas
        let mut searches = Vec::with_capacity(service_inst.replicas.len());
        for (id, _r) in service_inst.replicas.iter() {
            let mut s = self.clone();
            searches.push(s.search(id.clone()).then(|r| {
                match r {
                    Ok(r) => Ok(Ok(r)),
                    Err(e) => Ok(Err(e)),
                }
            }));
        }

        drop(service_inst);

        info!("[DSF ({:?})] Searching for viable replicas", own_id);
        let mut s = self.clone();

        Box::new(join_all(searches)
            .and_then(move |mut responses| {
                // Filter successful responses
                let ok: Vec<_> = responses.drain(..).filter_map(|r| r.ok() ).collect();
                info!("[DSF ({:?})] Searches complete, found {} viable replicas", s.id, ok.len());

                // TODO: limited subset of replicas
                let addrs: Vec<_> = ok.iter().filter(|_v| true).map(|v| v.address() ).collect();
                
                // Issue subscription requests                
                let req = net::Request::new(s.id.clone(), net::RequestKind::Subscribe(service_id), Flags::default());

                info!("[DSF ({:?})] Sending subscribe messages to {} peers", s.id, addrs.len());
                s.request_all(Ctx::default(), &addrs, req)
            })
            .and_then(move |res| {
                let mut service = service_arc.write().unwrap();
                let mut count = 0;
                
                for r in &res {
                    
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
                    return err(DsfError::NoReplicasFound)
                }

                ok(SubscribeInfo{count})
            }))

    }
}