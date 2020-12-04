use std::time::SystemTime;

use futures::future::join_all;

use tracing::{span, Level};
use log::{debug, info, warn, error};

use dsf_core::error::Error as CoreError;
use dsf_core::net;
use dsf_core::prelude::*;
use dsf_rpc::{SubscribeOptions, SubscriptionInfo, SubscriptionKind};

use crate::core::services::ServiceState;
use crate::error::Error;

use crate::daemon::Dsf;


impl Dsf {
    // Subscribe to data from a given service
    pub async fn subscribe(
        &mut self,
        options: SubscribeOptions,
    ) -> Result<Vec<SubscriptionInfo>, Error> {
        let span = span!(Level::DEBUG, "subscribe");
        let _enter = span.enter();

        info!("Subscribe: {:?}", &options.service);

        let id = match self.resolve_identifier(&options.service) {
            Ok(id) => id,
            Err(e) => return Err(e),
        };

        let own_id = self.id();

        // Fetch the known service from the service list
        let _service_info = match self.services().find(&id) {
            Some(s) => s,
            None => {
                // Only known services can be registered
                error!("unknown service (id: {})", id);
                return Err(Error::UnknownService);
            }
        };

        debug!("Service: {:?}", id);

        // TODO: query for replicas from the distributed database?

        // Fetch known replicas
        let replicas = self.replicas().find(&id);

        // Search for peer information for viable replicas
        let mut searches = Vec::with_capacity(replicas.len());
        for inst in replicas.iter() {

            let (locate, _req_id) = match self.dht_mut().locate(inst.info.peer_id.clone()) {
                Ok(v) => v,
                Err(e) => {
                    error!("Error starting DHT locate: {:?}", e);
                    return Err(Error::Unknown);
                }
            };

            searches.push(locate);
        };

        info!("Searching for viable replicas");

        // Execute searches
        let mut search_responses = join_all(searches).await;

        // Filter successful responses
        let ok: Vec<_> = search_responses.drain(..).filter_map(|r| r.ok() ).collect();
        info!("Searches complete, found {} viable replicas", ok.len());

        // Fetch addresses from viable replicas
        // TODO: limited subset of replicas
        let addrs: Vec<_> = ok.iter().filter(|_v| true).map(|v| v.info().address()).collect();

        // Issue subscription requests
        let req = net::Request::new(
            self.id(),
            rand::random(),
            net::RequestKind::Subscribe(id.clone()),
            Flags::default(),
        );
        info!("Sending subscribe messages to {} peers", addrs.len());

        let subscribe_responses = self.request_all(&addrs, req).await?;


        let mut subscription_info = vec![];

        for r in &subscribe_responses {
            let response = match r {
                Some(v) => v,
                None => {
                    warn!("[DSF ({:?})] No response to subscribe request", own_id);
                    continue;
                }
            };

            match response.data {
                net::ResponseKind::Status(net::Status::Ok)
                | net::ResponseKind::ValuesFound(_, _) => {
                    debug!(
                        "[DSF ({:?})] Subscription ack from: {:?}",
                        own_id, response.from
                    );

                    // Update replica status
                    self.replicas()
                        .update_replica(&id, &response.from, |r| {
                            r.info.active = true;
                        })
                        .unwrap();

                    subscription_info.push(SubscriptionInfo {
                        kind: SubscriptionKind::Peer(response.from.clone()),
                        service_id: id.clone(),
                        updated: Some(SystemTime::now()),
                        expiry: None,
                    });
                }
                net::ResponseKind::Status(net::Status::InvalidRequest) => {
                    debug!(
                        "[DSF ({:?})] Subscription denied from: {:?}",
                        own_id, response.from
                    );
                }
                _ => {
                    warn!(
                        "[DSF ({:?})] Unhandled response from: {:?}",
                        own_id, response.from
                    );
                }
            }
        }

        if subscription_info.len() > 0 {
            info!(
                "[DSF ({:?})] Subscription complete, updating service state",
                own_id
            );
            self.services().update_inst(&id, |s| {
                if s.state == ServiceState::Located {
                    s.state = ServiceState::Subscribed;
                }
            });
        } else {
            warn!(
                "[DSF ({:?})] Subscription failed, no viable replicas found",
                own_id
            );
            return Err(Error::Core(CoreError::NoReplicasFound));
        }

        Ok(subscription_info)
    }
}
