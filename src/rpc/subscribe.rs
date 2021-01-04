use std::time::SystemTime;
use std::pin::Pin;
use std::task::{Poll, Context};
use std::future::Future;

use futures::prelude::*;
use futures::channel::mpsc;
use futures::future::join_all;

use tracing::{span, Level};
use log::{trace, debug, info, warn, error};

use dsf_core::error::Error as CoreError;
use dsf_core::net;
use dsf_core::prelude::*;
use dsf_rpc::{self as rpc, SubscribeOptions, SubscriptionInfo, SubscriptionKind};

use crate::core::services::ServiceState;
use crate::core::peers::Peer;
use crate::error::Error;
use crate::daemon::Dsf;
use crate::daemon::net::NetFuture;

use super::ops::*;

pub enum SubscribeState {
    Init,
    Searching(kad::dht::SearchFuture<Data>),
    Locating(Vec<kad::dht::LocateFuture<Id, Peer>>),
    Pending(NetFuture),
    Error(Error),
}

pub struct SubscribeOp {
    pub(crate) opts: SubscribeOptions,
    pub(crate) state: SubscribeState,
}

pub struct SubscribeFuture {
    rx: mpsc::Receiver<rpc::Response>,
}



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
        let replicas: Vec<_> = search_responses.drain(..).filter_map(|r| r.ok() ).collect();
        info!("Searches complete, found {} viable replicas", replicas.len());

        // Fetch addresses from viable replicas
        // TODO: limited subset of replicas
        let peers: Vec<_> = replicas.iter().filter(|_v| true).map(|v| v.info().clone() ).collect();

        // Issue subscription requests
        let req = net::Request::new(
            self.id(),
            rand::random(),
            net::RequestKind::Subscribe(id.clone()),
            Flags::default(),
        );
        info!("Sending subscribe messages to {} peers", replicas.len());

        let subscribe_responses = self.net_op(peers, req).await;


        let mut subscription_info = vec![];

        for (_peer_id, response) in &subscribe_responses {
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

    pub fn poll_rpc_subscribe(&mut self, req_id: u64, subscribe_op: &mut SubscribeOp, ctx: &mut Context, mut done: mpsc::Sender<rpc::Response>) -> Result<bool, DsfError> {
        let SubscribeOp{opts, state} = subscribe_op;

        let span = span!(Level::DEBUG, "subscribe");
        let _enter = span.enter();

        // Resolve service identifier
        let id = match self.resolve_identifier(&opts.service) {
            Ok(id) => id,
            Err(_) => {
                error!("No matching service for {}", opts.service.index.unwrap());
                return Err(DsfError::UnknownService);
            },
        };

        match state {
            SubscribeState::Init => {

                // Fetch the service from the service list
                let _service_info = match self.services().find(&id) {
                    Some(s) => s,
                    None => {
                        // Only known services can be registered
                        error!("unknown service (id: {})", id);
                        return Err(DsfError::UnknownService);
                    }
                };

                // Search for viable replicas in the database
                let (query, _) = match self.dht_mut().search(id) {
                    Ok(v) => (v),
                    Err(e) => {
                        error!("Error starting DHT search: {:?}", e);
                        return Err(DsfError::Unknown)
                    }
                };

                *state = SubscribeState::Searching(query);
                Ok(false)
            },
            SubscribeState::Searching(search) => {
                match search.poll_unpin(ctx) {
                    Poll::Ready(Ok(v)) => {
                        debug!("DHT search complete! {} replicas found", v.len());
                        trace!("{:?}", v);

                        // TODO: Update located replicas
                        // Though maybe this happens in the rx handle?

                        // Fetch peers for viable replicas
                        let replicas = self.replicas().find(&id);
                        let replica_peers: Vec<_> = replicas.iter().map(|r| {
                            r.info.peer_id.clone()
                        }).collect();

                        // TODO: Skip lookup if no known peers

                        // Lookup (unknown) peers
                        let lookups: Vec<_> = replica_peers.iter().filter_map(|peer_id| {
                            match self.dht_mut().locate(peer_id.clone()) {
                                Ok(v) => Some(v.0),
                                Err(e) => {
                                    error!("Error starting DHT locate for {:?}: {:?}", peer_id, e);
                                    None
                                }
                            }
                        }).collect();
                    

                        *state = SubscribeState::Locating(lookups);
                        Ok(false)
                    },
                    Poll::Ready(Err(e)) => {
                        error!("DHT store error: {:?}", e);

                        // TODO: propagate error
                        *state = SubscribeState::Error(Error::Unknown);
                        Ok(false)
                    },
                    _ => Ok(false),
                }
            },
            SubscribeState::Locating(lookups) => {
                // Poll for completion of pending DHT lookups
                let completed: Vec<_> = lookups.iter_mut().enumerate().filter_map(|(i, l)| {
                    match l.poll_unpin(ctx) {
                        Poll::Ready(v) => Some((i, v)),
                        _ => None
                    }
                } ).collect();

                // Remove completed lookups
                for (i, _r) in &completed {
                    lookups.remove(*i);
                }

                if completed.len() == 0 {
                    debug!("Subscribe peer lookup complete");

                    // Create subscription request
                    let req = net::Request::new(
                        self.id(),
                        rand::random(),
                        net::RequestKind::Subscribe(id.clone()),
                        Flags::default(),
                    );

                    // Generate peer list for subscription request
                    let peers: Vec<_> = completed.iter().filter_map(|(_i, r)| {
                        match r {
                            Ok(e) => Some(e.info().clone()),
                            _ => None,
                        }
                    }).collect();

                    debug!("Issuing subscribe request to {} peers", peers.len());

                    // Issue net operation
                    let op = self.net_op(peers, req);

                    *state = SubscribeState::Pending(op);
                }

                Ok(false)
            },
            SubscribeState::Pending(op) => {
                // Poll on network operation completion
                match op.poll_unpin(ctx) {
                    Poll::Ready(d) => {
                        debug!("Subscribe requests complete");

                        // Generate subscription info
                        let subs: Vec<_> = d.iter().filter_map(|(peer_id, resp)| {
                            match &resp.data {
                                net::ResponseKind::Status(s) if *s == net::Status::Ok => {
                                    Some(SubscriptionInfo{
                                        service_id: id.clone(),
                                        kind: SubscriptionKind::Peer(peer_id.clone()),
                                        updated: Some(SystemTime::now()),
                                        expiry: None,
                                    })
                                },
                                _ => None
                            }
                        }).collect();

                        debug!("Found {} subscribers", subs.len());

                        // TODO: send completion
                        let resp = rpc::Response::new(req_id, rpc::ResponseKind::Subscribed(subs));

                        done.try_send(resp).unwrap();

                    },
                    _ => (),
                };

                Ok(false)
            },
            _ => Ok(true),
        }
    }
}
