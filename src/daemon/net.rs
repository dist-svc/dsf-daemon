use std::convert::TryFrom;
use std::net::SocketAddr;
use std::ops::Add;
use std::time::{Duration, SystemTime};

use async_std::future::timeout;
use futures::prelude::*;
use tracing::{span, Level};

use dsf_core::net;
use dsf_core::prelude::*;
use dsf_core::service::Subscriber;

use kad::prelude::*;

use crate::daemon::Dsf;
use crate::error::Error as DaemonError;
use crate::io::Connector;

use crate::daemon::dht::{Adapt, TryAdapt};

use crate::core::data::DataInfo;
use crate::core::peers::{Peer, PeerAddress, PeerState};
use crate::core::subscribers::SubscriptionKind;

impl<C> Dsf<C>
where
    C: Connector + Clone + Sync + Send + 'static,
{
    /// Handle a received request message and generate a response
    pub async fn handle_net(
        &mut self,
        addr: SocketAddr,
        req: net::Request,
    ) -> Result<net::Response, DaemonError> {
        let own_id = self.id();

        let span = span!(Level::DEBUG, "id", "{}", own_id);
        let _enter = span.enter();

        let req_id = req.id;
        let flags = req.flags.clone();
        let our_pub_key = self.service().public_key();
        let from = req.from.clone();
        let from2 = from.clone();

        trace!(
            "handling request (from: {:?} / {})\n {:?}",
            from,
            addr,
            &req
        );

        // Generic request processing here
        let peer = self.handle_base(&from, &addr.into(), &req.common, Some(SystemTime::now())).await;

        // Handle specific DSF and DHT messages
        let mut resp = if let Some(kad_req) = req.data.try_to(()).await {
            let dht_resp = self.handle_dht(from, peer, kad_req)?;

            net::Response::new(own_id, req_id, dht_resp.to().await, Flags::default()
        )
        } else {
            let dsf_resp = self.handle_dsf(from, peer, req.data).await?;

             net::Response::new(own_id, req_id, dsf_resp, Flags::default())
        };

        // Generic response processing here

        if flags.contains(Flags::PUB_KEY_REQUEST) {
            resp.common.public_key = Some(our_pub_key);
        }

        // Update peer info
        self.peers().update(&from2, |p| p.info.sent += 1 ).await;

        trace!("returning response (to: {:?})\n {:?}", from2, &resp);


        Ok(resp)
    }

    // Internal function to send a request and await a response
    /// This MUST be used in place of self.connector.clone.request for correct system behaviour
    pub(crate) async fn request(
        &mut self,
        address: Address,
        req: net::Request,
        timeout: Duration,
    ) -> Result<net::Response, DaemonError> {
        let req = req.clone();

        debug!("Sending request to: {:?} request: {:?}", address, &req);

        // Issue request and await response
        let resp = self
            .connector()
            .request(req.id, address.clone(), req, timeout)
            .await?;

        debug!(
            "Received response from: {:?} request: {:?}",
            &address, &resp
        );

        // Handle received message
        self.handle_base(&resp.from, &address, &resp.common, Some(SystemTime::now())).await;

        Ok(resp)
    }

    /// Internal function to send a series of requests
    pub(crate) async fn request_all(
        &mut self,
        addresses: &[Address],
        req: net::Request,
    ) -> Vec<Result<net::Response, DaemonError>> {
        let mut f = Vec::with_capacity(addresses.len());

        let c = self.connector();

        // Build requests
        for a in addresses {
            let req_future = c
                .request(req.id, a.clone(), req.clone(), Duration::from_secs(10))
                .map(move |resp| (resp, a.clone()));

            f.push(req_future);
        }

        let mut responses = future::join_all(f).await;

        for (r, a) in &responses {
            if let Ok(resp) = r {
                self.handle_base(&resp.from, a, &resp.common, Some(SystemTime::now())).await;
            }
        }

        responses.drain(..).map(|(resp, _addr)| resp).collect()
    }

    /// Internal function to send a response
    /// This MUST be used in place of self.connector.clone.respond for correct system behavior
    pub(crate) async fn respond(
        &mut self,
        address: Address,
        resp: net::Response,
    ) -> Result<(), DaemonError> {
        let resp = resp.clone();

        trace!(
            "[DSF ({:?})] Sending response to: {:?} response: {:?}",
            self.id(),
            address,
            &resp
        );

        timeout(
            Duration::from_secs(10),
            self.connector().respond(resp.id, address, resp),
        )
        .await??;

        Ok(())
    }

    /// Handles a base message, updating internal state for the sender
    pub(crate) async fn handle_base(
        &mut self,
        id: &Id,
        address: &Address,
        c: &net::Common,
        _seen: Option<SystemTime>,
    ) -> Peer {
        trace!(
            "[DSF ({:?})] Handling base message from: {:?} address: {:?} public_key: {:?}",
            self.id(),
            id,
            address,
            c.public_key
        );

        // Find or create (and push) peer
        let peer = self.peers().find_or_create(
            id.clone(),
            PeerAddress::Implicit(*address),
            c.public_key.clone(),
        ).await;

        // Update peer info
        self.peers().update(&id, |p| {
            p.info.seen = Some(SystemTime::now());
            p.info.received += 1;
        }).await;

        assert!(id != &self.id(), "handle_base called for self...");

        trace!(
            "[DSF ({:?})] Peer id: {:?} state: {:?} seen: {:?}",
            self.id(),
            id,
            peer.state(),
            peer.seen()
        );

        // Add public key if appropriate
        match (peer.state(), &c.public_key) {
            (PeerState::Unknown, Some(pk)) => {
                info!("Adding key: {:?} to peer: {:?}", pk, id);
                self.peers().update(&id, |p| p.info.state = PeerState::Known(pk.clone()) ).await;
            }
            _ => (),
        };

        // Update address if specified (and different from existing)
        if let Some(a) = c.remote_address {
            if a != peer.address() {
                info!("Setting explicit address {:?} for peer: {:?}", a, id);
                self.peers().update(&id, |p| p.info.address = PeerAddress::Explicit(a) ).await;
            }
        }

        peer
    }

    /// Handle a DSF type message
    async fn handle_dsf(
        &mut self,
        from: Id,
        peer: Peer,
        req: net::RequestKind,
    ) -> Result<net::ResponseKind, DaemonError> {
        match req {
            net::RequestKind::Hello => Ok(net::ResponseKind::Status(net::Status::Ok)),
            net::RequestKind::Subscribe(service_id) => {
                info!(
                    "Subscribe request from: {} for service: {}",
                    from, service_id
                );
                let service = match self.services().find(&service_id).await {
                    Some(s) => s,
                    None => {
                        // Only known services can be registered
                        error!("no service found (id: {})", service_id);
                        return Ok(net::ResponseKind::Status(net::Status::InvalidRequest));
                    }
                };

                // Fetch pages for service
                let pages = {
                    match &self.services().filter(&service_id, |s| s.primary_page.clone() ).await.flatten() {
                        Some(p) => vec![p.clone()],
                        None => vec![],
                    }
                };

                // TODO: verify this is coming from an active upstream subscriber

                // TODO: update subscriber count?

                // TODO: update peer subscription information here
                self.subscribers()
                    .update_peer(&service_id, &peer.id(), |inst| {
                        inst.info.updated = Some(SystemTime::now());
                        inst.info.expiry = Some(SystemTime::now().add(Duration::from_secs(3600)));
                    })
                    .await
                    .unwrap();

                Ok(net::ResponseKind::ValuesFound(service_id, pages))
            }
            net::RequestKind::Unsubscribe(service_id) => {
                info!(
                    "Unsubscribe request from: {} for service: {}",
                    from, service_id
                );

                self.subscribers().remove(&service_id, &peer.id()).await.unwrap();

                Ok(net::ResponseKind::Status(net::Status::Ok))
            }
            net::RequestKind::Query(id) => {
                info!("Query request from: {} for service: {}", from, id);
                let service = match self.services().find(&id).await {
                    Some(s) => s,
                    None => {
                        // Only known services can be registered
                        error!("no service found (id: {})", id);
                        return Ok(net::ResponseKind::Status(net::Status::InvalidRequest));
                    }
                };

                // TODO: fetch and return data


                info!("Query request complete");

                Err(DaemonError::Unimplemented)
            }
            net::RequestKind::Register(id, pages) => {
                info!("Register request from: {} for service: {}", from, id);
                // TODO: determine whether we should allow this service to be registered

                //self.service_register(&id, pages)?;

                Ok(net::ResponseKind::Status(net::Status::Ok))
            }
            net::RequestKind::Unregister(id) => {
                info!("Unegister request from: {} for service: {}", from, id);
                // TODO: determine whether we should allow this service to be unregistered

                unimplemented!()
            }
            net::RequestKind::PushData(id, data) => {
                info!("Data push from: {} for service: {}", from, id);

                let service = match self.services().find(&id).await {
                    Some(s) => s,
                    None => {
                        // Only known services can be registered
                        error!("no service found (id: {})", id);
                        return Ok(net::ResponseKind::Status(net::Status::InvalidRequest));
                    }
                };

                // Validate incoming data prior to processing
                if let Err(e) = self.services().validate_pages(&id, &data).await {
                    error!("Invalid data for service: {}", id);
                    return Ok(net::ResponseKind::Status(net::Status::InvalidRequest));
                }

                // Pick out service pages

                let data_mgr = self.data().clone();

                for p in &data {
                    // Apply page to service
                    if p.header().kind().is_page() {
                        self.services().update_inst(&id, |s| { let _ = s.apply_update(p); } ).await;
                    }

                    // Store data
                    if p.header().kind().is_data() {
                        if let Ok(info) = DataInfo::try_from(p) {
                            data_mgr.store_data(&info, p).await.unwrap();
                        };
                    }
                }
                

                // TODO: send data to subscribers
                let req_id = rand::random();
                let req = net::Request::new(
                    self.id(),
                    req_id,
                    net::RequestKind::PushData(id.clone(), data),
                    Flags::default(),
                );

                let peer_subs = self.subscribers().find_peers(&id).await?;
                let mut addresses = Vec::with_capacity(peer_subs.len());

                for peer_id in peer_subs {
                    if let Some(peer) = self.peers().find(&peer_id).await {
                        addresses.push(peer.address());
                    }
                }

                info!("Sending data push message id {} to: {:?}", req_id, addresses);

                // TODO: this is, not ideal...
                // DEADLOCK MAYBE?
                //let mut dsf = self.clone();
                //async_std::task::spawn(async move {
                //    dsf.request_all(&addresses, req).await;
                //});

                info!("Data push complete");

                Ok(net::ResponseKind::Status(net::Status::Ok))
            }
            _ => Err(DaemonError::Unimplemented),
        }
    }

    /// Handle a DHT request message
    fn handle_dht(
        &mut self,
        from: Id,
        peer: Peer,
        req: DhtRequest<Id, Data>,
    ) -> Result<DhtResponse<Id, Peer, Data>, DaemonError> {
        // TODO: resolve this into existing entry
        let from = DhtEntry::new(from.into(), peer);

        // Pass to DHT
        let resp = self.dht().handle(&from, &req).unwrap();

        Ok(resp)
    }
}
