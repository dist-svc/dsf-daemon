use std::convert::TryFrom;
use std::net::SocketAddr;
use std::ops::Add;
use std::time::{Duration, SystemTime};

use log::{trace, debug, info, warn, error};


use bytes::Bytes;

use async_std::future::timeout;

use futures::prelude::*;
use futures::channel::mpsc;
use futures::stream::StreamExt;

use tracing::{span, Level};

use dsf_core::prelude::*;
use dsf_core::net;
use dsf_core::wire::Container;

use crate::daemon::Dsf;
use crate::error::Error as DaemonError;

use crate::core::data::DataInfo;
use crate::core::peers::{Peer, PeerAddress, PeerState};


impl Dsf {
    pub async fn handle_net_raw(&mut self, msg: crate::io::NetMessage) -> Result<(), DaemonError> {

        // Decode message
        let decoded = match self.net_decode(&msg.data).await {
            Ok(v) => v,
            Err(e) => {
                warn!("error decoding message from {:?}: {:?}", msg.address, e);
                return Ok(())
            }
        };

        // Route responses as required internally
        let resp = match decoded {
            NetMessage::Response(resp) => {
                let resp = self.handle_net_resp(msg.address, resp).await?;
                resp
            },
            NetMessage::Request(req) => {
                let resp = self.handle_net_req(msg.address, req).await?;
                Some(resp)
            },
        };

        // Return response if provided
        if let Some(r) = resp {
            // Encode response
            let d = self.net_encode(net::Message::Response(r)).await?;

            // Short-circuit to respond
            msg.reply(&d).await?;
        }

        Ok(())
    }

    pub async fn net_decode(&mut self, data: &[u8]) -> Result<net::Message, DaemonError> {
        // Decode message
        let (container, _n) = Container::from(&data);
        let _id: Id = container.id().into();

        // Parse out base object
        // TODO: pass secret keys for encode / decode here
        let (base, _n) = match Base::parse(&data, |id| self.find_public_key(id), |_id| None) {
            Ok(v) => v,
            Err(e) => {
                error!("Error decoding base message: {:?}", e);
                return Err(e.into());
            }
        };

        // TODO: use n here?

        // Convert into message type
        let message = match net::Message::convert(base, |id| self.find_public_key(id) ) {
            Ok(v) => v,
            Err(e) => {
                error!("Error convertng network message: {:?}", e);
                return Err(e.into());
            }
        };

        Ok(message)
    }

    pub async fn net_encode(&mut self, msg: net::Message) -> Result<Bytes, DaemonError> {
        // Encode response
        let mut buff = vec![0u8; 4096];

        // Convert to base message
        let mut b: Base = msg.into();

        // Sign and encode outgoing message
        // TODO: pass secret keys for encode / encrypt here
        let n = b.encode(self.service().private_key().as_ref(), None, &mut buff)?;

        Ok(Bytes::from((&buff[..n]).to_vec()))
    }

    pub async fn issue_net_req(
        &mut self,
        addr: SocketAddr,
        req: net::Request,
    ) -> Result<mpsc::Receiver<NetResponse>, DaemonError> {
        debug!(
            "issuing request: {:?} to: {:?}",
            req,
            addr,
        );

        let req_id = req.id;

        // Add message to internal tracking
        let (tx, rx) = mpsc::channel(1);
        { self.net_requests.insert((addr.into(), req_id), tx) };

        // Pass message to sink for transmission
        self.net_sink.send((addr.into(), NetMessage::Request(req))).await.unwrap();

        // Return future channel
        Ok(rx)
    }

    /// Handle a received response message and generate an (optional) response
    pub async fn handle_net_resp(
        &mut self,
        addr: SocketAddr,
        resp: net::Response,
    ) -> Result<Option<net::Response>, DaemonError> {

        let from = resp.from.clone();
        let req_id = resp.id;
        
        // Generic net message processing here
        let peer = self.handle_base(&from, &addr.into(), &resp.common, Some(SystemTime::now())).await;

        // Parse out DHT responses
        if let Some(dht_resp) = self.net_to_dht_response(&resp.data) {
            let _ = self.handle_dht_resp(from.clone(), peer, req_id, dht_resp);

            return Ok(None);
        }

        // Look for matching requests
        match self.net_requests.remove(&(addr.into(), req_id)) {
            Some(mut a) => {
                trace!("Found pending request for id {} address: {}", req_id, addr);
                a.send(resp).await?;
            }
            None => {
                error!("Received response id {} with no pending request", req_id);
                return Ok(None);
            }
        };


        // TODO: three-way-ack support?

        Ok(None)
    }

    /// Handle a received request message and generate a response
    pub async fn handle_net_req(
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

        trace!(
            "handling request (from: {:?} / {})\n {:?}",
            from,
            addr,
            &req
        );

        // Generic net message processing here
        let peer = self.handle_base(&from, &addr.into(), &req.common, Some(SystemTime::now())).await;

        // Handle specific DSF and DHT messages
        let mut resp = if let Some(dht_req) = self.net_to_dht_request(&req.data) {

            let dht_resp = self.handle_dht_req(from.clone(), peer, req.id, dht_req)?;

            let net_resp = self.dht_to_net_response(dht_resp);

            net::Response::new(own_id, req_id, net_resp, Flags::default())
        } else {
            let dsf_resp = self.handle_dsf(from.clone(), peer, req.data).await?;

            net::Response::new(own_id, req_id, dsf_resp, Flags::default())
        };

        // Generic response processing here

        if flags.contains(Flags::PUB_KEY_REQUEST) {
            resp.common.public_key = Some(our_pub_key);
        }

        // Update peer info
        self.peers().update(&from, |p| p.info.sent += 1 );

        trace!("returning response (to: {:?})\n {:?}", from, &resp);


        Ok(resp)
    }

    // Internal function to send a request and await a response
    /// This MUST be used in place of self.connector.clone.request for correct system behaviour
    pub(crate) async fn request(
        &mut self,
        address: Address,
        req: net::Request,
        t: Duration,
    ) -> Result<net::Response, DaemonError> {
        let req = req.clone();

        debug!("Sending request to: {:?} request: {:?}", address, &req);

        // Issue request and await response
        let mut resp_ch = self
            .issue_net_req(address.clone().into(), req.clone())
            .await?;

        // Await response
        let resp = match timeout(t, resp_ch.next()).await {
           Ok(Some(v)) => v,
           _ => {
               debug!("No response from: {:?} for request: {:?}", address, &req);
               return Err(DaemonError::Timeout);
           } 
        };

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
    ) -> Result<Vec<Option<net::Response>>, DaemonError> {
        let mut f = Vec::with_capacity(addresses.len());

        // Build requests
        for a in addresses {
            let mut req_ch = self
                .issue_net_req(a.clone().into(), req.clone())
                .await?;
                
            f.push(async move {
                req_ch.next().await.map(move |resp| (resp, a.clone()))
            });
        }

        let mut responses = future::join_all(f).await;

        for r in &responses {
            if let Some((resp, addr)) = r {
                self.handle_base(&resp.from, addr, &resp.common, Some(SystemTime::now())).await;
            }
        }

        Ok(responses.drain(..).map(|v| v.map(|r| r.0 )).collect())
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
        );

        // Update peer info
        self.peers().update(&id, |p| {
            p.info.seen = Some(SystemTime::now());
            p.info.received += 1;
        });

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
                self.peers().update(&id, |p| p.info.state = PeerState::Known(pk.clone()) );
            }
            _ => (),
        };

        // Update address if specified (and different from existing)
        if let Some(a) = c.remote_address {
            if a != peer.address() {
                info!("Setting explicit address {:?} for peer: {:?}", a, id);
                self.peers().update(&id, |p| p.info.address = PeerAddress::Explicit(a) );
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
                let _service = match self.services().find(&service_id) {
                    Some(s) => s,
                    None => {
                        // Only known services can be registered
                        error!("no service found (id: {})", service_id);
                        return Ok(net::ResponseKind::Status(net::Status::InvalidRequest));
                    }
                };

                // Fetch pages for service
                let pages = {
                    match &self.services().filter(&service_id, |s| s.primary_page.clone() ).flatten() {
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
                    
                    .unwrap();

                Ok(net::ResponseKind::ValuesFound(service_id, pages))
            }
            net::RequestKind::Unsubscribe(service_id) => {
                info!(
                    "Unsubscribe request from: {} for service: {}",
                    from, service_id
                );

                self.subscribers().remove(&service_id, &peer.id()).unwrap();

                Ok(net::ResponseKind::Status(net::Status::Ok))
            }
            net::RequestKind::Query(id) => {
                info!("Query request from: {} for service: {}", from, id);
                let _service = match self.services().find(&id) {
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

                self.service_register(&id, pages)?;

                Ok(net::ResponseKind::Status(net::Status::Ok))
            }
            net::RequestKind::Unregister(id) => {
                info!("Unegister request from: {} for service: {}", from, id);
                // TODO: determine whether we should allow this service to be unregistered

                unimplemented!()
            }
            net::RequestKind::PushData(id, data) => {
                info!("Data push from: {} for service: {}", from, id);

                // TODO: why find _then_ validate_pages, duplicated ops?!
                let _service = match self.services().find(&id) {
                    Some(s) => s,
                    None => {
                        // Only known services can be registered
                        error!("no service found (id: {})", id);
                        return Ok(net::ResponseKind::Status(net::Status::InvalidRequest));
                    }
                };

                // Validate incoming data prior to processing
                if let Err(e) = self.services().validate_pages(&id, &data) {
                    error!("Invalid data for service: {} ({:?})", id, e);
                    return Ok(net::ResponseKind::Status(net::Status::InvalidRequest));
                }

                // Pick out service pages

                let data_mgr = self.data().clone();

                for p in &data {
                    // Apply page to service
                    if p.header().kind().is_page() {
                        self.services().update_inst(&id, |s| { let _ = s.apply_update(p); } );
                    }

                    // Store data
                    if p.header().kind().is_data() {
                        if let Ok(info) = DataInfo::try_from(p) {
                            data_mgr.store_data(&info, p).unwrap();
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

                let peer_subs = self.subscribers().find_peers(&id)?;
                let mut addresses = Vec::with_capacity(peer_subs.len());

                for peer_id in peer_subs {
                    if let Some(peer) = self.peers().find(&peer_id) {
                        addresses.push(peer.address());
                    }
                }

                info!("Sending data push message id {} to: {:?}", req_id, addresses);

                // TODO: this is, not ideal...
                // DEADLOCK MAYBE?
                self.request_all(&addresses, req).await?;

                info!("Data push complete");

                Ok(net::ResponseKind::Status(net::Status::Ok))
            }
            _ => Err(DaemonError::Unimplemented),
        }
    }
}
