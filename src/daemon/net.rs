use std::collections::HashMap;
use std::convert::TryFrom;
use std::future::Future;
use std::net::SocketAddr;
use std::ops::Add;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;
use std::time::{Duration, SystemTime};

use kad::common::message;
use log::{debug, error, info, trace, warn};

use bytes::Bytes;

use async_std::future::timeout;

use futures::channel::mpsc;
use futures::prelude::*;
use futures::stream::StreamExt;

use tracing::{span, Level};

use dsf_core::net;
use dsf_core::prelude::*;
use dsf_core::wire::Container;

use crate::daemon::Dsf;
use crate::error::Error as DaemonError;

use crate::core::data::DataInfo;
use crate::core::peers::{Peer, PeerAddress, PeerState, PeerFlags};

/// Network operation for management by network module
pub struct NetOp {
    /// Network request (required for retries)
    req: net::Request,

    /// Pending network requests by peer ID
    reqs: HashMap<Id, mpsc::Receiver<net::Response>>,

    /// Received responses by peer ID
    resps: HashMap<Id, net::Response>,

    /// Completion channel
    done: mpsc::Sender<HashMap<Id, net::Response>>,

    /// Operation start timestamp
    ts: Instant,
}

impl Future for NetOp {
    type Output = Result<(), ()>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
        // Poll incoming response channels
        let resps: Vec<_> = self
            .reqs
            .iter_mut()
            .filter_map(|(id, r)| match r.poll_next_unpin(ctx) {
                Poll::Ready(Some(r)) => Some((id.clone(), r)),
                _ => None,
            })
            .collect();

        // Update received responses and remove resolved requests
        for (id, resp) in resps {
            self.reqs.remove(&id);
            self.resps.insert(id, resp);
        }

        // Check for completion
        let done = if self.reqs.len() == 0 {
            debug!("Net operation {} complete", self.req.id);
            true

        // Check for timeouts
        } else if Instant::now().saturating_duration_since(self.ts) > Duration::from_secs(3) {
            debug!("Net operation {} timeout", self.req.id);
            true
        } else {
            false
        };

        // Issue completion and resolve when done
        if done {
            // TODO: gracefully drop request channels when closed
            // probably this is the case statement in the network handler

            let resps = self.resps.clone();
            if let Err(e) = self.done.try_send(resps) {
                trace!(
                    "Error sending net done (rx channel may have been dropped): {:?}",
                    e
                );
            }
            return Poll::Ready(Ok(()));
        }

        // Always arm waker because we can't really do anything else
        // TODO: investigate embedding wakers / propagating via DSF wake/poll
        let w = ctx.waker().clone();
        w.wake();

        Poll::Pending
    }
}

pub struct NetFuture {
    rx: mpsc::Receiver<HashMap<Id, net::Response>>,
}

impl Future for NetFuture {
    type Output = HashMap<Id, net::Response>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.rx.poll_next_unpin(ctx) {
            Poll::Ready(Some(v)) => Poll::Ready(v),
            _ => Poll::Pending,
        }
    }
}

impl Dsf {
    pub async fn handle_net_raw(&mut self, msg: crate::io::NetMessage) -> Result<(), DaemonError> {
        // Decode message
        let decoded = match self.net_decode(&msg.data).await {
            Ok(v) => v,
            Err(e) => {
                warn!("error decoding message from {:?}: {:?}", msg.address, e);
                return Ok(());
            }
        };

        trace!("Net message: {:?}", decoded);
        let from = decoded.from();

        // Route responses as required internally
        let resp = match decoded {
            NetMessage::Response(resp) => {
                let resp = self.handle_net_resp(msg.address, resp)?;
                resp
            }
            NetMessage::Request(req) => {
                let resp = self.handle_net_req(msg.address, req)?;
                Some(resp)
            }
        };

        // Return response if provided
        if let Some(r) = resp {
            // Encode response
            let d = self.net_encode(Some(&from), net::Message::Response(r)).await?;

            // Short-circuit to respond
            msg.reply(&d).await?;
        }

        Ok(())
    }

    pub async fn net_decode(&mut self, data: &[u8]) -> Result<net::Message, DaemonError> {
        // Decode message
        let (container, _n) = Container::from(&data);
        let _id: Id = container.id().into();

        // Parse out message object
        // TODO: pass secret keys for encode / decode here
        let (message, _n) = match net::Message::parse(&data, self) {
            Ok(v) => v,
            Err(e) => {
                error!("Error decoding base message: {:?}", e);
                return Err(e.into());
            }
        };

        // TODO: handle crypto mode errors
        // (eg. message encoded with SYMMETRIC but no pubkey for derivation)


        // Upgrade to symmetric mode on incoming symmetric message
        // TODO: there needs to be another transition for this in p2p comms
        if message.flags().contains(Flags::SYMMETRIC_MODE) {
            self.peers().update(&message.from(), |p| {
                if !p.flags.contains(PeerFlags::SYMMETRIC_ENABLED) {
                    warn!("Enabling symmetricy crypto for peer: {}", message.from());
                    p.flags |= PeerFlags::SYMMETRIC_ENABLED;
                }
                
            });
        }

        Ok(message)
    }

    pub async fn net_encode(&mut self, id: Option<&Id>, mut msg: net::Message) -> Result<Bytes, DaemonError> {
        // Encode response
        let mut buff = vec![0u8; 4096];

        // Fetch cached keys if available, otherwise use service keys
        let (enc_key, sym) = match id.map(|id| (self.keys(id), self.peers().filter(id, |p| p.flags )) ) {
            Some((Some(k), Some(f))) if f.contains(PeerFlags::SYMMETRIC_ENABLED) => (k, true),
            Some((Some(k), _)) => (k, false),
            _ => (self.service().keys(), false)
        };

        if sym {
            *msg.flags_mut() |= Flags::SYMMETRIC_MODE;
        }

        // Encode and sign message
        let n = msg.encode(&enc_key, &mut buff)?;

        Ok(Bytes::from((&buff[..n]).to_vec()))
    }

    pub fn net_op(&mut self, peers: Vec<Peer>, req: net::Request) -> NetFuture {
        let req_id = req.id;

        // Generate requests
        let reqs: HashMap<_, _> = peers
            .iter()
            .map(|p| {
                let peer_rx = self.issue_net_req(p.address().into(), req.clone()).unwrap();
                (p.id(), peer_rx)
            })
            .collect();

        // Create net operation
        let (tx, rx) = mpsc::channel(1);
        let op = NetOp {
            req,
            reqs,
            resps: HashMap::new(),
            done: tx,
            ts: Instant::now(),
        };

        // Register operation
        self.net_ops.insert(req_id, op);

        // Return future
        NetFuture { rx }
    }

    pub(crate) fn poll_net_ops(&mut self, ctx: &mut Context<'_>) {
        // Poll on all pending net operations
        let completed: Vec<_> = self
            .net_ops
            .iter_mut()
            .filter_map(|(req_id, op)| match op.poll_unpin(ctx) {
                Poll::Ready(_) => Some(*req_id),
                _ => None,
            })
            .collect();

        // Remove completed operations
        for req_id in completed {
            self.net_ops.remove(&req_id);
        }
    }

    pub fn issue_net_req(
        &mut self,
        addr: SocketAddr,
        req: net::Request,
    ) -> Result<mpsc::Receiver<NetResponse>, DaemonError> {
        debug!("issuing {} request id: {} to: {:?}", req.data, req.id, addr,);

        let req_id = req.id;

        // Add message to internal tracking
        let (tx, rx) = mpsc::channel(1);
        self.net_requests.insert((addr.into(), req_id), tx);

        // Pass message to sink for transmission
        // TODO: deadlock appears when this line is enabled
        // TODO: pass ID into this
        //#[cfg(deadlock)]
        if let Err(e) = self
            .net_sink
            .try_send((addr.into(), None, NetMessage::Request(req)))
        {
            error!("Request send error: {:?}", e);
            return Err(DaemonError::Unknown);
        }

        // Return future channel
        Ok(rx)
    }

    /// Handle a received response message and generate an (optional) response
    pub fn handle_net_resp(
        &mut self,
        addr: SocketAddr,
        resp: net::Response,
    ) -> Result<Option<net::Response>, DaemonError> {
        let from = resp.from.clone();
        let req_id = resp.id;

        // Generic net message processing here
        let peer = self.handle_base(&from, &addr.into(), &resp.common, Some(SystemTime::now()));

        // Parse out DHT responses
        if let Some(dht_resp) = self.net_to_dht_response(&resp.data) {
            let _ = self.handle_dht_resp(from.clone(), peer, req_id, dht_resp);

            return Ok(None);
        }

        // Look for matching requests
        match self.net_requests.remove(&(addr.into(), req_id)) {
            Some(mut a) => {
                trace!("Found pending request for id {} address: {}", req_id, addr);
                if let Err(e) = a.try_send(resp) {
                    error!(
                        "Error forwarding message for id {} from {}: {:?}",
                        req_id, addr, e
                    );
                }
            }
            None => {
                error!("Received response id {} with no pending request", req_id);
            }
        };

        // TODO: three-way-ack support?

        Ok(None)
    }

    /// Handle a received request message and generate a response
    pub fn handle_net_req(
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
        let peer = self.handle_base(&from, &addr.into(), &req.common, Some(SystemTime::now()));

        // Handle specific DSF and DHT messages
        let mut resp = if let Some(dht_req) = self.net_to_dht_request(&req.data) {
            let dht_resp = self.handle_dht_req(from.clone(), peer, req.id, dht_req)?;

            let net_resp = self.dht_to_net_response(dht_resp);

            net::Response::new(own_id, req_id, net_resp, Flags::default())
        } else {
            let dsf_resp = self.handle_dsf(from.clone(), peer, req.data)?;

            net::Response::new(own_id, req_id, dsf_resp, Flags::default())
        };

        // Generic response processing here

        if flags.contains(Flags::PUB_KEY_REQUEST) {
            resp.common.public_key = Some(our_pub_key);
        }

        // Update peer info
        self.peers().update(&from, |p| p.info.sent += 1);

        trace!("returning response (to: {:?})\n {:?}", from, &resp);

        Ok(resp)
    }

    /// Handles a base message, updating internal state for the sender
    pub(crate) fn handle_base(
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

        // Update key cache
        match (self.key_cache.contains_key(id), &c.public_key) {
            (false, Some(pk)) => {
                if let Ok(k) = self.service().keys().derive_peer(pk.clone()) {
                    self.key_cache.insert(id.clone(), k.clone());
                }
            },
            _ => (),
        }

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
                self.peers()
                    .update(&id, |p| p.info.state = PeerState::Known(pk.clone()));
            }
            _ => (),
        };

        // Update address if specified (and different from existing)
        if let Some(a) = c.remote_address {
            if a != peer.address() {
                info!("Setting explicit address {:?} for peer: {:?}", a, id);
                self.peers()
                    .update(&id, |p| p.info.address = PeerAddress::Explicit(a));
            }
        }

        peer
    }

    /// Handle a DSF type message
    fn handle_dsf(
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
                    match &self
                        .services()
                        .filter(&service_id, |s| s.primary_page.clone())
                        .flatten()
                    {
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

                info!("Register request for service: {} complete", id);

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

                for p in &data {
                    // Apply any updates to the service
                    if p.header().kind().is_page() {
                        self.services().update_inst(&id, |s| {
                            let _ = s.apply_update(p);
                        });
                    }

                    // Store data pages
                    if p.header().kind().is_data() {
                        if let Ok(info) = DataInfo::try_from(p) {
                            // TODO: this is (probably) one of the sloww bits
                            self.data().store_data(&info, p).unwrap();
                        };
                    }
                }

                // Generate data push message
                let req_id = rand::random();
                let req = net::Request::new(
                    self.id(),
                    req_id,
                    net::RequestKind::PushData(id.clone(), data),
                    Flags::default(),
                );

                // Generate peer list for data push
                // TODO: we should be cleverer about this to avoid
                // loops etc. (and prolly add a TTL if it can be repeated?)
                let peer_ids = self.subscribers().find_peers(&id)?;
                let peer_subs: Vec<_> = peer_ids
                    .iter()
                    .filter_map(|peer_id| self.peers().find(peer_id))
                    .collect();

                info!(
                    "Sending data push message id {} to: {:?}",
                    req_id, peer_subs
                );

                // Issue data push requests
                // TODO: we should probably wire the return here to send a delayed PublishInfo to the requester?
                // TODO: deadlock? yeees, deadlock is here or related to this call
                let _ = self.net_op(peer_subs, req);

                info!("Data push complete");

                Ok(net::ResponseKind::Status(net::Status::Ok))
            }
            _ => Err(DaemonError::Unimplemented),
        }
    }
}
