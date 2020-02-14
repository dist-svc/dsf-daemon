use std::time::{SystemTime, Duration};
use std::ops::Add;
use std::net::SocketAddr;

use futures::prelude::*;
use async_std::future::timeout;
use tracing::{span, Level};

use dsf_core::prelude::*;
use dsf_core::net;

use kad::prelude::*;

use crate::daemon::Dsf;
use crate::io::Connector;
use crate::error::Error;

use crate::daemon::dht::{Adapt, TryAdapt};

use crate::core::peers::{Peer, PeerAddress, PeerState};


impl <C> Dsf <C> where C: Connector + Clone + Sync + Send + 'static
{
    /// Handle a received request message and generate a response
    pub fn handle(&mut self, addr: SocketAddr, req: net::Request) -> Result<net::Response, Error> {
        let own_id = self.id();

        let span = span!(Level::DEBUG, "id", "{}", own_id);
        let _enter = span.enter();

        let req_id = req.id;
        let flags = req.flags.clone();
        let pub_key = self.service().public_key();
        let from = req.from.clone();
        let from2 = from.clone();

        trace!("handling request (from: {:?} / {})\n {:?}", from, addr, &req);

        // Generic request processing here
        let peer = self.handle_base(&from, &addr, &req.common, Some(SystemTime::now()));

        // Handle specific DSF and DHT messages
        if let Some(kad_req) = req.data.try_to(()) {
            self.handle_dht(from, peer, kad_req)
            .map(move |resp| {
                let kind = resp.to();
                net::Response::new(own_id, req_id, kind, Flags::default())
            })
        } else {
            self.handle_dsf(from, peer, req.data)
            .map(move |kind| {
                net::Response::new(own_id, req_id, kind, Flags::default())
            })

        // Generic response processing here
        }.map(move |mut resp| {
            if flags.contains(Flags::PUB_KEY_REQUEST) {
                resp.common.public_key = Some(pub_key);
            }

            // Update peer info
            if let Some(mut peer) = self.peers().find(&from2) {
                peer.update(|mut p| {
                    p.sent += 1;
                });
            }

            trace!("returning response (to: {:?})\n {:?}", from2, &resp);

            resp
        })
    }

    // Internal function to send a request and await a response
    /// This MUST be used in place of self.connector.clone.request for correct system behaviour
    pub(crate) async fn request(&mut self, address: Address, req: net::Request, timeout: Duration) -> Result<net::Response, Error> {
        let req = req.clone();

        debug!("Sending request to: {:?} request: {:?}", address, &req);

        // Issue request and await response
        let resp = self.connector().request(req.id, address.clone(), req, timeout).await?;
        
        debug!("Received response from: {:?} request: {:?}", &address, &resp);

        // Handle received message
        self.handle_base(&resp.from, &address, &resp.common, Some(SystemTime::now()));

        Ok(resp)
    }

    /// Internal function to send a series of requests
    pub(crate) async fn request_all(&mut self, addresses: &[Address], req: net::Request) -> Vec<Result<net::Response, Error>> {
        
        let mut f = Vec::with_capacity(addresses.len());

        let c = self.connector();

        // Build requests
        for a in addresses {
            let mut req = req.clone();
            req.common.id = RequestId::default();

            let req_future = c.request(req.id, a.clone(), req, Duration::from_secs(10))
                .map(move |resp| (resp, a.clone()) );

            f.push(req_future);
        }

        let mut responses = future::join_all(f).await;

        for (r, a) in &responses {
            if let Ok(resp) = r {
                self.handle_base(&resp.from, a, &resp.common, Some(SystemTime::now()));
            }
        }

        responses.drain(..).map(|(resp, _addr)| resp ).collect()
    }

    /// Internal function to send a response
    /// This MUST be used in place of self.connector.clone.respond for correct system behavior
    pub(crate) async fn respond(&mut self, address: Address, resp: net::Response) -> Result<(), Error> {
        let resp = resp.clone();

        trace!("[DSF ({:?})] Sending response to: {:?} response: {:?}", self.id(), address, &resp);

        timeout(Duration::from_secs(10), self.connector().respond(resp.id, address, resp)).await??;

        Ok(())
    }

    /// Handles a base message, updating internal state for the sender
    pub(crate) fn handle_base(&mut self, id: &Id, address: &Address, c: &net::Common, _seen: Option<SystemTime>) -> Peer {
        trace!("[DSF ({:?})] Handling base message from {:?} {:?} {:?}", self.id(), id, address, c.public_key);

        // Find or create (and push) peer
        let mut peer = self.peers().find_or_create(id.clone(), PeerAddress::Implicit(*address), c.public_key.clone());

        // Update peer info
        peer.update(|mut p| {
            p.set_seen(SystemTime::now());
            p.received += 1;
        });

        assert!(id != &self.id(), "handle_base called for self...");

        trace!("[DSF ({:?})] Peer id: {:?} state: {:?} seen: {:?}", self.id(), id, peer.state(), peer.seen());

        // Add public key if appropriate
        match (peer.state(), c.public_key) {
            (PeerState::Unknown, Some(pk)) => {
                info!("Adding key: {:?} to peer: {:?}", pk, id);
                peer.update(|p| p.set_state(PeerState::Known(pk.clone())) )
            },
            _ => (),
        };

        // Update address if specified (and different from existing)
        if let Some(a) = c.remote_address {
            if a != peer.address() {
                info!("Setting explicit address {:?} for peer: {:?}", a, id);
                peer.update(|p| p.update_address(PeerAddress::Explicit(a)) );
            }
        }

        peer
    }
    

    /// Handle a DSF type message
    fn handle_dsf(&mut self, from: Id, peer: Peer, req: net::RequestKind) -> Result<net::ResponseKind, Error> {
    
        match req {
            net::RequestKind::Hello => {
                Ok(net::ResponseKind::Status(net::Status::Ok))
            },
            net::RequestKind::Subscribe(id) => {
                info!("Subscribe request from: {} for service: {}", from, id);
                let service = match self.services().find(&id) {
                    Some(s) => s,
                    None => {
                        // Only known services can be registered
                        error!("no service found (id: {})", id);
                        return Ok(net::ResponseKind::Status(net::Status::InvalidRequest));
                    }
                };

                let mut service = service.write().unwrap();
                // TODO: pass subscription expiry time through here
                service.update_subscription(id, peer, SystemTime::now(), SystemTime::now().add(Duration::from_secs(3600)));

                Ok(net::ResponseKind::Status(net::Status::Ok))
            },
            net::RequestKind::Query(id) => {
                info!("Query request from: {} for service: {}", from, id);
                let service = match self.services().find(&id) {
                    Some(s) => s,
                    None => {
                        // Only known services can be registered
                        error!("no service found (id: {})", id);
                        return Ok(net::ResponseKind::Status(net::Status::InvalidRequest));
                    }
                };

                let _service = service.write().unwrap();

                // TODO: fetch and return data

                info!("Query request complete");

                Err(Error::Unimplemented)
            },
            net::RequestKind::PushData(id, data) => {
                info!("Data push from: {} for service: {}", from, id);
                let service = match self.services().find(&id) {
                    Some(s) => s,
                    None => {
                        // Only known services can be registered
                        error!("no service found (id: {})", id);
                        return Ok(net::ResponseKind::Status(net::Status::InvalidRequest));
                    }
                };

                let mut service = service.write().unwrap();

                // Store data against service
                service.add_data(&data[0]).unwrap();
                self.services().sync_inst(&service);

                info!("Data push complete");

                Ok(net::ResponseKind::Status(net::Status::Ok))
            },
            _ => {
                Err(Error::Unimplemented)
            }
        }

    }

    /// Handle a DHT request message
    fn handle_dht(&mut self, from: Id, peer: Peer, req: DhtRequest<Id, Data>) -> Result<DhtResponse<Id, Peer, Data>, Error> {
        // TODO: resolve this into existing entry
        let from = DhtEntry::new(from.into(), peer);

        // Pass to DHT
        let resp = self.dht().handle(&from, &req).unwrap();
        
        Ok(resp)
    }
}
