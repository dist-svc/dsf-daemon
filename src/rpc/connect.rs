use std::time::Duration;
use std::pin::Pin;
use std::task::{Poll, Context};
use std::future::Future;

use tracing::{span, Level};

use log::{debug, info, warn, error};

use futures::prelude::*;
use futures::channel::mpsc;

use kad::prelude::*;

use dsf_core::net;
use dsf_core::prelude::*;

use dsf_rpc::{ConnectInfo, ConnectOptions};

use crate::daemon::Dsf;

use crate::core::peers::{Peer, PeerAddress};
use crate::error::Error as DsfError;


pub enum ConnectState {
    Init,
    Pending,
    Done,
}

pub struct ConnectCtx {
    opts: ConnectOptions,
    state: ConnectState,
    tx: mpsc::Sender<Result<ConnectInfo, DsfError>>,
}

pub struct ConnectFuture {
    connect: kad::dht::ConnectFuture<Id, Peer>,
}

impl Future for ConnectFuture {
    type Output = Result<ConnectInfo, DsfError>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {

        match self.connect.poll_unpin(ctx) {
            Poll::Ready(Ok(r)) => {
                info!("Connect complete! {:?}", r);

                Poll::Ready(Ok(
                    ConnectInfo {
                        id: r[0].info().id().clone(),
                        peers: r.len(),
                    }
                ))
            },
            Poll::Ready(Err(e)) => {
                error!("DHT connect error: {:?}", e);
                Poll::Ready(Err(DsfError::Unknown))
            }
            _ => Poll::Pending,
        }
    }
}


impl Dsf {
    pub fn connect(&mut self, options: ConnectOptions) -> Result<ConnectFuture, DsfError> {
        let span = span!(Level::DEBUG, "connect");
        let _enter = span.enter();

        info!("Connect: {:?}", options.address);

        // Check we're not connecting to ourself
        //        if self.bind_address() == options.address {
        //            warn!("[DSF ({:?})] Cannot connect to self", self.id);
        //        }

        // Generate DHT connect operation
        let (connect, req_id, dht_req) = match self.dht_mut().connect_start() {
            Ok(v) => v,
            Err(e) => {
                error!("Error starting DHT connect: {:?}", e);
                return Err(DsfError::Unknown);
            }
        };

        // Set request flags for initial connection
        let flags = Flags::ADDRESS_REQUEST | Flags::PUB_KEY_REQUEST;

        // Convert into DSF message
        let mut net_req_body = self.dht_to_net_request(dht_req);
        let mut net_req = NetRequest::new(self.id(), req_id, net_req_body, flags);

        // Attach public key for TOFU
        net_req.common.public_key = Some(self.service().public_key());


        // Send message
        // This bypasses DSF state tracking as it is managed by the DHT
        // TODO: this precludes _retries_ and state tracking... find a better solution
        self.net_sink.try_send((options.address.clone().into(), NetMessage::Request(net_req))).unwrap();


        // Return connect future for polling
        Ok(ConnectFuture{
            connect
        })
    }

    async fn handle_connect(&mut self, _ctx: &mut ConnectCtx) {
        //let ConnectCtx{opts, state, tx} = ctx;

        unimplemented!()
    }
}
