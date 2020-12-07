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

use dsf_rpc::{self as rpc, ConnectInfo, ConnectOptions};

use crate::daemon::Dsf;

use crate::core::peers::{Peer, PeerAddress};
use crate::error::Error as DsfError;


pub enum ConnectState {
    Init,
    Pending(kad::dht::ConnectFuture<Id, Peer>),
    Done,
    Error,
}


pub struct ConnectOp {
    pub(crate) opts: ConnectOptions,
    pub(crate) state: ConnectState,
}


pub struct ConnectFuture {
    rx: mpsc::Receiver<rpc::Response>,
}


impl Future for ConnectFuture {
    type Output = Result<ConnectInfo, DsfError>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
        
        let resp = match self.rx.poll_next_unpin(ctx) {
            Poll::Ready(Some(r)) => r,
            _ => return Poll::Pending,
        };

        match resp.kind() {
            rpc::ResponseKind::Connected(r) => Poll::Ready(Ok(r)),
            rpc::ResponseKind::Error(e) => Poll::Ready(Err(e.into())),
            _ => Poll::Pending,
        }
    }
}


impl Dsf {
    pub fn connect(&mut self, options: ConnectOptions) -> Result<ConnectFuture, DsfError> {
        unimplemented!()
    }

    pub fn poll_rpc_connect(&mut self, connect_op: &mut ConnectOp, ctx: &mut Context) -> Result<bool, DsfError> {
        let ConnectOp{opts, state} = connect_op;

        match state {
            ConnectState::Init => {
                info!("Connect: {:?}", opts.address);

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
                self.net_sink.try_send((opts.address.clone().into(), NetMessage::Request(net_req))).unwrap();

                *state = ConnectState::Pending(connect);

                Ok(false)
            },
            ConnectState::Pending(connect) => {

                match connect.poll_unpin(ctx) {
                    Poll::Ready(Ok(v)) => {
                        debug!("DHT connect complete! {:?}", v);

                        *state = ConnectState::Done;
                    },
                    Poll::Ready(Err(e)) => {
                        error!("DHT connect error: {:?}", e);

                        *state = ConnectState::Error;
                    },
                    _ => (),
                }
                Ok(false)
            }
            ConnectState::Done => {
                Ok(true)
            }
            ConnectState::Error => {
                Ok(true)
            }
        }
    }
}
