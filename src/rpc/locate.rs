use std::pin::Pin;
use std::task::{Poll, Context};
use std::future::Future;
use std::time::SystemTime;

use futures::prelude::*;
use futures::channel::mpsc;


use tracing::{span, Level};
use log::{debug, info, warn, error};

use dsf_core::prelude::*;
use dsf_rpc::{self as rpc, LocateInfo, LocateOptions};

use crate::daemon::Dsf;
use crate::error::Error;

use crate::core::peers::Peer;
use crate::core::services::ServiceState;


use super::ops::*;

pub enum LocateState {
    Init,
    Pending(kad::dht::SearchFuture<Page>),
    Done,
    Error,
}


pub struct LocateOp {
    pub(crate) opts: LocateOptions,
    pub(crate) state: LocateState,
}


pub struct LocateFuture {
    rx: mpsc::Receiver<rpc::Response>,
}


impl Future for LocateFuture {
    type Output = Result<LocateInfo, DsfError>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
        
        let resp = match self.rx.poll_next_unpin(ctx) {
            Poll::Ready(Some(r)) => r,
            _ => return Poll::Pending,
        };

        match resp.kind() {
            rpc::ResponseKind::Located(r) => Poll::Ready(Ok(r)),
            rpc::ResponseKind::Error(e) => Poll::Ready(Err(e.into())),
            _ => Poll::Pending,
        }
    }
}


impl Dsf {
    pub fn locate(&mut self, options: LocateOptions) -> Result<LocateFuture, Error> {
        let req_id = rand::random();

        let (tx, rx) = mpsc::channel(1);

        // Create connect object
        let op = RpcOperation {
            req_id,
            kind: RpcKind::locate(options),
            done: tx,
        };

        // Add to tracking
        debug!("Adding RPC op {} to tracking", req_id);
        self.rpc_ops.as_mut().unwrap().insert(req_id, op);

        Ok(LocateFuture{ rx })
    }

    pub fn poll_rpc_locate(&mut self, req_id: u64, create_op: &mut LocateOp, ctx: &mut Context, mut done: mpsc::Sender<rpc::Response>) -> Result<bool, DsfError> {
        let LocateOp{opts, state } = create_op;
        
        match state {
            LocateState::Init => {
                // Short-circuit for owned services
                match self.services().find(&opts.id) {
                    Some(service_info) if service_info.origin => {
                        let i = LocateInfo { origin: true, updated: false };

                        let resp = rpc::Response::new(req_id, rpc::ResponseKind::Located(i));
                        done.try_send(resp).unwrap();

                        *state = LocateState::Done;
                    },
                    _ => ()
                }

                // Initiate search via DHT
                let (search, req_id) = match self.dht_mut().search(opts.id.clone()) {
                    Ok(r) => r,
                    Err(e) => {
                        error!("DHT store error: {:?}", e);
                        return Err(DsfError::Unknown)
                    }
                };

                *state = LocateState::Pending(search);
                Ok(false)
            },
            LocateState::Pending(search) => {
                match search.poll_unpin(ctx) {
                    Poll::Ready(Ok(v)) => {
                        debug!("DHT search complete! {:?}", v);

                        // Register or update service
                        let _service_info = match self.service_register(&opts.id, v) {
                            Ok(i) => i,
                            Err(e) => {
                                error!("Error registering located service: {:?}", e);
                                // TODO: handle errors better?
                                *state = LocateState::Error;
                                return Ok(true)
                            }
                        };

                        // Return info
                        let info = LocateInfo { origin: true, updated: false, };
                        let resp = rpc::Response::new(req_id, rpc::ResponseKind::Located(info));
                        done.try_send(resp).unwrap();

                        *state = LocateState::Done;

                        Ok(false)
                    },
                    Poll::Ready(Err(e)) => {
                        error!("DHT search error: {:?}", e);

                        let resp = rpc::Response::new(req_id, rpc::ResponseKind::Error(dsf_core::error::Error::Unknown));

                        *state = LocateState::Error;

                        Ok(false)
                    },
                    Poll::Pending => Ok(false),
                }
            },
            _ => Ok(true)
        }
    }
}
