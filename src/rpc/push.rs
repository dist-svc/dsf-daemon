use std::convert::TryFrom;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::channel::mpsc;
use futures::prelude::*;

use log::{debug, error, info};
use tracing::{span, Level};

use dsf_core::prelude::*;

use dsf_core::net;
use dsf_core::service::publisher::DataOptions;
use dsf_rpc::{self as rpc, DataInfo, PublishInfo, PushOptions};

use super::ops::*;
use crate::core::peers::Peer;
use crate::daemon::{net::NetFuture, Dsf};
use crate::error::Error;

pub enum PushState {
    Init,
    Pending(NetFuture),
    Done,
    Error(DsfError),
}

pub struct PushOp {
    pub(crate) opts: PushOptions,
    pub(crate) state: PushState,
}

pub struct PushFuture {
    rx: mpsc::Receiver<rpc::Response>,
}

impl Future for PushFuture {
    type Output = Result<PublishInfo, DsfError>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
        let resp = match self.rx.poll_next_unpin(ctx) {
            Poll::Ready(Some(r)) => r,
            _ => return Poll::Pending,
        };

        match resp.kind() {
            rpc::ResponseKind::Published(r) => Poll::Ready(Ok(r)),
            rpc::ResponseKind::Error(e) => Poll::Ready(Err(e.into())),
            _ => Poll::Pending,
        }
    }
}

impl Dsf {
    /// Push pre-signed data for a locally known service
    pub fn push(&mut self, options: PushOptions) -> Result<PushFuture, Error> {
        let req_id = rand::random();
        let (tx, rx) = mpsc::channel(1);

        // Create connect object
        let op = RpcOperation {
            req_id,
            kind: RpcKind::push(options),
            done: tx,
        };

        // Add to tracking
        debug!("Adding RPC op {} to tracking", req_id);
        self.rpc_ops.insert(req_id, op);

        Ok(PushFuture { rx })
    }

    pub fn poll_rpc_push(
        &mut self,
        req_id: u64,
        register_op: &mut PushOp,
        ctx: &mut Context,
        mut done: mpsc::Sender<rpc::Response>,
    ) -> Result<bool, DsfError> {
        let PushOp { opts, state } = register_op;

        // Resolve ID from ID or Index options
        let id = match self.resolve_identifier(&opts.service) {
            Ok(id) => id,
            Err(_e) => {
                error!("no matching service for");
                return Err(DsfError::UnknownService);
            }
        };

        match state {
            PushState::Init => {
                debug!("Starting push operation");

                // Fetch the known service from the service list
                let service_info = match self.services().find(&id) {
                    Some(s) => s,
                    None => {
                        // Only known services can be registered
                        error!("unknown service (id: {})", id);
                        *state = PushState::Error(DsfError::UnknownService);
                        return Err(DsfError::UnknownService);
                    }
                };

                // Parse out / validate incoming data
                let (base, _n) =
                    match Base::parse(&opts.data, |id| self.find_public_key(id), |_id| None) {
                        Ok(v) => v,
                        Err(e) => {
                            error!("Invalid data for push");
                            return Err(e);
                        }
                    };

                // TODO: check data validity (kind, index, etc.)

                // TODO: Push data to subs
                // (beware of the loop possibilities here)

                Ok(false)
            }
            PushState::Pending(req) => Ok(false),
            _ => Ok(true),
        }
    }
}
