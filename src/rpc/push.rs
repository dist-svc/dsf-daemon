use std::convert::TryFrom;
use std::pin::Pin;
use std::task::{Poll, Context};
use std::future::Future;

use futures::prelude::*;
use futures::channel::mpsc;

use tracing::{span, Level};
use log::{debug, info, error};

use dsf_core::prelude::*;

use dsf_core::net;
use dsf_core::service::publisher::{DataOptions};
use dsf_rpc::{self as rpc, DataInfo, PublishInfo, PushOptions};

use crate::daemon::Dsf;
use crate::core::peers::Peer;
use crate::error::Error;
use super::ops::*;

pub enum PushState {
    Init,
    Pending(kad::dht::StoreFuture<Id, Peer>),
    Done,
    Error,
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
        self.rpc_ops.as_mut().unwrap().insert(req_id, op);

        Ok(PushFuture{ rx })
    }

    pub fn poll_rpc_push(&mut self, req_id: u64, register_op: &mut PushOp, ctx: &mut Context, mut done: mpsc::Sender<rpc::Response>) -> Result<bool, DsfError> {
        let PushOp{ opts, state } = register_op;

        unimplemented!()
    }
}
