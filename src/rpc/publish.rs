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
use dsf_core::service::publisher::{DataOptions, Publisher};
use dsf_rpc::{self as rpc, DataInfo, PublishInfo, PublishOptions};

use crate::daemon::Dsf;
use crate::core::peers::Peer;
use crate::error::Error;
use super::ops::*;

pub enum PublishState {
    Init,
    Pending(kad::dht::StoreFuture<Id, Peer>),
    Done,
    Error,
}

pub struct PublishOp {
    pub(crate) opts: PublishOptions,
    pub(crate) state: PublishState,
}

pub struct PublishFuture {
    rx: mpsc::Receiver<rpc::Response>,
}


impl Future for PublishFuture {
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
    /// Publish a locally known service
    #[cfg(nope)]
    pub async fn publish(&mut self, options: PublishOptions) -> Result<PublishInfo, Error> {
        let span = span!(Level::DEBUG, "publish");
        let _enter = span.enter();

        // Resolve ID from ID or Index options
        let id = self.resolve_identifier(&options.service)?;

        let mut services = self.services();

        // Fetch the known service from the service list
        let service_info = match services.find(&id) {
            Some(s) => s,
            None => {
                // Only known services can be registered
                error!("unknown service (id: {})", id);
                return Err(Error::UnknownService);
            }
        };


        // Fetch the private key for signing service pages
        let _private_key = match service_info.private_key {
            Some(s) => s,
            None => {
                // Only known services can be registered
                error!("no service private key (id: {})", id);
                return Err(Error::NoPrivateKey);
            }
        };

        // Setup publishing object
        let body = match options.data {
            Some(d) => Body::Cleartext(d),
            None => Body::None,
        };
        let data_options = DataOptions {
            data_kind: options.kind.into(),
            body,
            ..Default::default()
        };

        let mut page: Option<Page> = None;

        services.update_inst(&id, |s| {
            let mut buff = vec![0u8; 1024];
            let opts = data_options.clone();

            info!("Generating data page");
            let mut r = s.service.publish_data(opts, &mut buff).unwrap();

            r.1.raw = Some(buff[..r.0].to_vec());
            page = Some(r.1);
        });

        let page = page.unwrap();

        let info = PublishInfo {
            index: page.header().index(),
        };

        info!("Storing data page");

        // Store new service data
        let data_info = DataInfo::try_from(&page).unwrap();
        self.data().store_data(&data_info, &page)?;


        // Generate push data message
        let req = net::Request::new(
            self.id(),
            rand::random(),
            net::RequestKind::PushData(id.clone(), vec![page]),
            Flags::default(),
        );

        // Generate subscriber address list
        let peers = self.peers();
        let peer_subs = self.subscribers().find_peers(&id)?;
        let mut addresses = Vec::<Address>::with_capacity(peer_subs.len());

        for peer_id in peer_subs {
            if let Some(p) = peers.find(&peer_id) {
                addresses.push(p.address());
            }
        }

        // Push updates to all subscribers
        info!("Sending data push messages");
        self.request_all(&addresses, req).await?;

        // TODO: update info with results

        Ok(info)
    }

    pub fn publish(&mut self, options: PublishOptions) -> Result<PublishFuture, Error> {
        let req_id = rand::random();
        let (tx, rx) = mpsc::channel(1);

        // Create connect object
        let op = RpcOperation {
            req_id,
            kind: RpcKind::publish(options),
            done: tx,
        };

        // Add to tracking
        debug!("Adding RPC op {} to tracking", req_id);
        self.rpc_ops.as_mut().unwrap().insert(req_id, op);

        Ok(PublishFuture{ rx })

    }

    pub fn poll_rpc_publish(&mut self, req_id: u64, register_op: &mut PublishOp, ctx: &mut Context, mut done: mpsc::Sender<rpc::Response>) -> Result<bool, DsfError> {
        let PublishOp{ opts, state } = register_op;

        unimplemented!()
    }
}
