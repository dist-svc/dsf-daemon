use std::time::SystemTime;
use std::pin::Pin;
use std::task::{Poll, Context};
use std::future::Future;

use futures::prelude::*;
use futures::channel::mpsc;
use tracing::{span, Level};
use log::{trace, debug, info, error};

use dsf_core::prelude::*;
use dsf_rpc::{self as rpc, RegisterInfo, RegisterOptions};

use crate::core::services::ServiceState;
use crate::core::peers::Peer;

use crate::daemon::Dsf;
use crate::error::{Error as DsfError};
use super::ops::*;

pub enum RegisterState {
    Init,
    Pending(kad::dht::StoreFuture<Id, Peer>),
    Done,
    Error,
}

pub struct RegisterOp {
    pub(crate) opts: RegisterOptions,
    pub(crate) state: RegisterState,
}

pub struct RegisterFuture {
    rx: mpsc::Receiver<rpc::Response>,
}

impl Future for RegisterFuture {
    type Output = Result<RegisterInfo, DsfError>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
        let resp = match self.rx.poll_next_unpin(ctx) {
            Poll::Ready(Some(r)) => r,
            _ => return Poll::Pending,
        };

        match resp.kind() {
            rpc::ResponseKind::Registered(r) => Poll::Ready(Ok(r)),
            rpc::ResponseKind::Error(e) => Poll::Ready(Err(e.into())),
            _ => Poll::Pending,
        }
    }
}


impl Dsf {
    /// Register a locally known service
    pub fn register(&mut self, options: RegisterOptions) -> Result<RegisterFuture, DsfError> {
        let req_id = rand::random();

        let (tx, rx) = mpsc::channel(1);

        // Create connect object
        let op = RpcOperation {
            req_id,
            kind: RpcKind::register(options),
            done: tx,
        };

        // Add to tracking
        debug!("Adding RPC op {} to tracking", req_id);
        self.rpc_ops.as_mut().unwrap().insert(req_id, op);

        Ok(RegisterFuture{ rx })
    }

    pub fn poll_rpc_register(&mut self, req_id: u64, register_op: &mut RegisterOp, ctx: &mut Context, mut done: mpsc::Sender<rpc::Response>) -> Result<bool, DsfError> {
        let RegisterOp{ opts, state } = register_op;

        let id = self.resolve_identifier(&opts.service)?;

        match state {
            RegisterState::Init => {
                info!("Register: {:?}", &opts.service);
        
                // Fetch the known service from the service list
                if self.services().find(&id).is_none() {
                    // Only known services can be registered
                    error!("unknown service (id: {})", id);
                    return Err(DsfError::UnknownService.into());
                };
        
                let mut pages = vec![];
                let mut page_version = 0u16;
                let mut replica_version = None;
        
                // Generate pages / update service instance
                let _service_info = self.services().update_inst(&id, |s| {
        
                    debug!("Generating service page");
                    let primary_page = match s.publish(false) {
                        Ok(v) => v,
                        Err(e) => {
                            error!("Error generating primary page: {:?}", e);
                            return
                        },
                    };
        
                    page_version = primary_page.header().index();
        
                    pages.push(primary_page);
        
                    // Generate replica page unless disabled
                    if !opts.no_replica {
                        debug!("Generating replica page");
        
                        // Generate a replica page
                        let replica_page = match s.replicate(self.service(), false) {
                            Ok(v) => v,
                            Err(e) => {
                                error!("Error generating replica page: {:?}", e);
                                return
                            },
                        };
        
                        replica_version = Some(replica_page.header().index());
        
                        pages.push(replica_page);
                    }
                });
        
        
                debug!("Registering service");
                trace!("Pages: {:?}", pages);
        
                // Store pages
                let (store, _req_id) = match self.dht_mut().store(id, pages) {
                    Ok(r) => r,
                    Err(e) => {
                        error!("DHT store error: {:?}", e);
                        return Err(DsfError::Unknown)
                    }
                };

                *state = RegisterState::Pending(store);
                Ok(false)
            },
            RegisterState::Pending(store) => {
                match store.poll_unpin(ctx) {
                    Poll::Ready(Ok(v)) => {
                        debug!("DHT store complete! {:?}", v);

                        // Update service
                        self.services().update_inst(&id, |s| {
                            s.state = ServiceState::Registered;
                            s.last_updated = Some(SystemTime::now());
                        });

                        // Return info
                        let i = RegisterInfo {
                            // TODO: fix page and replica versions
                            page_version: 0,
                            replica_version: None,
                            peers: v.len(),
                        };

                        let resp = rpc::Response::new(req_id, rpc::ResponseKind::Registered(i));

                        done.try_send(resp).unwrap();

                        *state = RegisterState::Done;

                        Ok(true)
                    },
                    Poll::Ready(Err(e)) => {
                        error!("DHT store error: {:?}", e);

                        let resp = rpc::Response::new(req_id, rpc::ResponseKind::Error(dsf_core::error::Error::Unknown));

                        *state = RegisterState::Error;

                        Ok(true)
                    },
                    _ => Ok(false),
                }
            },
            RegisterState::Done => {
                Ok(true)
            },
            RegisterState::Error => {
                Ok(true)
            }
        }

    }
}
