use std::time::SystemTime;
use std::pin::Pin;
use std::task::{Poll, Context};
use std::future::Future;

use futures::prelude::*;
use futures::channel::mpsc;
use tracing::{span, Level};
use log::{trace, debug, info, error};

use dsf_core::prelude::*;
use dsf_rpc::{RegisterInfo, RegisterOptions};

use crate::core::services::ServiceState;
use crate::core::peers::Peer;

use crate::daemon::Dsf;
use crate::error::{Error as DsfError};

#[derive(Copy, Clone, PartialEq, Debug)]
pub enum RegisterState {
    Init,
    Pending,
    Done,
}

pub struct RegisterCtx {
    opts: RegisterOptions,
    state: RegisterState,
    tx: mpsc::Sender<Result<RegisterInfo, DsfError>>
}

pub struct RegisterFuture {
    store: kad::dht::StoreFuture<Id, Peer>,
    page_version: u16,
    replica_version: Option<u16>,
}

impl Future for RegisterFuture {
    type Output = Result<RegisterInfo, DsfError>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.store.poll_unpin(ctx) {
            Poll::Ready(Ok(r)) => {
                debug!("DHT store complete! {:?}", r);

                // TODO: send service update

                Poll::Ready(Ok(RegisterInfo{
                    page_version: self.page_version,
                    replica_version: self.replica_version,
                    peers: r.len(),
                }))
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
    /// Register a locally known service
    pub fn register(&mut self, options: RegisterOptions) -> Result<RegisterFuture, DsfError> {
        let span = span!(Level::DEBUG, "register");
        let _enter = span.enter();

        info!("Register: {:?}", &options.service);

        let id = self.resolve_identifier(&options.service)?;


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
            if !options.no_replica {
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
        // TODO: get store info / number of peers storing
        let (store, _req_id) = match self.dht_mut().store(id, pages) {
            Ok(r) => r,
            Err(e) => {
                error!("DHT store error: {:?}", e);
                return Err(DsfError::Unknown)
            }
        };

        // TODO: Update local instance information
        #[cfg(nope)]
        services.update_inst(&id, |s| {
            s.state = ServiceState::Registered;
            s.last_updated = Some(SystemTime::now());
        });

        Ok(RegisterFuture{
            store,
            page_version,
            replica_version,
        })
    }


}
