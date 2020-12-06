use std::time::SystemTime;
use std::pin::Pin;
use std::task::{Poll, Context};
use std::future::Future;

use futures::channel::mpsc;
use tracing::{span, Level};
use log::{trace, debug, info, error};

use dsf_core::prelude::*;
use dsf_rpc::{RegisterInfo, RegisterOptions};

use crate::core::services::ServiceState;

use crate::daemon::Dsf;
use crate::error::Error;

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

#[derive(Debug, Clone)]
pub enum RegisterError {
    UnknownService,
    NoPrivateKey,
    Inner(DsfError),
}


pub struct RegisterFuture {
    rx: mpsc::Receiver<Result<RegisterInfo, DsfError>>,
}

impl Future for RegisterFuture {
    type Output = Result<RegisterInfo, DsfError>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
        self.store.poll_next_unpin(ctx)
    }
}


impl Dsf {
    /// Register a locally known service
    pub async fn register(&mut self, options: RegisterOptions) -> Result<RegisterInfo, Error> {
        let span = span!(Level::DEBUG, "register");
        let _enter = span.enter();

        info!("Register: {:?}", &options.service);

        let id = self.resolve_identifier(&options.service)?;

        let mut services = self.services();

        // Generate pages

        // Fetch the known service from the service list
        if self.services().find(&id).is_none() {
            // Only known services can be registered
            error!("unknown service (id: {})", id);
            return Err(Error::UnknownService.into());
        };

        let mut pages = vec![];
        let mut page_version = 0u16;
        let mut replica_version = None;

        // Update service instance
        let _service_info = services.update_inst(&id, |s| {

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
        let store = self.store(&id, pages)?;

        // Update local storage info
        services.update_inst(&id, |s| {
            s.state = ServiceState::Registered;
            s.last_updated = Some(SystemTime::now());
        });

        Ok(RegisterInfo{
            peers,
            page_version,
            replica_version,
        })
    }

    async fn handle_register(&mut self, ctx: &mut RegisterCtx) -> Result<bool, DsfError> {
        let RegisterCtx{opts, state, tx} = ctx;

        match state {
            RegisterState::Init => {
                info!("Register start: {:?}", &opts.service);

                let id = self.resolve_identifier(&opts.service)?;
                let mut services = self.services();
        
                // Generate pages
        
                // Fetch the known service from the service list
                if self.services().find(&id).is_none() {
                    // Only known services can be registered
                    error!("unknown service (id: {})", id);
                    return Err(Error::UnknownService.into());
                };
        
                let mut pages = vec![];
                let mut page_version = 0u16;
                let mut replica_version = None;
        
                // Update service instance
                let _service_info = services.update_inst(&id, |s| {
        
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

                // Update registration state
                state = RegisterState::Pending;
            }
            RegisterState::Pending => {}
            RegisterState::Done => {}
        }

        unimplemented!()
    }
}
