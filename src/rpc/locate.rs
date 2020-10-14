use std::time::SystemTime;

use tracing::{span, Level};

use dsf_core::prelude::*;
use dsf_core::service::Subscriber;
use dsf_rpc::{LocateInfo, LocateOptions};

use crate::core::services::ServiceState;

use crate::daemon::Dsf;
use crate::error::Error;
use crate::io;

impl<C> Dsf<C>
where
    C: io::Connector + Clone + Sync + Send + 'static,
{
    pub async fn locate(&mut self, options: LocateOptions) -> Result<LocateInfo, Error> {
        let span = span!(Level::DEBUG, "locate");
        let _enter = span.enter();

        let mut services = self.services();
        let replica_manager = self.replicas();

        // Skip search for owned services...
        if let Some(service) = services.info(&options.id) {
            if service.origin {
                return Ok(LocateInfo {
                    origin: true,
                    updated: false,
                });
            }
        }

        // Search for associated service pages
        let mut pages = self.search(&options.id).await?;

        //let services = services.clone();

        debug!("locate, found {} pages", pages.len());
        
        // Register new service
        self.service_register(&options.id, pages)?;

        Ok(LocateInfo {
            origin: false,
            updated: true,
        })
    }
}
