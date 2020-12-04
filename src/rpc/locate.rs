use tracing::{span, Level};
use log::{debug, info, warn, error};

use dsf_rpc::{LocateInfo, LocateOptions};

use crate::daemon::Dsf;
use crate::error::Error;

impl Dsf {
    pub async fn locate(&mut self, options: LocateOptions) -> Result<LocateInfo, Error> {
        let span = span!(Level::DEBUG, "locate");
        let _enter = span.enter();

        let services = self.services();

        // Skip search for owned services...
        if let Some(service_info) = services.find(&options.id) {
            if service_info.origin {
                return Ok(LocateInfo {
                    origin: true,
                    updated: false,
                });
            }
        }

        debug!("Starting locate for id: {:?}", &options.id);

        // Search for associated service pages
        let pages = self.search(&options.id).await?;

        //let services = services.clone();

        info!("locate, found {} pages", pages.len());

        // Register new service
        self.service_register(&options.id, pages)?;

        Ok(LocateInfo {
            origin: false,
            updated: true,
        })
    }
}
