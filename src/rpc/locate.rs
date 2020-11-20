use tracing::{span, Level};

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

        // Search for associated service pages
        let pages = self.search(&options.id).await?;

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
