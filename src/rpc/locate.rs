use tracing::{span, Level};

use dsf_rpc::{LocateInfo, LocateOptions};

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

        let services = self.services();

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
