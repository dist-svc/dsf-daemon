use std::time::SystemTime;

use tracing::{span, Level};

use kad::store::Datastore;

use dsf_core::options::Options;
use dsf_core::prelude::*;
use dsf_core::service::Publisher;

use dsf_rpc::{CreateOptions, RegisterOptions, ServiceIdentifier};

use crate::core::services::*;
use crate::daemon::Dsf;
use crate::error::Error;
use crate::io;

impl<C> Dsf<C>
where
    C: io::Connector + Clone + Sync + Send + 'static,
{
    /// Create (and publish) a new service
    pub async fn create(&mut self, options: CreateOptions) -> Result<ServiceInfo, Error> {
        let span = span!(Level::DEBUG, "create");
        let _enter = span.enter();

        let services = self.services();

        info!("Creating service: {:?}", options);
        let mut sb = ServiceBuilder::generic();

        // Attach a body if provided
        if let Some(body) = options.body {
            sb = sb.body(body);
        } else {
            sb = sb.body(Body::None);
        }

        // Append addresses as private options
        sb = sb.private_options(
            options
                .addresses
                .iter()
                .map(|v| Options::address(v.clone()))
                .collect(),
        );

        // TODO: append metadata
        for _m in options.metadata {
            //TODO
        }

        // If the service is not public, encrypt the object
        if !options.public {
            sb = sb.encrypt();
        }

        debug!("Generating service");
        let mut service = sb.build().unwrap();
        let id = service.id();

        debug!("Generating service page");
        let mut buff = vec![0u8; 1024];
        let (n, mut primary_page) = service.publish_primary(&mut buff).unwrap();
        primary_page.raw = Some(buff[..n].to_vec());

        // Register service in local database
        debug!("Storing service information");
        let mut info = self
            .services()
            .register(service, &primary_page, ServiceState::Created, None)
            .await
            .unwrap();

        let pages = vec![primary_page];

        // Register service in distributed database
        if !options.register {
            info!("Registering service locally");
            // Write the service to the database
            self.datastore().store(&id, &pages);

        } else {
            info!("Registering and replicating service");
            // TODO URGENT: re-enable this when register is back
            let _register_info = self
                .register(RegisterOptions {
                    service: ServiceIdentifier {
                        id: Some(id.clone()),
                        index: None,
                    },
                    no_replica: false,
                })
                .await?;

            // Update local service state
            info = self.services().update_inst(&id, |s| {
                s.state = ServiceState::Registered;
                s.last_updated = Some(SystemTime::now());
            }).await.unwrap();
        }

        // Return service info
        Ok(info)
    }
}
