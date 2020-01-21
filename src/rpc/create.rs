

use std::time::SystemTime;

use futures::prelude::*;
use futures::future::{ok, Either};
use tracing::{span, Level};

use kad::store::Datastore;

use dsf_core::prelude::*;
use dsf_core::service::Publisher;
use dsf_core::net;
use dsf_core::options::Options;

use dsf_rpc::{CreateOptions, RegisterOptions, ServiceIdentifier};

use crate::core::services::*;
use crate::daemon::Dsf;
use crate::io;


impl <C> Dsf <C> where C: io::Connector + Clone + Sync + Send + 'static {

    /// Create (and publish) a new service
    pub async fn create(&mut self, options: CreateOptions) -> Result<ServiceInfo, DsfError> {
        let span = span!(Level::DEBUG, "create", "{}", self.id());
        let _enter = span.enter();

        let _services = self.services();

        info!("Creating service: {:?}", options);
        let mut sb = ServiceBuilder::default();

        sb.generic();

        // Attach a body if provided
        if let Some(body) = options.body {
            sb.body(Body::Cleartext(body.data));
        } else {
            sb.body(Body::None);
        }

        // Append addresses as private options
        for a in options.addresses {
            sb.append_private_option(Options::address(a));
        }

        // TODO: append metadata
        for _m in options.metadata {
            //TODO
        }   

        // If the service is not public, encrypt the object
        if !options.public {
            sb.encrypt();
        }

        info!("Generating service");
        let mut service = sb.build().unwrap();
        let id = service.id();

        info!("Generating service page");
        let mut buff = vec![0u8; 1024];
        let (n, mut primary_page) = service.publish_primary(&mut buff).unwrap();
        primary_page.raw = Some(buff[..n].to_vec());

        // Register service in local database
        info!("Storing service information");
        let service = self.services().register(service, &primary_page, ServiceState::Created, None).unwrap();

        let pages = vec![primary_page];
        
        // Register service in distributed database
        if !options.register {
            // Write the service to the database
            self.datastore().store(&id, &pages);
            
        } else {
            info!("Registering and replicating service");
            // TODO URGENT: re-enable this when register is back
            //self.register(RegisterOptions{service: ServiceIdentifier{id: Some(id.clone()), index: None}, no_replica: false }).await?;

            // Update local service state
            let mut s = service.write().unwrap();
            s.state = ServiceState::Registered;
            s.last_updated = Some(SystemTime::now());
        }

        // Return service info
        let s = service.read().unwrap();
        Ok(s.info())
    }
}