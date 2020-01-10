

use std::time::SystemTime;

use futures::prelude::*;
use futures::future::{ok, Either};

use rr_mux::Connector;
use kad::store::Datastore;

use dsf_core::prelude::*;
use dsf_core::service::Publisher;
use dsf_core::net;
use dsf_core::options::Options;


use crate::core::ctx::Ctx;
use crate::core::services::*;
use crate::daemon::dsf::Dsf;
use crate::rpc::{CreateOptions, RegisterOptions, ServiceIdentifier};


impl <C> Dsf <C> 
where
    C: Connector<RequestId, Address, net::Request, net::Response, DsfError, Ctx> + Clone + Sync + Send + 'static,
{
    /// Create (and publish) a new service
    pub fn create(&mut self, options: CreateOptions) -> impl Future<Item=ServiceInfo, Error=DsfError> {
        let _services = self.services.clone();

        info!("Creating service: {:?}", options);
        let mut sb = ServiceBuilder::default();

        sb.generic();

        if let Some(body) = options.body {
            sb.body(Body::Cleartext(body.data));
        } else {
            sb.body(Body::None);
        }

        for a in options.addresses {
            sb.append_private_option(Options::address(a));
        }

        for _m in options.metadata {
            //TODO
        }   

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
        let service = self.services.register(service, &primary_page, ServiceState::Created, None).unwrap();

        let pages = vec![primary_page];
        
        // Register service in distributed database
        if !options.register {
            self.store.store(&id, &pages);
            Either::A(ok(service.read().unwrap().info()))
        } else {
            info!("Registering and replicating service");
            Either::B(
                self.register(RegisterOptions{service: ServiceIdentifier{id: Some(id.clone()), index: None}, no_replica: false })
                .map(move |_| {
                    let mut s = service.write().unwrap();
                    s.state = ServiceState::Registered;
                    s.last_updated = Some(SystemTime::now());
                    s.info()
                } )
            )
        }
    }
}