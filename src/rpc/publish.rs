

use tracing::{span, Level};

use dsf_core::prelude::*;

use dsf_core::net;
use dsf_core::service::publisher::{Publisher, DataOptionsBuilder};
use dsf_rpc::{PublishOptions, PublishInfo};

use crate::daemon::Dsf;
use crate::error::Error;
use crate::io;


impl <C> Dsf <C> where C: io::Connector + Clone + Sync + Send + 'static {
    /// Register a locally known service
    pub async fn publish(&mut self, options: PublishOptions) -> Result<PublishInfo, Error> {
        let span = span!(Level::DEBUG, "publish");
        let _enter = span.enter();

        // Resolve ID from ID or Index options
        let id = self.resolve_identifier(&options.service)?;

        let mut services = self.services();

        let (info, addresses, req) = {
            // Fetch the known service from the service list
            let service_arc = match services.find(&id) {
                Some(s) => s,
                None => {
                    // Only known services can be registered
                    error!("unknown service (id: {})", id);
                    return Err(Error::UnknownService)
                }
            };

            let mut service_inst = service_arc.write().unwrap();
            let service = &mut service_inst.service;

            // Fetch the private key for signing service pages
            let _private_key = match service.private_key() {
                Some(s) => s,
                None => {
                    // Only known services can be registered
                    error!("no service private key (id: {})", id);
                    return Err(Error::NoPrivateKey)
                }
            };

            let mut data_options = DataOptionsBuilder::default();
            if let Some(kind) = options.kind {
                data_options.data_kind(kind.into());
            }

            if let Some(body) = options.data {
                data_options.body(Body::Cleartext(body));
            }
            
            let data_options = data_options.build().unwrap();

            info!("Generating data page");
            let mut buff = vec![0u8; 1024];
            let (n, mut page) = service.publish_data(data_options, &mut buff).unwrap();
            page.raw = Some(buff[..n].to_vec());

            let info = PublishInfo{
                index: page.version(),
            };

            let service_id = service.id();

            // Drop to ensure mutex is closed out prior to later update
            drop(service);

            info!("Storing data page");
            // Store data against service
            service_inst.add_data(&page)?;
            services.sync_inst(&service_inst);

            // TODO: send data to subscribers
            let req = net::Request::new(self.id(), net::RequestKind::PushData(service_id, vec![page]), Flags::default());
            let addresses: Vec<_> = service_inst.subscribers.iter().map(|(_id, s)| {
                s.peer.address()
            }).collect();

            (info, addresses, req)
        };

        info!("Sending data push messages");
        self.request_all(&addresses, req).await;
          
        // TODO: update info

        Ok(info)
    }

}