

use futures::prelude::*;
use futures::future::{err};

use rr_mux::{Connector};


use dsf_core::prelude::*;
use dsf_core::types::Kind;

use dsf_core::net;
use dsf_core::service::publisher::{Publisher, DataOptionsBuilder};

use crate::rpc::{PublishOptions, PublishInfo};
use crate::core::ctx::Ctx;
use crate::daemon::dsf::Dsf;


impl <C> Dsf <C> 
where
    C: Connector<RequestId, Address, net::Request, net::Response, DsfError, Ctx> + Clone + Sync + Send + 'static,
{
    /// Register a locally known service
    pub fn publish(&mut self, options: PublishOptions) -> Box<dyn Future<Item=PublishInfo, Error=DsfError> + Send> {
        let id = match self.resolve_identifier(&options.service) {
            Ok(id) => id,
            Err(e) => return Box::new(err(e)),
        };

        info!("[DSF ({:?})] Publish: {:?}", self.id, &id);
        let _services = self.services.clone();

        // Fetch the known service from the service list
        let service_arc = match self.services.find(&id) {
            Some(s) => s,
            None => {
                // Only known services can be registered
                error!("unknown service (id: {})", id);
                return Box::new(err(DsfError::UnknownService))
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
                return Box::new(err(DsfError::NoPrivateKey))
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
        match service_inst.add_data(&page) {
            Ok(_) => (),
            Err(e) => return Box::new(err(e)),
        }
        self.services.sync_inst(&service_inst);

        // TODO: send data to subscribers
        let req = net::Request::new(self.id.clone(), net::RequestKind::PushData(service_id, vec![page]), Flags::default());
        let addresses: Vec<_> = service_inst.subscribers.iter().map(|(_id, s)| {
            s.peer.address()
        }).collect();

        info!("Sending data push messages");
        Box::new(self.request_all(Ctx::default(), &addresses, req)
            .map(move |_| {
                info
            }))
    }

}