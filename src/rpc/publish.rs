use std::convert::TryFrom;

use tracing::{span, Level};

use dsf_core::prelude::*;

use dsf_core::net;
use dsf_core::service::publisher::{Publisher, DataOptions};
use dsf_rpc::{DataInfo, PublishInfo, PublishOptions};

use crate::daemon::Dsf;
use crate::error::Error;
use crate::io;

use crate::core::subscribers::SubscriptionKind;

impl<C> Dsf<C>
where
    C: io::Connector + Clone + Sync + Send + 'static,
{
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
                    return Err(Error::UnknownService);
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
                    return Err(Error::NoPrivateKey);
                }
            };

            let mut data_options = DataOptions{
                data_kind: options.kind.map(|k| k.into()),
                body: options.data.map(Body::Cleartext),
                ..Default::Default()
            };

            info!("Generating data page");
            let mut buff = vec![0u8; 1024];
            let (n, mut page) = service.publish_data(data_options, &mut buff).unwrap();
            page.raw = Some(buff[..n].to_vec());

            let info = PublishInfo {
                index: page.version(),
            };

            let service_id = service.id();

            info!("Storing data page");

            let data_info = DataInfo::try_from(&page).unwrap();

            self.data().store_data(&data_info, &page)?;

            // Store data against service
            //service_inst.add_data(&page)?;
            services.sync_inst(&service_inst);

            // TODO: send data to subscribers
            let req = net::Request::new(
                self.id(),
                net::RequestKind::PushData(service_id.clone(), vec![page]),
                Flags::default(),
            );

            let subscriptions = self.subscribers().find(&service_id)?;

            let addresses: Vec<_> = subscriptions
                .iter()
                .filter_map(|s| {
                    if let SubscriptionKind::Peer(peer_id) = &s.info.kind {
                        self.peers().find(peer_id).map(|p| p.address())
                    } else {
                        None
                    }
                })
                .collect();

            (info, addresses, req)
        };

        info!("Sending data push messages");
        self.request_all(&addresses, req).await;

        // TODO: update info

        Ok(info)
    }
}
