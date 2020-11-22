use std::convert::TryFrom;

use tracing::{span, Level};

use dsf_core::prelude::*;

use dsf_core::net;
use dsf_core::service::publisher::{DataOptions, Publisher};
use dsf_rpc::{DataInfo, PublishInfo, PublishOptions};

use crate::daemon::Dsf;
use crate::error::Error;


impl Dsf {
    /// Register a locally known service
    pub async fn publish(&mut self, options: PublishOptions) -> Result<PublishInfo, Error> {
        let span = span!(Level::DEBUG, "publish");
        let _enter = span.enter();

        // Resolve ID from ID or Index options
        let id = self.resolve_identifier(&options.service)?;

        let mut services = self.services();

        // Fetch the known service from the service list
        let service_info = match services.find(&id) {
            Some(s) => s,
            None => {
                // Only known services can be registered
                error!("unknown service (id: {})", id);
                return Err(Error::UnknownService);
            }
        };


        // Fetch the private key for signing service pages
        let _private_key = match service_info.private_key {
            Some(s) => s,
            None => {
                // Only known services can be registered
                error!("no service private key (id: {})", id);
                return Err(Error::NoPrivateKey);
            }
        };

        // Setup publishing object
        let body = match options.data {
            Some(d) => Body::Cleartext(d),
            None => Body::None,
        };
        let data_options = DataOptions {
            data_kind: options.kind.into(),
            body,
            ..Default::default()
        };

        let mut page: Option<Page> = None;

        services.update_inst(&id, |s| {
            let mut buff = vec![0u8; 1024];
            let opts = data_options.clone();

            info!("Generating data page");
            let mut r = s.service.publish_data(opts, &mut buff).unwrap();

            r.1.raw = Some(buff[..r.0].to_vec());
            page = Some(r.1);
        });

        let page = page.unwrap();

        let info = PublishInfo {
            index: page.header().index(),
        };

        info!("Storing data page");

        // Store new service data
        let data_info = DataInfo::try_from(&page).unwrap();
        self.data().store_data(&data_info, &page)?;


        // Generate push data message
        let req = net::Request::new(
            self.id(),
            rand::random(),
            net::RequestKind::PushData(id.clone(), vec![page]),
            Flags::default(),
        );

        // Generate subscriber address list
        let peers = self.peers();
        let peer_subs = self.subscribers().find_peers(&id)?;
        let mut addresses = Vec::<Address>::with_capacity(peer_subs.len());

        for peer_id in peer_subs {
            if let Some(p) = peers.find(&id) {
                addresses.push(p.address());
            }
        }

        // Push updates to all subscribers
        info!("Sending data push messages");
        self.request_all(&addresses, req).await?;

        // TODO: update info with results

        Ok(info)
    }
}
