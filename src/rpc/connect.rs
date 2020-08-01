use std::time::Duration;

use tracing::{span, Level};

use kad::prelude::DhtEntry;

use dsf_core::net;
use dsf_core::prelude::*;

use dsf_rpc::{ConnectInfo, ConnectOptions};

use crate::daemon::dht::Ctx;
use crate::daemon::Dsf;
use crate::io;

use crate::core::peers::PeerAddress;
use crate::error::Error as DsfError;

use crate::daemon::dht::TryAdapt;

impl<C> Dsf<C>
where
    C: io::Connector + Clone + Sync + Send + 'static,
{
    pub async fn connect(&mut self, options: ConnectOptions) -> Result<ConnectInfo, DsfError> {
        let span = span!(Level::DEBUG, "connect");
        let _enter = span.enter();

        info!("Connect: {:?}", options.address);

        //        if self.bind_address() == options.address {
        //            warn!("[DSF ({:?})] Cannot connect to self", self.id);
        //        }

        let req_id = rand::random::<u16>();

        // Generate connection request message
        let flag = Flags::ADDRESS_REQUEST | Flags::PUB_KEY_REQUEST;
        let mut req = net::Request::new(self.id(), req_id, net::RequestKind::FindNode(self.id()), flag);

        let service = self.service();

        //TODO: forward address here
        //req.common.remote_address = Some(self.ext_address());

        req.common.public_key = Some(service.public_key());

        let our_id = self.id();
        let _peers = self.peers();
        let _dht = self.dht();

        let address = options.address.clone();

        // Attach public key for bootstrapping use
        //req.public_key = Some(self.service.read().unwrap().public_key().clone());

        // Execute request
        // TODO: could this just be a DHT::connect?

        trace!("Sending request");

        let d = options.timeout.or(Some(Duration::from_secs(2))).unwrap();
        let res = self.request(address, req, d).await;

        // Handle errors
        let resp = match res {
            Ok(r) => r,
            Err(e) => {
                warn!("Error connecting to {:?}: {:?}", address, e);
                return Err(e);
            }
        };

        trace!("response from peer: {:?}", &resp.from);

        // Update peer information and prepare response
        let peer =
            self.peers()
                .find_or_create(resp.from.clone(), PeerAddress::Implicit(address), None);
        let mut info = ConnectInfo {
            id: resp.from.clone(),
            peers: 0,
        };

        trace!("Starting DHT connect");

        // Pass response to DHT to finish connecting
        let mut data = resp.data.try_to((our_id.clone(), self.peers())).unwrap();

        // Drop any entries referring to us
        if let kad::common::Response::NodesFound(_id, nodes) = &mut data {
            nodes.retain(|n| n.id() != &our_id);
        }

        let ctx = Ctx::INCLUDE_PUBLIC_KEY | Ctx::PUB_KEY_REQUEST | Ctx::ADDRESS_REQUEST;
        match self
            .dht()
            .handle_connect_response(DhtEntry::new(resp.from.clone(), peer), data, ctx)
            .await
        {
            Ok(nodes) => {
                // Nodes already added to PeerManager in WireAdaptor
                info.peers = nodes.len();
            }
            Err(e) => {
                error!("connect response error: {:?}", e);
                return Err(DsfError::Unknown);
            }
        }

        info!("Connect complete! {:?}", info);

        Ok(info)
    }
}
