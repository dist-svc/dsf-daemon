
use std::time::Duration;

use futures::prelude::*;

use kad::prelude::{DhtEntry};

use dsf_core::prelude::*;
use dsf_core::net;

use dsf_rpc::{ConnectOptions, ConnectInfo};

use crate::daemon::Dsf;
use crate::io;

use crate::error::{Error as DsfError};
use crate::core::{peers::PeerAddress};

use crate::daemon::dht::TryAdapt;



impl <C> Dsf <C> where C: io::Connector + Clone + Sync + Send + 'static
{
    pub async fn connect(&mut self, options: ConnectOptions) -> Result<ConnectInfo, DsfError> {
        info!("[DSF ({:?})] Connect: {:?}", self.id(), options.address);

//        if self.bind_address() == options.address {
//            warn!("[DSF ({:?})] Cannot connect to self", self.id);
//        }

        // Generate connection request message
        let flag = Flags::ADDRESS_REQUEST | Flags::PUB_KEY_REQUEST;
        let mut req = net::Request::new(self.id(), net::RequestKind::FindNode(self.id()), flag);

        let service = self.service();
        req.common.remote_address = Some(self.ext_address());
        req.common.public_key = Some(service.public_key());

        let our_id = self.id();
        let mut peers = self.peers.clone();
        let mut dht = self.dht.clone();

        let address = options.address.clone();

        // Attach public key for bootstrapping use
        //req.public_key = Some(self.service.read().unwrap().public_key().clone());

        // Execute request
        // TODO: could this just be a DHT::connect?

        let timeout = options.timeout.or(Some(Duration::from_secs(3))).unwrap();
        let res = future::timeout(timeout, self.request(ctx.clone(), address, req)).await;

        // Handle errors
        let (resp, _ctx_in) = match res {
            Ok(r) => r,
            Err(e) => {
                warn!("[DSF ({:?})] Connect error: {:?}", our_id, e);
                return Err(DsfError::Timeout)
            }
        };

        // Update peer information and prepare response
        let peer = self.peers().find_or_create(resp.from.clone(), PeerAddress::Implicit(address), None);
        let mut info = ConnectInfo{
            id: resp.from.clone(),
            peers: 0,
        };

        // Pass response to DHT to finish connecting
        let data = resp.data.try_to((our_id, self.peers())).unwrap();
        match self.dht().handle_connect_response(DhtEntry::new(resp.from, peer), data, ctx).await {
            Ok(nodes) => {
                // Nodes already added to PeerManager in WireAdaptor
                info.peers = nodes.len();
            },
            Err(e) => {
                error!("connect response error: {:?}", e);
                return Err(DsfError::Unknown)
            }
        }

        Ok(info)
    }

}