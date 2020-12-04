use std::time::Duration;
use std::pin::Pin;
use std::task::{Poll, Context};
use std::future::Future;

use tracing::{span, Level};

use log::{debug, info, warn, error};
use futures::prelude::*;

use kad::prelude::DhtEntry;

use dsf_core::net;
use dsf_core::prelude::*;

use dsf_rpc::{ConnectInfo, ConnectOptions};

use crate::daemon::Dsf;

use crate::core::peers::PeerAddress;
use crate::error::Error as DsfError;


pub enum ConnectState {

}

pub struct ConnectFuture {

}

impl Future for ConnectFuture {
    type Output = Result<ConnectInfo, DsfError>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        unimplemented!()
    }
}


impl Dsf {
    pub async fn connect(&mut self, options: ConnectOptions) -> Result<ConnectInfo, DsfError> {
        let span = span!(Level::DEBUG, "connect");
        let _enter = span.enter();

        info!("Connect: {:?}", options.address);

        //        if self.bind_address() == options.address {
        //            warn!("[DSF ({:?})] Cannot connect to self", self.id);
        //        }

        // Generate DHT connect operation
        let (connect, req_id, dht_req) = match self.dht_mut().connect_start() {
            Ok(v) => v,
            Err(e) => {
                error!("Error starting DHT connect: {:?}", e);
                return Err(DsfError::Unknown);
            }
        };

        // Convert into DSF message
        let mut net_req_body = self.dht_to_net_request(dht_req);

        // Set request flags for initial connection
        let flags = Flags::ADDRESS_REQUEST | Flags::PUB_KEY_REQUEST;

        let mut net_req = NetRequest::new(self.id(), req_id, net_req_body, flags);

        // Attach public key for TOFU
        net_req.common.public_key = Some(self.service().public_key());


        // Send message
        // This bypasses DSF state tracking as it is managed by the DHT
        // TODO: not this precludes _retries_ and state tracking
        // Find a better solution...
        self.net_sink.send((options.address.clone().into(), NetMessage::Request(net_req))).await.unwrap();


        // Await DHT connect future
        let peers = match connect.await {
            Ok(n) => n,
            Err(e) => {
                error!("DHT connect error: {:?}", e);
                return Err(DsfError::Unknown)
            }
        };

        let info = ConnectInfo{
            // TODO: placeholder / incorrect response
            id: self.id(),
            peers: peers.len(),
        };
        info!("Connect complete! {:?}", info);

        Ok(info)
    }
}
