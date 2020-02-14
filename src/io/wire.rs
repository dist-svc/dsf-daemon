//! Wire adaptor implements wire encoding/decoding for DSF messages
//! 

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use std::pin::Pin;
use std::task::{Poll, Context};

use futures::prelude::*;
use futures::channel::mpsc;

use async_std::future::timeout;

//use tracing::{Level, span};

use dsf_core::prelude::*;
use dsf_core::net::{Message as DsfMessage, Request as DsfRequest, Response as DsfResponse};
use dsf_core::wire::Container;

use crate::error::Error;
use crate::io::net::NetMessage;

use super::Connector;

type RequestMap = Arc<Mutex<HashMap<RequestId, mpsc::Sender<NetResponse>>>>;

/// Wire implements wire encoding/decoding and request/response muxing for DSF messages
pub struct Wire {
    sink: mpsc::Sender<(Address, DsfMessage)>,
    requests: RequestMap,

    stream: mpsc::Receiver<(Address, DsfMessage)>,

    private_key: PrivateKey,
    connections: HashMap<Id, ConnectionInfo>,
}

/// Connection info object
pub struct ConnectionInfo {
    rx_count: u64,
    addresses: Vec<(Address, SystemTime)>,

    public_key: Option<PublicKey>,
    secret_key: Option<SecretKey>,
}

impl ConnectionInfo {
    fn new() -> Self {
        Self {
            rx_count: 0u64,
            addresses: vec![],

            public_key: None,
            secret_key: None,
        }
    }
}

impl Wire {
    /// Create a new wire adaptor
    pub fn new(private_key: PrivateKey) -> Self {
        let requests = Arc::new(Mutex::new(HashMap::new()));
        let (sink, stream) = mpsc::channel::<(Address, DsfMessage)>(0);

        let connections = HashMap::new();

        Self{requests, sink, stream, private_key, connections}
    }

    /// Create a new connector instance linked to a wire adaptor
    pub fn connector(&self) -> WireConnector {
        WireConnector{ sink: self.sink.clone(), requests: self.requests.clone() }
    }


    pub fn encode(&self, msg: DsfMessage) -> Result<Vec<u8>, Error> {
        let mut buff = vec![0u8; 4096];

        // Convert to base message
        let mut b: Base = msg.into();

        // Sign and encode
        let n = b.encode(Some(&self.private_key), None, &mut buff)?;

        Ok((&buff[..n]).to_vec())
    }

    pub fn decode<PK>(&self, data: &[u8], find_pub_key: PK) -> Result<DsfMessage, Error> 
    where 
        PK: Fn(&Id) -> Option<PublicKey>,
    {
        // Parse base container
        let (container, _n) = Container::from(data);
        let _id: Id = container.id().into();

        // Parse out base object
        // TODO: pass secret keys for encode / decode here
        let (base, _n) = Base::parse(&data, &find_pub_key, |_id| None )?;

        // TODO: use n here?

        // Convert into message type
        let message = DsfMessage::convert(base, &find_pub_key )?;

        Ok(message)
    }

    fn find_pub_key(&self, id: &Id) -> Option<PublicKey> {
        self.connections.get(id).and_then(|i| i.public_key )
    }

    /// Handle incoming messages
    pub async fn handle(&mut self, msg: NetMessage) -> Result<Option<DsfRequest>, Error> {
        trace!("handling message from: {:?}", msg.address);

        // Decode network message to DSF message
        // TODO: provide ID to Key query..?
        let decoded = match self.decode(&msg.data, |id| self.find_pub_key(id) ) {
            Ok(v) => v,
            Err(e) => {
                debug!("Error {:?} decoding message from: {:?}", e, msg.address);
                // TODO: feed back decoding error to stats / rate limiting
                return Ok(None)
            }
        };

        trace!("handling message: {:?} from: {:?}", decoded, msg.address);

        let from_id = decoded.from();
        let info = self.connections.entry(from_id.clone()).or_insert(ConnectionInfo::new());
        info.rx_count += 1;

        if !info.addresses.iter().any(|(addr, _seen)| addr == &msg.address ) {
            info.addresses.push((msg.address.clone(), SystemTime::now()));
        }

        match (info.public_key, decoded.pub_key()) {
            (None, Some(pk)) => {
                debug!("Registering new public key for ID: {:?}", from_id);
                // TODO: emit this information / handle this
                info.public_key = Some(pk);
            },
            (Some(p1), Some(p2)) if p1 != p2 => {
                error!("Public key mismatch for ID: {:?}", from_id);
                // TODO: emit this information / handle this
                return Ok(None)
            }
            _ => (),
        }

        // Pass through requests
        let resp = match decoded {
            DsfMessage::Response(resp) => resp,
            DsfMessage::Request(req) => return Ok(Some(req)),
        };

        // Handle responses
        let req_id = resp.id;

        // Find pending request
        trace!("pending request lock");
        let mut a = match self.requests.lock().unwrap().remove(&req_id) {
            Some(a) => a,
            None => {
                error!("Received response id {} with no pending request", req_id);
                return Ok(None)
            }
        };

        // Forward response
        a.send(resp).await?;

        Ok(None)
    }

    // Send a response message
    pub async fn respond(
        &self, target: Address, resp: DsfResponse,
    ) -> Result<(), Error> {
        
        // Send message
        let mut sink = self.sink.clone();
        sink.send((target, DsfMessage::Response(resp))).await.unwrap();

        Ok(())
    }

    pub fn handle_outgoing(&mut self, target: Address, msg: DsfMessage) -> Result<NetMessage, Error> {

        // Encoding should never fail..?
        // what about oversize etc...
        let data = self.encode(msg).unwrap();

        // TODO: need outgoing IDs here..?
        //let info = self.connections.get_mut(msg.)

        Ok(NetMessage::new(0, target, data.into()))
    }
}

impl Stream for Wire {
    type Item = NetMessage;

    fn poll_next(mut self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Option<Self::Item>> {
        
        let p = Pin::new(&mut self.stream).poll_next(ctx);

        match p {
            Poll::Ready(Some((address, message))) => {
                // Encode outgoing message
                let net_message = self.handle_outgoing(address, message).unwrap();
                Poll::Ready(Some(net_message))
            },
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

// WireConnector provides a Connector implementation for DSF use
#[derive(Clone)]
pub struct WireConnector {
    sink: mpsc::Sender<(Address, DsfMessage)>,

    requests: RequestMap,
}

#[async_trait]
impl Connector for WireConnector {
        // Send a request and receive a response or error at some time in the future
        async fn request(
            &self, req_id: RequestId, target: Address, req: NetRequest, t: Duration,
        ) -> Result<NetResponse, Error> {   
            debug!("issuing request: {:?} (id: {:?}) to: {:?} (expiry {}s)", req, req_id, target, t.as_secs());
            
            // Create per-request channel
            let (tx, mut rx) = mpsc::channel(0);

            // Add response channel to map
            debug!("new request lock");
            self.requests.lock().unwrap().insert(req_id, tx);
            debug!("got new request lock");

            // Pass message to internal sink
            let mut sink = self.sink.clone();
            sink.send((target, DsfMessage::Request(req))).await.unwrap();

            // Await and return message
            let res = timeout(t, rx.next() ).await;
            
            // Handle timeouts
            let res = match res {
                Ok(Some(v)) => {
                    trace!("received response: {:?}", v);
                    Ok(v)
                },
                // TODO: this seems like it should be a retry point..?
                Ok(None) => {
                    debug!("No response received");
                    Err(Error::Timeout)
                },
                Err(e) => {
                    debug!("Response error: {:?}", e);
                    Err(Error::Timeout)
                },
            };

            // Remove request from tracking
            self.requests.lock().unwrap().remove(&req_id);

            res
        }
    
        // Send a response message
        async fn respond(
            &self, _req_id: RequestId, target: Address, resp: NetResponse,
        ) -> Result<(), Error> {
            
            // Send message
            let mut sink = self.sink.clone();
            sink.send((target, DsfMessage::Response(resp))).await.unwrap();

            Ok(())
        }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use async_std::task;

    use tracing_subscriber::{FmtSubscriber, filter::LevelFilter};

    use dsf_core::crypto::{new_pk, hash};
    use dsf_core::net::*;

    use crate::error::BaseError;
    use super::*;

    #[test]
    fn test_wire_encode_decode() {
        let (pub_key_0, pri_key_0) = new_pk().unwrap();
        let id_0 = hash(&pub_key_0).unwrap();

        let w = Wire::new(pri_key_0.clone());

        let req = DsfMessage::Request(NetRequest::new(id_0.clone(), RequestKind::Hello, Flags::empty()));

        let enc = w.encode(req.clone()).unwrap();

        let dec = w.decode(&enc, |_id| Some(pub_key_0) ).unwrap();

        assert_eq!(req, dec);
        assert_eq!(req.request_id(), dec.request_id());
    }

    #[test]
    fn test_wire_encode_decode_error() {
        let (pub_key_0, pri_key_0) = new_pk().unwrap();
        let id_0 = hash(&pub_key_0).unwrap();

        let w = Wire::new(pri_key_0.clone());

        let req = NetRequest::new(id_0.clone(), RequestKind::Hello, Flags::empty());

        let mut enc = w.encode(DsfMessage::request(req.clone())).unwrap();
        enc[100] = 0xff;

        assert_eq!(w.decode(&enc, |_id| Some(pub_key_0) ), Err(Error::Base(BaseError::InvalidSignature)));
    }

    #[test]
    fn test_wire_interop() {
        let _ = FmtSubscriber::builder().with_max_level(LevelFilter::DEBUG).try_init();

        let _addr_0: Address = "127.0.0.1:19993".parse().unwrap();
        let addr_1: Address = "127.0.0.1:19994".parse().unwrap();

        let (pub_key_0, pri_key_0) = new_pk().unwrap();
        let id_0 = hash(&pub_key_0).unwrap();

        let mut w0 = Wire::new(pri_key_0.clone());
        let c0 = w0.connector();

        let (pub_key_1, pri_key_1) = new_pk().unwrap();
        let id_1 = hash(&pub_key_1).unwrap();

        let w1 = Wire::new(pri_key_1.clone());
        let _c1 = w1.connector();

        let req = NetRequest::new(id_0.clone(), RequestKind::Hello, Flags::empty());
        let resp = NetResponse::new(id_1.clone(), req.id, ResponseKind::NoResult, Flags::empty()).with_public_key(pub_key_1);

        let resp_encoded = w1.encode(DsfMessage::response(resp.clone())).unwrap();
        let resp_message = NetMessage::new(0, addr_1, resp_encoded.into());

        task::block_on(async move {
            // Create message passing task
            task::spawn(async move {
                let _req = w0.next().await;
                w0.handle(resp_message).await.unwrap();
            });

            let r1 = c0.request(req.id, addr_1, req.clone().with_public_key(pub_key_1), Duration::from_secs(2)).await.unwrap();

            assert_eq!(r1, resp);

            // No key should be required this time
            //let r2 = c0.request(req.id, addr_1, req.clone(), Duration::from_secs(2)).await.unwrap();
        });
    }

}