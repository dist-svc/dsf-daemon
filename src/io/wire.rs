//! Wire adaptor implements wire encoding/decoding for DSF messages
//! 

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::pin::Pin;


use futures::prelude::*;
use futures::channel::mpsc;
use futures::task::{Poll, Context};

use async_std::future::timeout;
use async_std::task::{self, JoinHandle};

use dsf_core::prelude::*;
use dsf_core::wire::Container;

use crate::error::Error;
use super::Connector;

type RequestMap = Arc<Mutex<HashMap<RequestId, mpsc::Sender<NetResponse>>>>;

/// Wire implements wire encoding/decoding and request/response muxing for DSF messages
pub struct Wire {
    sink: mpsc::Sender<NetMessage>,
    requests: RequestMap,

    stream: mpsc::Receiver<NetMessage>,

    private_key: PrivateKey,
    connections: HashMap<Id, ConnectionInfo>,
}

pub struct ConnectionInfo {

    public_key: Option<PublicKey>,
    secret_key: Option<SecretKey>,
}

impl Wire {
    /// Create a new wire adaptor
    pub fn new(private_key: PrivateKey) -> Self {
        let requests = Arc::new(Mutex::new(HashMap::new()));
        let (sink, mut stream) = mpsc::channel::<NetMessage>(0);

        let connections = HashMap::new();

        Self{requests, sink, stream, private_key, connections}
    }

    /// Create a new connector instance linked to a wire adaptor
    pub fn connector(&self) -> WireConnector {
        WireConnector{ sink: self.sink.clone(), requests: self.requests.clone() }
    }


    pub fn encode(&self, msg: NetMessage) -> Result<Vec<u8>, Error> {
        let mut buff = vec![0u8; 4096];

        // Convert to base message
        let mut b: Base = msg.into();

        // Sign and encode
        let n = b.encode(Some(&self.private_key), None, &mut buff)?;

        Ok((&buff[..n]).to_vec())
    }

    pub fn decode<PK>(&self, data: &[u8], find_pub_key: PK) -> Result<NetMessage, Error> 
    where 
        PK: Fn(&Id) -> Option<PublicKey>,
    {
        
        // Parse base container
        let (container, n) = Container::from(data);
        let id: Id = container.id().into();

        // Parse out base object
        // TODO: pass secret keys for encode / decode here
        let (base, _n) = Base::parse(&data, &find_pub_key, |_id| None )?;

        // TODO: use n here?

        // Convert into message type
        let message = NetMessage::convert(base, &find_pub_key )?;

        Ok(message)
    }


    pub async fn handle(&mut self, resp: NetResponse) -> Result<(), Error> {
        let req_id = resp.id;

        // Find pending request
        let mut a = match self.requests.lock().unwrap().remove(&req_id) {
            Some(a) => a,
            None => {
                error!("Received response id {} with no pending request", req_id);
                return Ok(())
            }
        };

        // Forward response
        a.send(resp).await?;

        Ok(())
    }

}

impl Stream for Wire {
    type Item = NetMessage;

    fn poll_next(mut self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Option<Self::Item>> {
        //let mut stream = self.stream.lock().unwrap();
        Pin::new(&mut self.stream).poll_next(ctx)
    }
}

// WireConnector provides a Connector implementation for DSF use
#[derive(Clone)]
pub struct WireConnector {
    sink: mpsc::Sender<NetMessage>,

    requests: RequestMap,
}

#[async_trait]
impl Connector for WireConnector {
        // Send a request and receive a response or error at some time in the future
        async fn request(
            &self, req_id: RequestId, target: Address, req: NetRequest, t: Duration,
        ) -> Result<NetResponse, Error> {            
            // Create per-request channel
            let (tx, mut rx) = mpsc::channel(0);

            // Add response channel to map
            self.requests.lock().unwrap().insert(req_id, tx);

            // Send message
            let mut sink = self.sink.clone();
            sink.send(NetMessage::Request(req)).await.unwrap();

            // Await and return message
            let res = timeout(t, rx.next() ).await;
            
            // Handle timeouts
            let res = match res {
                Ok(Some(v)) => Ok(v),
                // TODO: this seems like it should be a retry point..?
                Ok(None) => {
                    error!("No response received");
                    Err(Error::Timeout)
                },
                Err(e) => {
                    error!("Response error: {:?}", e);
                    Err(Error::Timeout)
                },
            };

            // Remove request from tracking
            if let Err(_e) = &res {
                self.requests.lock().unwrap().remove(&req_id);
            }

            res
        }
    
        // Send a response message
        async fn respond(
            &self, req_id: RequestId, target: Address, resp: NetResponse,
        ) -> Result<(), Error> {
            unimplemented!()
        }
}

