
use std::io;

use bytes::Bytes;


use futures::{Stream, Sink, Future};
use futures::stream::{SplitSink};

use actix::prelude::*;



use tokio::codec::Framed;

#[cfg(unix)]
use tokio_uds::{UnixListener, UnixStream};

#[cfg(windows)]
use tokio_uds_windows::{UnixListener, UnixStream};

use daemon_engine::JsonCodec;


use crate::rpc;

use super::{Engine, RpcRequest, RpcResponse};

/// UnixActor sends and receives UnixPacket objects over the internal socket
pub struct UnixActor {
    path: String,
    id: usize,
    engine: Addr<Engine>,
}

#[derive(Debug, Message)]
struct UnixConnect(pub Framed<UnixStream, JsonCodec::<rpc::Response, rpc::Request>>);

/// UnixTx for transmitted Unix messages
#[derive(Clone, Debug, Message)]
pub struct UnixTx(pub usize, pub Bytes);

impl UnixActor {
    /// Create a new incoming UDP actor on the provided address
    pub fn new(path: String, engine: Addr<Engine>) -> Result<Addr<Self>, io::Error> {
        debug!("Creating UnixActor with path: {:?}", path);
        
        let _ = std::fs::remove_file(&path);

        let listener = UnixListener::bind(&path)?;

        Ok(UnixActor::create(move |ctx| {
            // On incoming connections
            ctx.add_stream(listener.incoming().map(|stream| {
                // Create a framed stream
                let framed = Framed::new(stream, JsonCodec::<rpc::Response, rpc::Request>::new());
                // And bind it to a connection object
                UnixConnect(framed)
            }));
            UnixActor{ path, id: 0, engine }
        }))
    }
}

impl Actor for UnixActor {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        debug!("Started UnixActor with path: {:?}", self.path);

        //self.subscribe_sync::<ArbiterBroker, RpcResponse>(ctx);
    }

    fn stopping(&mut self, _ctx: &mut Self::Context) -> Running {
        debug!("Stopped UnixActor with path: {:?}", self.path);

        Running::Stop
    }
}

/// Handler for new connections
impl StreamHandler<UnixConnect, io::Error> for UnixActor {
    fn handle(&mut self, conn: UnixConnect, _: &mut Context<Self>) {
        trace!("Unix {:?} connect from: {:?})", self.path, conn);
        
        // Assign socket connnection ID
        let id = self.id;
        self.id += 1;
        let engine = self.engine.clone();

        // Create an actor to manage a connected RPC session
        UnixSession::create(move |ctx| {
            let (w, r) = conn.0.split();
            let stream = r.map_err(|e| {
                     error!("{:?}", e);
                     io::Error::new(io::ErrorKind::InvalidData, "")
                } );
                
            UnixSession::add_stream(stream, ctx);
            UnixSession{id, sink: w, engine }
        });
    }
}

/// Session actor attached to a specific connection instance
pub struct UnixSession {
    id: usize,
    sink: SplitSink<Framed<UnixStream, JsonCodec<rpc::Response, rpc::Request>>>,
    engine: Addr<Engine>,
}

impl Actor for UnixSession {
    type Context = Context<Self>;
}

/// Handler for received data from a given session
impl StreamHandler<rpc::Request, io::Error> for UnixSession {
    fn handle(&mut self, req: rpc::Request, ctx: &mut Context<Self>) {
        trace!("Unix socket session {} rx: {:?})", self.id, req);
        
        // Send request to engine
        self.engine.do_send(RpcRequest(req, ctx.address()));
    }
}

/// Handler for sending data directly to a given session
impl Handler<RpcResponse> for UnixSession {
    type Result = ();

    fn handle(&mut self, resp: RpcResponse, _: &mut Context<Self>) -> Self::Result {
        trace!("Unix socket session {} tx: {:?})", self.id, resp);
        
        // Forward response to socket instance
        (&mut self.sink).send(resp.0).wait().unwrap();
    }
}
