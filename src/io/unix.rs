
use std::sync::{Arc, Mutex};
use std::io;
use std::collections::HashMap;
use std::pin::Pin;

use futures::prelude::*;
use futures::{select};
use futures::channel::mpsc;
use futures::task::{Poll, Context};

use async_std::task::{self, JoinHandle};
use async_std::os::unix::net::{UnixListener, UnixStream};

use tracing::{span, Level};
use tracing_futures::Instrument;

use bytes::Bytes;

use dsf_rpc::{Request as RpcRequest, Response as RpcResponse};

pub const UNIX_BUFF_LEN: usize = 10 * 1024;

#[derive(Debug)]
pub enum UnixError {
    Io(io::Error),
    Sender(mpsc::SendError),
    NoMatchingConnection
}

impl From<io::Error> for UnixError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<mpsc::SendError> for UnixError {
    fn from(e: mpsc::SendError) -> Self {
        Self::Sender(e)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct UnixMessage {
    pub connection_id: u32,
    pub data: Bytes,
}

impl UnixMessage {
    pub fn new(connection_id: u32, data: Bytes) -> Self {
        Self{connection_id, data}
    }

    pub fn response(&self, data: Bytes) -> Self {
        Self{
            connection_id: self.connection_id,
            data,
        }
    }
}

pub struct Unix {
    connections: Arc<Mutex<HashMap<u32, Connection>>>,
    rx_stream: mpsc::Receiver<UnixMessage>,

    _handle: JoinHandle<Result<(), UnixError>>,
}

struct Connection {
    index: u32,
    sink: mpsc::Sender<UnixMessage>,

    _handle: JoinHandle<Result<(), UnixError>>,
}

impl Unix {

    /// Create a new unix socket IO connector
    pub async fn new(path: &str) -> Result<Self, UnixError> {
        debug!("Creating UnixActor with path: {}", path);

        let _ = std::fs::remove_file(&path);
        let listener = UnixListener::bind(&path).await?;
        let mut index = 0;

        let connections = Arc::new(Mutex::new(HashMap::new()));
        let c = connections.clone();
        let (rx_sink, rx_stream) = mpsc::channel::<UnixMessage>(0);

        let handle = task::spawn(async move {
            let mut incoming= listener.incoming();

            while let Some(stream) = incoming.next().await {
                let stream = stream?;

                let conn = Connection::new(stream, index, rx_sink.clone());

                c.lock().unwrap().insert(index, conn);

                index += 1;
            }

            Ok(())
        }.instrument(span!(Level::TRACE, "UNIX", path)) );

        Ok(Self{connections, rx_stream, _handle: handle})
    }

    /// Send a network message
    pub async fn send(&mut self, msg: UnixMessage) -> Result<(), UnixError> {
        let mut connections = self.connections.lock().unwrap();
        let interface = match connections.get_mut(&msg.connection_id) {
            Some(v) => v,
            None => return Err(UnixError::NoMatchingConnection),
        };

        interface.sink.send(msg).await?;

        Ok(())
    }
}

impl Stream for Unix {
    type Item = UnixMessage;

    fn poll_next(mut self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.rx_stream).poll_next(ctx)
    }
}

impl Connection {
    fn new(stream: UnixStream, index: u32, rx_sink: mpsc::Sender<UnixMessage>) -> Connection {

        let mut stream = stream;
        let mut rx_sink = rx_sink;

        let (tx_sink, tx_stream) = mpsc::channel::<UnixMessage>(0);

        let handle = task::spawn(async move {
            debug!("new connection");

            let mut buff = vec![0u8; UNIX_BUFF_LEN];
            let mut tx_stream = tx_stream.fuse();

            loop {
                select!{
                    tx = tx_stream.next() => {
                        if let Some(tx) = tx {
                            debug!("tx: {:?}", tx.data);
                            stream.write(&tx.data).await?;
                        }
                    },
                    res = stream.read(&mut buff).fuse() => {
                        match res {
                            Ok(n) => {
                                if n == 0 {
                                    continue
                                }

                                debug!("rx: {:?}", &buff[..n]);

                                let u = UnixMessage::new(index, Bytes::copy_from_slice(&buff[..n]));
                                rx_sink.send(u).await?;
                            },
                            Err(e) => {
                                error!("rx error: {:?}", e);
                                break;
                            },
                        }
                    }
                }
            }

            debug!("connection closed {}", index);

            Ok(())
        }.instrument(span!(Level::TRACE, "UNIX", index)) );

        Connection{index, _handle: handle, sink: tx_sink}
    }
}


#[cfg(test)]
mod test {
    use super::*;

    use tracing_subscriber::FmtSubscriber;

    #[test]
    fn test_unix() {

        let _ = FmtSubscriber::builder().with_max_level(Level::DEBUG).try_init();

        task::block_on( async {
            let mut unix = Unix::new("/tmp/dsf-unix-test").await
                .expect("Error creating unix socket listener");

            let mut stream = UnixStream::connect("/tmp/dsf-unix-test").await
                .expect("Error connecting to unix socket");

            let data = Bytes::copy_from_slice(&[0x11, 0x22, 0x33, 0x44]);
            let mut buff = vec![0u8; UNIX_BUFF_LEN];

            // Client to server
            stream.write(&data).await
                .expect("Error writing data");

            let res = unix.next().await
            .expect("Error awaiting unix message");
        
            assert_eq!(res, UnixMessage::new(0, data.clone()));

            // Server to client
            unix.send(UnixMessage::new(0, data.clone())).await
                .expect("Error sending message to client");

            let n = stream.read(&mut buff).await
                .expect("Error reading from client");

            assert_eq!(&buff[..n], &data);

            
        })
    }

}

