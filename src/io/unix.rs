
use std::io::Error as IoError;
use std::collections::HashMap;

use futures::prelude::*;
use futures::{select};
use futures::channel::mpsc;

use async_std::task::{self, JoinHandle};
use async_std::os::unix::net::{UnixListener, UnixStream};

use tracing::{span, Level};
use tracing_futures::Instrument;

use dsf_rpc::{Request as RpcRequest, Response as RpcResponse};

#[derive(Debug, Clone, PartialEq)]
pub enum UnixError {

}

#[derive(Debug, Clone, PartialEq)]
pub struct UnixMessage {
    
}

pub struct Unix {
    index: u32,
    connections: HashMap<u32, ()>,
    handle: JoinHandle<Result<(), IoError>>,

    rx_sink: mpsc::Sender<RpcRequest>,
    rx_stream: mpsc::Receiver<RpcRequest>,
}

struct Connection {
    handle: JoinHandle<Result<(), IoError>>,

}

impl Unix {

    /// Create a new unix socket IO connector
    pub async fn new(path: &str) -> Result<Self, IoError> {
        debug!("Creating UnixActor with path: {}", path);

        let _ = std::fs::remove_file(&path);
        let listener = UnixListener::bind(&path).await?;

        let (rx_sink, rx_stream) = mpsc::channel::<RpcRequest>(0);

        let handle = task::spawn(async move {
            let mut incoming= listener.incoming();

            while let Some(stream) = incoming.next().await {
                let stream = stream?;

                Unix::new_connection(stream);
            }

            Ok(())
        });

        Ok(Self{index: 0, connections: HashMap::new(), handle, rx_stream, rx_sink})
    }

    fn new_connection(stream: UnixStream) -> Connection {

        unimplemented!();
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
            
        })
    }

}

