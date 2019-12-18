
use std::net::SocketAddr;
use std::io::Error as IoError;

use std::collections::HashMap;

use futures::prelude::*;
use futures::{select};
use futures::channel::mpsc;

use async_std::task::{self, JoinHandle};
use async_std::net::{TcpListener, TcpStream, UdpSocket};

use tracing::{span, Level};
use tracing_futures::Instrument;

use bytes::Bytes;

pub const UDP_BUFF_SIZE: usize = 4096;

#[derive(Debug, Clone, PartialEq)]
pub enum NetKind {
    Udp,
    Tcp,
}

/// NetCommands to support dynamic binding and unbinding of network
/// interfaces
/// TODO: all of this
#[derive(Debug, Clone, PartialEq)]
pub enum NetCommand {
    Bind(NetKind, SocketAddr),
    Unbind(NetKind, SocketAddr),
}

/// Network message 
#[derive(Debug, Clone, PartialEq)]
pub struct NetMessage {
    pub interface: u32,
    pub address: SocketAddr,
    pub data: Bytes,
}

impl NetMessage {
    pub fn new(interface: u32, address: SocketAddr, data: Bytes) -> Self {
        Self{interface, address, data}
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum NetError {
    NoMatchingInterface,
}

/// Network manager object
pub struct Net {
    bindings: HashMap<u32, Binding>,
    index: u32,

    rx_sink: mpsc::Sender<NetMessage>,
    rx_stream: mpsc::Receiver<NetMessage>,
}

#[derive(Debug)]
struct Binding {
    handle: JoinHandle<Result<(), IoError>>,
    sink: mpsc::Sender<NetMessage>,
    info: NetInfo,
}

/// Network binding information object
#[derive(Debug, Clone, PartialEq)]
pub struct NetInfo {
    kind: NetKind,
    addr: SocketAddr,
}

impl NetInfo {
    pub fn new(addr: SocketAddr, kind: NetKind) -> Self {
        Self{addr, kind}
    }
}

impl Net {
    /// Create a new network manager object
    pub fn new() -> Self {
        let (rx_sink, rx_stream) = mpsc::channel::<NetMessage>(0);

        Net{ bindings: HashMap::new(), index: 0, rx_sink, rx_stream }
    }

    /// List bound network interfaces
    pub fn list(&self) -> Vec<NetInfo> {
        self.bindings.iter().map(|(_k, b)| b.info.clone() ).collect()
    }

    /// Bind to a new interface
    pub async fn bind(&mut self, kind: NetKind, addr: SocketAddr) -> Result<(), IoError> {
        match kind {
            NetKind::Udp => self.listen_udp(addr).await,
            NetKind::Tcp => unimplemented!(),
        }
    }

    /// Unbind from an existing interface
    pub fn unbind(&mut self, interface: u32) -> Result<(), NetError> {
        let _interface = match self.bindings.remove(&interface) {
            Some(v) => v,
            None => return Err(NetError::NoMatchingInterface),
        };

        Ok(())
    }

    /// Send a network message
    async fn send(&mut self, msg: NetMessage) -> Result<(), NetError> {
        let interface = match self.bindings.get_mut(&msg.interface) {
            Some(v) => v,
            None => return Err(NetError::NoMatchingInterface),
        };

        interface.sink.send(msg).await;

        Ok(())
    }

    /// Receive a network message
    async fn receive(&mut self) -> Option<NetMessage> {
        let res = self.rx_stream.next().await;

        res
    }

    /// Start listening on the provided UDP address
    async fn listen_udp(&mut self, address: SocketAddr) -> Result<(), IoError> {
        let socket = UdpSocket::bind(address).await?;
        let interface = self.index;

        let mut rx_sink = self.rx_sink.clone();
        let (mut tx_sink, mut tx_stream) = mpsc::channel::<NetMessage>(0);

        debug!("Starting UDP listener {}: {}", interface, address);

        let handle = task::spawn(async move {
            let mut buff = vec![0u8; UDP_BUFF_SIZE];

            loop {
                select! {
                    res = socket.recv_from(&mut buff).fuse() => {
                        match res {
                            Ok((n, address)) => {
                                let data = Bytes::copy_from_slice(&buff[..n]);
                                event!(Level::TRACE, kind="UDP receive", address = %address, data = ?data);

                                let msg = NetMessage{
                                    interface,
                                    address,
                                    data,
                                };
                                rx_sink.send(msg).await.unwrap()
                            },
                            Err(e) => error!("recieve error: {:?}", e),
                        }
                    },
                    res = tx_stream.next() => {
                        match res {
                            Some(d) => {
                                event!(Level::TRACE, kind="UDP transmit", address = %d.address, data = ?d.data);

                                socket.send_to(&d.data, &d.address).await.unwrap();
                            },
                            None => debug!("tx stream closed"),
                        }
                    }
                }
            }

            Ok(())
        }.instrument(span!(Level::TRACE, "UDP", interface, address=%address)) );

        let binding = Binding{ handle, sink: tx_sink, info: NetInfo::new(address, NetKind::Udp) };

        self.bindings.insert(interface, binding);
        self.index += 1;

        Ok(())
    }

}


#[cfg(test)]
mod test {
    use super::*;

    use tracing_subscriber::FmtSubscriber;

    #[test]
    fn test_udp() {
        let addr_a = "127.0.0.1:19993".parse().unwrap();
        let addr_b = "127.0.0.1:19994".parse().unwrap();

        let _ = FmtSubscriber::builder().with_max_level(Level::DEBUG).try_init();

        task::block_on( async {
            let mut net = Net::new();
            assert_eq!(net.list().len(), 0);

            // Bind to a UDP port
            net.bind(NetKind::Udp, addr_a).await
                .expect("error binding udp interface 1");
            assert_eq!(net.list().len(), 1);

            net.bind(NetKind::Udp, addr_b).await
                .expect("error binding udp interface 2");
            assert_eq!(net.list().len(), 2);

            // Send some messages
            let data = Bytes::copy_from_slice(&[0x11, 0x22, 0x33, 0x44]);

            net.send(NetMessage::new(0, addr_b, data.clone())).await
                .expect("Error sending net message");

            let res = net.receive().await
                .expect("Error awaiting net message");
            
            assert_eq!(res, NetMessage::new(1, addr_a, data.clone()));

            net.send(NetMessage::new(1, addr_a, data.clone())).await
            .expect("Error sending net message");

            let res = net.receive().await
                .expect("Error awaiting net message");
            
            assert_eq!(res, NetMessage::new(0, addr_b, data.clone()));

            // Unbind from UDP port
            net.unbind(0).unwrap();
            net.unbind(1).unwrap();
            assert_eq!(net.list().len(), 0);
        })
    }

}

