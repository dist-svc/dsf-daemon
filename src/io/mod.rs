
use std::net::SocketAddr;
use std::io::Error as IoError;

use std::collections::HashMap;

use futures::prelude::*;
use futures::{select};
use futures::channel::mpsc;

use async_std::task::{self, JoinHandle};
use async_std::net::{TcpListener, TcpStream, UdpSocket};

use tracing::{span, Level};

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

#[derive(Debug, Clone, PartialEq)]
pub struct NetMessage {
    interface: u32,
    address: SocketAddr,
    data: Bytes,
}

pub struct Net {
    bindings: HashMap<u32, Binding>,
    index: u32,
}

#[derive(Debug)]
struct Binding {
    handle: JoinHandle<Result<(), IoError>>,
    info: Info,
}

#[derive(Debug, Clone, PartialEq)]
struct Info {
    kind: NetKind,
    addr: SocketAddr,
}

impl Info {
    pub fn new(addr: SocketAddr, kind: NetKind) -> Self {
        Self{addr, kind}
    }
}

impl Net {
    pub fn new() -> Self {
        Net{ bindings: HashMap::new(), index: 0 }
    }

    /// List bound network interfaces
    pub fn list(&self) -> Result<(), ()> {
        unimplemented!()
    }

    /// Bind to a new interface
    pub fn bind(&mut self, kind: NetKind, addr: SocketAddr) -> Result<(), ()> {
        unimplemented!()
    }

    /// Unbind from an existing interface
    pub fn unbind(&mut self) -> Result<(), ()> {
        unimplemented!()
    }

    async fn listen_tcp(&mut self, addr: SocketAddr) -> Result<(), IoError> {
        let listener = TcpListener::bind(addr).await?;
        let index = self.index;

        debug!("Starting TCP listener {}: {}", index, addr);

        let handle = task::spawn(async move {
            
            let mut incoming = listener.incoming();

            while let Some(stream) = incoming.next().await {
                let stream = stream?;

                task::spawn(async {
                    // TODO: handle new connection
                });

            }

            Ok(())
        });

        let binding = Binding{ handle, info: Info::new(addr, NetKind::Tcp) };

        self.bindings.insert(index, binding);
        self.index += 1;

        Ok(())
    }

    async fn listen_udp(&mut self, addr: SocketAddr) -> Result<(), IoError> {
        let socket = UdpSocket::bind(addr).await?;
        let interface = self.index;

        let (mut rx_sink, mut rx_stream) = mpsc::channel::<NetMessage>(0);
        let (mut tx_sink, mut tx_stream) = mpsc::channel::<NetMessage>(0);

        debug!("Starting UDP listener {}: {}", interface, addr);

        let handle = task::spawn(async move {
            let mut buff = vec![0u8; UDP_BUFF_SIZE];

            loop {
                select! {
                    res = socket.recv_from(&mut buff).fuse() => {
                        trace!("receive: {:?}", res);
                        match res {
                            Ok((n, address)) => {
                                let msg = NetMessage{
                                    interface,
                                    address,
                                    data: Bytes::copy_from_slice(&buff[..n]),
                                };
                                rx_sink.send(msg).await.unwrap()
                            },
                            Err(e) => error!("recieve error: {:?}", e),
                        }
                    },
                    res = tx_stream.next() => {
                        trace!("send: {:?}", res);
                        match res {
                            Some(d) => {
                                socket.send_to(&d.data, &d.address).await.unwrap();
                            },
                            None => (),
                        }
                    }
                }
            }

            Ok(())
        });

        let binding = Binding{ handle, info: Info::new(addr, NetKind::Udp) };

        self.bindings.insert(interface, binding);
        self.index += 1;

        Ok(())
    }

}

