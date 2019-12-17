
use std::net::SocketAddr;
use std::io::Error as IoError;

use std::collections::HashMap;


use futures::prelude::*;

use async_std::task::{self, JoinHandle};
use async_std::net::{TcpListener, TcpStream, UdpSocket};

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
        let index = self.index;


        let handle = task::spawn(async move {
            let mut buff = vec![0u8; UDP_BUFF_SIZE];

            loop {
                let (n, peer) = socket.recv_from(&mut buff).await?;

                // TODO: something here
            }

            Ok(())
        });

        let binding = Binding{ handle, info: Info::new(addr, NetKind::Udp) };

        self.bindings.insert(index, binding);
        self.index += 1;

        Ok(())
    }

}

