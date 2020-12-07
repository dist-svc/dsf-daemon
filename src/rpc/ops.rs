

use futures::channel::mpsc;

use dsf_rpc::*;

use crate::error::Error;

use super::connect::{ConnectOp, ConnectState};

pub type RpcSender = mpsc::Sender<Response>;


/// RPC operation container object
/// Used to track RPC operation kind / state / response etc.
pub struct RpcOperation {
    pub req_id: u64,
    pub kind: RpcKind,
    pub resp: RpcSender,
}

pub enum RpcKind {
    Status,
    Connect(ConnectOp),
}

impl RpcKind {
    pub fn connect(opts: ConnectOptions) -> Self {
        RpcKind::Connect(ConnectOp{opts, state: ConnectState::Init})
    }
}