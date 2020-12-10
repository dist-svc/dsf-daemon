

use futures::channel::mpsc;

use dsf_rpc::*;

use crate::error::Error;

use super::connect::{ConnectOp, ConnectState};
use super::register::{RegisterOp, RegisterState};

pub type RpcSender = mpsc::Sender<Response>;


/// RPC operation container object
/// Used to track RPC operation kind / state / response etc.
pub struct RpcOperation {
    pub req_id: u64,
    pub kind: RpcKind,
    pub done: RpcSender,
}

pub enum RpcKind {
    Status,
    Connect(ConnectOp),
    Register(RegisterOp),
}

impl RpcKind {
    pub fn connect(opts: ConnectOptions) -> Self {
        RpcKind::Connect(ConnectOp{opts, state: ConnectState::Init})
    }

    pub fn register(opts: RegisterOptions) -> Self {
        RpcKind::Register(RegisterOp{opts, state: RegisterState::Init})
    }
}