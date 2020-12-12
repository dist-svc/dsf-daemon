

use futures::channel::mpsc;

use dsf_rpc::*;

use crate::error::Error;

use super::connect::{ConnectOp, ConnectState};
use super::create::{CreateOp, CreateState};
use super::register::{RegisterOp, RegisterState};
use super::locate::{LocateOp, LocateState};

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
    Create(CreateOp),
    Register(RegisterOp),
    Locate(LocateOp),
}

impl RpcKind {
    pub fn connect(opts: ConnectOptions) -> Self {
        RpcKind::Connect(ConnectOp{opts, state: ConnectState::Init})
    }

    pub fn create(opts: CreateOptions) -> Self {
        RpcKind::Create(CreateOp{id: None, opts, state: CreateState::Init})
    }

    pub fn register(opts: RegisterOptions) -> Self {
        RpcKind::Register(RegisterOp{opts, state: RegisterState::Init})
    }

    pub fn locate(opts: LocateOptions) -> Self {
        RpcKind::Locate(LocateOp{opts, state: LocateState::Init})
    }
}