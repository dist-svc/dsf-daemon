use futures::channel::mpsc;

use dsf_rpc::*;

use crate::error::Error;

use super::connect::{ConnectOp, ConnectState};
use super::lookup::{LookupOp, LookupState};

use super::create::{CreateOp, CreateState};
use super::locate::{LocateOp, LocateState};
use super::register::{RegisterOp, RegisterState};

use super::publish::{PublishOp, PublishState};
use super::push::{PushOp, PushState};
use super::subscribe::{SubscribeOp, SubscribeState};

use super::bootstrap::{BootstrapOp, BootstrapState};

pub type RpcSender = mpsc::Sender<Response>;

/// RPC operation container object
/// Used to track RPC operation kind / state / response etc.
pub struct RpcOperation {
    pub req_id: u64,
    pub kind: RpcKind,
    pub done: RpcSender,
}

#[derive(strum_macros::Display)]
pub enum RpcKind {
    Connect(ConnectOp),
    Lookup(LookupOp),
    Create(CreateOp),
    Register(RegisterOp),
    Locate(LocateOp),
    Subscribe(SubscribeOp),
    Publish(PublishOp),
    Push(PushOp),
    Bootstrap(BootstrapOp),
}

impl RpcKind {
    pub fn connect(opts: ConnectOptions) -> Self {
        RpcKind::Connect(ConnectOp {
            opts,
            state: ConnectState::Init,
        })
    }

    pub fn lookup(opts: peer::SearchOptions) -> Self {
        RpcKind::Lookup(LookupOp {
            opts,
            state: LookupState::Init,
        })
    }

    pub fn create(opts: CreateOptions) -> Self {
        RpcKind::Create(CreateOp {
            id: None,
            opts,
            state: CreateState::Init,
        })
    }

    pub fn register(opts: RegisterOptions) -> Self {
        RpcKind::Register(RegisterOp {
            opts,
            state: RegisterState::Init,
        })
    }

    pub fn locate(opts: LocateOptions) -> Self {
        RpcKind::Locate(LocateOp {
            opts,
            state: LocateState::Init,
        })
    }

    pub fn publish(opts: PublishOptions) -> Self {
        RpcKind::Publish(PublishOp {
            opts,
            state: PublishState::Init,
        })
    }

    pub fn subscribe(opts: SubscribeOptions) -> Self {
        RpcKind::Subscribe(SubscribeOp {
            opts,
            state: SubscribeState::Init,
        })
    }

    pub fn push(opts: PushOptions) -> Self {
        RpcKind::Push(PushOp {
            opts,
            state: PushState::Init,
        })
    }

    pub fn bootstrap(opts: ()) -> Self {
        RpcKind::Bootstrap(BootstrapOp {
            opts,
            state: BootstrapState::Init,
        })
    }
}
