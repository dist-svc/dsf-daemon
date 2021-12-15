use futures::Future;
use futures::channel::mpsc;


use dsf_core::prelude::{Page, Id, DsfError as CoreError, Service};
use dsf_core::types::CryptoHash;

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

/// Basic engine operation, used to construct higher-level functions
pub enum Op {
    DhtGet(Id),
    DhtPut(Vec<Page>),

    ServiceGet(Id),

    ServiceUpdate(Id, UpdateFn)
}

impl core::fmt::Debug for Op {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DhtGet(arg0) => f.debug_tuple("DhtGet").field(arg0).finish(),
            Self::DhtPut(arg0) => f.debug_tuple("DhtPut").field(arg0).finish(),
            Self::ServiceGet(arg0) => f.debug_tuple("ServiceGet").field(arg0).finish(),
            Self::ServiceUpdate(arg0, _arg1) => f.debug_tuple("ServiceUpdate").field(arg0).finish(),
        }
    }
}

pub type UpdateFn = Box<dyn Fn(&mut Service) -> Result<Res, CoreError> + Send + 'static>;

/// Basic engine response, used to construct higher-level functions
#[derive(Clone, PartialEq, Debug)]
pub enum Res {
    Pages(Vec<Page>),
    Service(Service),
}

#[async_trait::async_trait]
pub trait Engine: Sync + Send {
    type Output: Future<Output=Result<Res, CoreError>> + Send;

    /// Base execute function, non-blocking, returns a future result
    fn exec(&self, op: Op) -> Self::Output;

    /// Search for pages in the DHT
    async fn dht_get(&self, id: Id) -> Result<Vec<Page>, CoreError> {
        match self.exec(Op::DhtGet(id)).await? {
            Res::Pages(p) => Ok(p),
            _ => Err(CoreError::Unknown),
        }
    }

    /// Store pages in the DHT
    async fn dht_put(&self, pages: Vec<Page>) -> Result<Vec<Page>, CoreError> {
        match self.exec(Op::DhtPut(pages)).await? {
            Res::Pages(p) => Ok(p),
            _ => Err(CoreError::Unknown),
        }
    }

    /// Resolve a service by index or id
    async fn service_get(&self, service: Id) -> Result<Service, CoreError> {
        match self.exec(Op::ServiceGet(service)).await? {
            Res::Service(s) => Ok(s),
            _ => Err(CoreError::Unknown),
        }
    }

    async fn service_update(&self, service: Id, f: UpdateFn) -> Result<Res, CoreError> {
        self.exec(Op::ServiceUpdate(service, f)).await
    }
}
