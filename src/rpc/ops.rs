use futures::Future;
use futures::channel::mpsc;


use dsf_core::prelude::{Page, Id, DsfError as CoreError, Service};
use dsf_core::types::CryptoHash;

use dsf_rpc::*;

use crate::core::peers::Peer;
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
pub enum OpKind {
    DhtGet(Id),
    DhtPut(Id, Vec<Page>),

    ServiceResolve(ServiceIdentifier),
    ServiceGet(Id),
    ServiceUpdate(Id, UpdateFn)
}

impl core::fmt::Debug for OpKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DhtGet(id) => f.debug_tuple("DhtGet").field(id).finish(),
            Self::DhtPut(id, pages) => f.debug_tuple("DhtPut").field(id).field(pages).finish(),
            Self::ServiceResolve(arg0) => f.debug_tuple("ServiceResolve").field(arg0).finish(),
            Self::ServiceGet(id) => f.debug_tuple("ServiceGet").field(id).finish(),
            Self::ServiceUpdate(id, _f) => f.debug_tuple("ServiceUpdate").field(id).finish(),
        }
    }
}

pub type UpdateFn = Box<dyn Fn(&mut Service) -> Result<Res, CoreError> + Send + 'static>;

/// Basic engine response, used to construct higher-level functions
#[derive(Clone, PartialEq, Debug)]
pub enum Res {
    Id(Id),
    Service(Service),
    Pages(Vec<Page>),
    Peers(Vec<Peer>),
    Ids(Vec<Id>,)
}

#[async_trait::async_trait]
pub trait Engine: Sync + Send {
    //type Output: Future<Output=Result<Res, CoreError>> + Send;

    /// Base execute function, non-blocking, returns a future result
    async fn exec(&self, op: OpKind) -> Result<Res, CoreError>;

    /// Search for pages in the DHT
    async fn dht_get(&self, id: Id) -> Result<Vec<Page>, CoreError> {
        match self.exec(OpKind::DhtGet(id)).await? {
            Res::Pages(p) => Ok(p),
            _ => Err(CoreError::Unknown),
        }
    }

    /// Store pages in the DHT
    async fn dht_put(&self, id: Id, pages: Vec<Page>) -> Result<Vec<Id>, CoreError> {
        match self.exec(OpKind::DhtPut(id, pages)).await? {
            Res::Ids(p) => Ok(p),
            _ => Err(CoreError::Unknown),
        }
    }

    /// Resolve a service index to ID
    async fn service_resolve(&self, identifier: ServiceIdentifier) -> Result<Service, CoreError> {
        match self.exec(OpKind::ServiceResolve(identifier)).await? {
            Res::Service(s) => Ok(s),
            _ => Err(CoreError::Unknown),
        }
    }

    /// Fetch a service object by ID
    async fn service_get(&self, service: Id) -> Result<Service, CoreError> {
        match self.exec(OpKind::ServiceGet(service)).await? {
            Res::Service(s) => Ok(s),
            _ => Err(CoreError::Unknown),
        }
    }

    /// Execute an update function on a mutable service instance
    async fn service_update(&self, service: Id, f: UpdateFn) -> Result<Res, CoreError> {
        self.exec(OpKind::ServiceUpdate(service, f)).await
    }
}
