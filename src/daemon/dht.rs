use std::collections::HashMap;
use std::time::Duration;

use kad::prelude::*;

use dsf_core::net::{Request, RequestKind, ResponseKind};
use dsf_core::prelude::*;
use dsf_core::types::{Data, Flags, Id, RequestId};

use crate::core::peers::{Peer, PeerAddress, PeerManager};
use crate::io::Connector;

/// Adaptor to convert between DSF and DHT requests/responses
#[derive(Clone)]
pub struct DhtAdaptor<C> {
    id: Id,
    pub_key: PublicKey,

    peers: PeerManager,

    connector: C,
}

bitflags!(
    /// Contex object for managing outgoing messages
    pub struct Ctx: u32 {
        /// Include public key with outgoing message
        const INCLUDE_PUBLIC_KEY = (1 << 0);
        const ADDRESS_REQUEST    = (1 << 1);
        const PUB_KEY_REQUEST    = (1 << 2);
    }
);

impl<C> DhtAdaptor<C>
where
    C: Connector + Clone + Sync + Send,
{
    pub fn new(id: Id, pub_key: PublicKey, peers: PeerManager, connector: C) -> Self {
        DhtAdaptor {
            id,
            pub_key,
            peers,
            connector,
        }
    }
}

#[async_trait]
impl<C> DhtConnector<Id, Peer, Data, RequestId, Ctx> for DhtAdaptor<C>
where
    C: Connector + Clone + Sync + Send,
{
    // Send a request and receive a response or error at some time in the future
    async fn request(
        &mut self,
        ctx: Ctx,
        _req_id: RequestId,
        target: DhtEntry<Id, Peer>,
        req: DhtRequest<Id, Data>,
    ) -> Result<DhtResponse<Id, Peer, Data>, DhtError> {
        let peers = self.peers.clone();
        let id = self.id.clone();
        let c = self.connector.clone();

        let req_id = rand::random::<u16>();

        // Build DSF Request from DHT request
        let mut net_req = Request::new(self.id.clone(), req_id, req.to(), Flags::default());
        trace!("DHT request: {:?}", req);

        if ctx.contains(Ctx::INCLUDE_PUBLIC_KEY) {
            net_req.set_public_key(self.pub_key.clone());
        }
        if ctx.contains(Ctx::ADDRESS_REQUEST) {
            net_req.flags().insert(Flags::ADDRESS_REQUEST);
        }
        if ctx.contains(Ctx::PUB_KEY_REQUEST) {
            net_req.flags().insert(Flags::PUB_KEY_REQUEST);
        }

        // Issue request and await response
        // TODO: remove timeout duration from here
        let resp = match c
            .request(
                req_id,
                target.info().address().clone(),
                net_req,
                Duration::from_secs(2),
            )
            .await
        {
            Ok(v) => v,
            Err(e) => {
                error!("error issuing DHT request: {:?}", e);
                return Err(DhtError::Connector);
            }
        };

        trace!("DHT response: {:?}", resp);

        // Convert response
        let resp = match resp.data.try_to((id, peers)) {
            Some(v) => v,
            None => {
                error!("error converting response to DHT object");
                return Err(DhtError::Connector);
            }
        };

        Ok(resp)
    }

    // Send a response message
    async fn respond(
        &mut self,
        _ctx: Ctx,
        _req_id: RequestId,
        _target: DhtEntry<Id, Peer>,
        _resp: DhtResponse<Id, Peer, Data>,
    ) -> Result<(), DhtError> {
        unimplemented!()
    }
}

/// Reducer function reduces pages stored in the database
pub(crate) fn dht_reducer(pages: &[Page]) -> Vec<Page> {
    // Build sorted array for filtering
    let mut ordered: Vec<_> = pages.iter().collect();
    ordered.sort_by_key(|p| p.header().index());
    ordered.reverse();

    // Place pages into a map to dedup on ID
    let mut map = HashMap::new();
    let mut svc_id = None;
    for p in ordered {
        let id = match p.info() {
            PageInfo::Primary(_pri) => {
                svc_id = Some(p.id().clone());
                Some(p.id().clone())
            }
            PageInfo::Secondary(sec) => Some(sec.peer_id.clone()),
            PageInfo::Data(_) => None,
        };

        if let Some(id) = id {
            map.insert(id, p);
        }
    }
    // If there is no primary page, drop secondary pages
    if svc_id.is_none() {
        return vec![];
    }

    // TODO: should we be checking page sigs here or earlier?
    // pretty sure it should be earlier...

    // Convert map to array, and remove any invalid pages
    // TODO: this currently removes all pages :-/
    let id: Id = svc_id.unwrap();

    map.iter()
        .filter(|(_k, p)| p.id() == &id)
        .map(|(_k, p)| (*p).clone())
        .collect()
}

/// Adapt trait to allow coercion between different types from unrelated crates
/// without requiring implementations in either (eg. between dsf_core and kad messages)
#[async_trait]
pub trait Adapt<T> {
    async fn to(&self) -> T;
}

/// TryAdapt trait to allow coercion between different types from unrelated crates
/// without requiring implementations in either (eg. between dsf_core and kad messages)
#[async_trait]
pub trait TryAdapt<T, C> {
    async fn try_to(&self, c: C) -> Option<T>;
}

/// Adapt from DhtRequest to RequestKind (outgoing requests)
#[async_trait]
impl Adapt<RequestKind> for DhtRequest<Id, Data> {
    async fn to(&self) -> RequestKind {
        trace!("Adapt: {:?}", self);

        match self {
            DhtRequest::Ping => RequestKind::Ping,
            DhtRequest::FindNode(id) => RequestKind::FindNode(Id::from(id.clone())),
            DhtRequest::FindValue(id) => RequestKind::FindValue(Id::from(id.clone())),
            DhtRequest::Store(id, values) => {
                RequestKind::Store(Id::from(id.clone()), values.to_vec())
            }
        }
    }
}

/// Adapt from DhtResponse to ResponseKind (outgoing responses)
impl Adapt<ResponseKind> for DhtResponse<Id, Peer, Data> {
    async fn to(&self) -> ResponseKind {
        trace!("Adapt: {:?}", self);

        match self {
            DhtResponse::NodesFound(id, nodes) => ResponseKind::NodesFound(
                Id::from(id.clone()),
                nodes
                    .iter()
                    .filter_map(|n| {
                        // Drop nodes without keys from responses
                        // TODO: is this the desired behaviour?
                        if n.info().pub_key().is_none() {
                            None
                        } else {
                            Some((
                                Id::from(n.id().clone()),
                                n.info().address(),
                                n.info().pub_key().unwrap(),
                            ))
                        }
                    })
                    .collect(),
            ),
            DhtResponse::ValuesFound(id, values) => {
                ResponseKind::ValuesFound(Id::from(id.clone()), values.to_vec())
            }
            DhtResponse::NoResult => ResponseKind::NoResult,
        }
    }
}

/// Adapt from RequestKind to DhtRequest (incoming requests)
impl TryAdapt<DhtRequest<Id, Data>, ()> for RequestKind {
    async fn try_to(&self, _c: ()) -> Option<DhtRequest<Id, Data>> {
        trace!("Adapt: {:?}", self);

        match self {
            RequestKind::Ping => Some(DhtRequest::Ping),
            RequestKind::FindNode(id) => Some(DhtRequest::FindNode(Id::into(id.clone()))),
            RequestKind::FindValue(id) => Some(DhtRequest::FindValue(Id::into(id.clone()))),
            RequestKind::Store(id, values) => {
                Some(DhtRequest::Store(Id::into(id.clone()), values.to_vec()))
            }
            _ => None,
        }
    }
}

/// Adapt from ResponseKind to DhtResponse (incoming responses)
impl TryAdapt<DhtResponse<Id, Peer, Data>, (Id, PeerManager)> for ResponseKind {
    async fn try_to(&self, ctx: (Id, PeerManager)) -> Option<DhtResponse<Id, Peer, Data>> {
        trace!("Adapt: {:?}", self);
        let own_id = ctx.0;
        let mut known = ctx.1.clone();

        // TODO: fix peers:new here peers:new
        match self {
            ResponseKind::NodesFound(id, nodes) => Some(DhtResponse::NodesFound(
                Id::into(id.clone()),
                nodes
                    .iter()
                    .filter_map(move |(id, addr, key)| {
                        if id == &own_id {
                            return None;
                        }

                        Some(
                            (
                                Id::into(id.clone()),
                                known.find_or_create(
                                    id.clone(),
                                    PeerAddress::Implicit(addr.clone()),
                                    Some(key.clone()),
                                ).await,
                            )
                                .into(),
                        )
                    })
                    .collect(),
            )),
            ResponseKind::ValuesFound(id, values) => Some(DhtResponse::ValuesFound(
                Id::into(id.clone()),
                values.to_vec(),
            )),
            ResponseKind::NoResult => Some(DhtResponse::NoResult),
            _ => None,
        }
    }
}
