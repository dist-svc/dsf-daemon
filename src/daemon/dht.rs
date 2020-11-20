use std::collections::HashMap;

use kad::prelude::*;

use futures::channel::mpsc;

use dsf_core::net::{Request, RequestKind, ResponseKind};
use dsf_core::prelude::*;
use dsf_core::types::{Data, Flags, Id, RequestId};

use super::Dsf;

use crate::core::peers::{Peer, PeerAddress};

/// Adaptor to convert between DSF and DHT requests/responses
#[derive(Clone)]
pub struct DhtAdaptor {
    dht_sink: mpsc::Sender<DsfDhtMessage>,
}

pub struct DsfDhtMessage {
    target: DhtEntry<Id, Peer>,
    req: DhtRequest<Id, Data>,
    resp_sink: mpsc::Sender<DhtResponse<Id, Peer, Data>>,
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


impl DhtAdaptor {
    pub fn new(dht_sink: mpsc::Sender<DsfDhtMessage>) -> Self {
        DhtAdaptor {
            dht_sink
        }
    }
}

pub fn is_dht_msg(msg: &NetRequest) -> bool {
    match msg.data {
        RequestKind::Ping | 
        RequestKind::Store(_) |
        RequestKind::FindValue(_) |
        RequestKind::FindNode(_) => true,
        _ => false,
    }
}

#[async_trait]
impl DhtConnector<Id, Peer, Data, RequestId, Ctx> for DhtAdaptor {
    // Send a request and receive a response or error at some time in the future
    async fn request(
        &mut self,
        ctx: Ctx,
        _req_id: RequestId,
        target: DhtEntry<Id, Peer>,
        req: DhtRequest<Id, Data>,
    ) -> Result<DhtResponse<Id, Peer, Data>, DhtError> {

        // Create new response channel
        let (resp_source, resp_sink) = mpsc::channel(1);

        let m = DsfDhtMessage{
            target,
            req,
            resp_sink,
        };

        // Send request
        self.dht_sink.send(m).await;

        // Await response
        let resp = resp_source.await;

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

impl Dsf {
    /// Handle a DHT request message
    fn handle_dht(
        &mut self,
        from: Id,
        peer: Peer,
        req: DhtRequest<Id, Data>,
    ) -> Result<DhtResponse<Id, Peer, Data>, anyhow::Error> {
        // TODO: resolve this into existing entry
        let from = DhtEntry::new(from.into(), peer);

        // Pass to DHT
        let resp = self.dht().handle(&from, &req).unwrap();

        Ok(resp)
    }
    

    fn dht_to_net_request(&mut self, req: DhtRequest<Id, Data>) -> Result<Option<NetRequestKind>, anyhow::Error> {

        match req {
            DhtRequest::Ping => RequestKind::Ping,
            DhtRequest::FindNode(id) => RequestKind::FindNode(Id::from(id.clone())),
            DhtRequest::FindValue(id) => RequestKind::FindValue(Id::from(id.clone())),
            DhtRequest::Store(id, values) => {
                RequestKind::Store(Id::from(id.clone()), values.to_vec())
            }
        }
    }

    fn dht_to_net_response(&mut self, resp: DhtResponse<Id, Peer, Data>) -> Result<Option<NetResponseKind>, anyhow::Error> {

        match resp {
            DhtResponse::NodesFound(id, nodes) => {
                
            let nodes = nodes
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
                .collect();
        
                ResponseKind::NodesFound(Id::from(id.clone()), nodes)
            },
            DhtResponse::ValuesFound(id, values) => {
                ResponseKind::ValuesFound(Id::from(id.clone()), values.to_vec())
            }
            DhtResponse::NoResult => ResponseKind::NoResult,
        }

    }

    fn net_to_dht_request(&mut self, req: NetRequestKind) -> Result<Option<DhtRequest<Id, Data>>, anyhow::Error> {
        match req {
            RequestKind::Ping => Some(DhtRequest::Ping),
            RequestKind::FindNode(id) => Some(DhtRequest::FindNode(Id::into(id.clone()))),
            RequestKind::FindValue(id) => Some(DhtRequest::FindValue(Id::into(id.clone()))),
            RequestKind::Store(id, values) => {
                Some(DhtRequest::Store(Id::into(id.clone()), values.to_vec()))
            }
            _ => None,
        }
    }

    fn net_to_dht_response(&mut self, resp: NetResponseKind) -> Result<Option<DhtResponse<Id, Peer, Data>>, anyhow::Error> {

        // TODO: fix peers:new here peers:new
        match resp {
            ResponseKind::NodesFound(id, nodes) => {
                let mut dht_nodes = Vec::with_capacity(nodes.len());

                for (id, addr, key) in nodes {
                    let node = self.peers().find_or_create(
                        id.clone(),
                        PeerAddress::Implicit(addr.clone()),
                        Some(key.clone()),
                    );

                    dht_nodes.push((id.clone(), node).into());
                }

                Some(DhtResponse::NodesFound(Id::into(id.clone()), dht_nodes))
            }
            ResponseKind::ValuesFound(id, values) => Some(DhtResponse::ValuesFound(
                Id::into(id.clone()),
                values.to_vec(),
            )),
            ResponseKind::NoResult => Some(DhtResponse::NoResult),
            _ => None,
        }
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

