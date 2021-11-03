use std::collections::HashMap;

use kad::prelude::*;

use futures::channel::mpsc;

use dsf_core::net::{RequestKind, ResponseKind};
use dsf_core::prelude::*;
use dsf_core::types::{Data, Id, RequestId};

use super::Dsf;

use crate::core::peers::{Peer, PeerAddress, PeerFlags};
use crate::error::Error;

/// Adaptor to convert between DSF and DHT requests/responses
#[derive(Clone)]
pub struct DhtAdaptor {
    dht_sink: mpsc::Sender<DsfDhtMessage>,
}

pub struct DsfDhtMessage {
    pub(crate) target: DhtEntry<Id, Peer>,
    pub(crate) req: DhtRequest<Id, Data>,
    pub(crate) resp_sink: mpsc::Sender<DhtResponse<Id, Peer, Data>>,
}

impl Dsf {
    /// Handle a DHT request message
    pub(crate) fn handle_dht_req(
        &mut self,
        from: Id,
        peer: Peer,
        req_id: RequestId,
        req: DhtRequest<Id, Data>,
    ) -> Result<DhtResponse<Id, Peer, Data>, Error> {
        // Map peer to existing DHT entry
        // TODO: resolve this rather than creating a new instance
        // (or, use only the index and rely on external storage etc.?)
        let entry = DhtEntry::new(from.into(), peer);

        // Pass to DHT
        let resp = self.dht_mut().handle_req(req_id, &entry, &req).unwrap();

        Ok(resp)
    }

    // Handle a DHT request message
    pub(crate) fn handle_dht_resp(
        &mut self,
        from: Id,
        peer: Peer,
        req_id: RequestId,
        resp: DhtResponse<Id, Peer, Data>,
    ) -> Result<(), Error> {
        // Map peer to existing DHT entry
        // TODO: resolve this rather than creating a new instance
        // (or, use only the index and rely on external storage etc.?)
        let entry = DhtEntry::new(from.into(), peer);

        // Pass to DHT
        let resp = self.dht_mut().handle_resp(req_id, &entry, &resp).unwrap();

        Ok(resp)
    }

    pub(crate) fn is_dht_req(msg: &NetRequest) -> bool {
        match msg.data {
            RequestKind::Ping
            | RequestKind::Store(_, _)
            | RequestKind::FindValue(_)
            | RequestKind::FindNode(_) => true,
            _ => false,
        }
    }

    pub(crate) fn is_dht_resp(msg: &NetResponse) -> bool {
        match msg.data {
            ResponseKind::NoResult
            | ResponseKind::NodesFound(_, _)
            | ResponseKind::ValuesFound(_, _) => true,
            _ => false,
        }
    }

    pub(crate) fn dht_to_net_request(&mut self, req: DhtRequest<Id, Data>) -> NetRequestKind {
        match req {
            DhtRequest::Ping => RequestKind::Ping,
            DhtRequest::FindNode(id) => RequestKind::FindNode(Id::from(id.clone())),
            DhtRequest::FindValue(id) => RequestKind::FindValue(Id::from(id.clone())),
            DhtRequest::Store(id, values) => {
                RequestKind::Store(Id::from(id.clone()), values.to_vec())
            }
        }
    }

    pub(crate) fn dht_to_net_response(
        &mut self,
        resp: DhtResponse<Id, Peer, Data>,
    ) -> NetResponseKind {
        match resp {
            DhtResponse::NodesFound(id, nodes) => {
                let nodes = nodes
                    .iter()
                    .filter_map(|n| {
                        // Drop unseen or nodes without keys from responses
                        // TODO: is this the desired behaviour?
                        if n.info().pub_key().is_none() || n.info().seen().is_none() {
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
            }
            DhtResponse::ValuesFound(id, values) => {
                ResponseKind::ValuesFound(Id::from(id.clone()), values.to_vec())
            }
            DhtResponse::NoResult => ResponseKind::NoResult,
        }
    }

    pub(crate) fn net_to_dht_request(
        &mut self,
        req: &NetRequestKind,
    ) -> Option<DhtRequest<Id, Data>> {
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

    pub(crate) fn net_to_dht_response(
        &mut self,
        resp: &NetResponseKind,
    ) -> Option<DhtResponse<Id, Peer, Data>> {
        // TODO: fix peers:new here peers:new
        match resp {
            ResponseKind::NodesFound(id, nodes) => {
                let mut dht_nodes = Vec::with_capacity(nodes.len());

                for (id, addr, key) in nodes {
                    // Add peer to local tracking
                    let node = self.peers().find_or_create(
                        id.clone(),
                        PeerAddress::Implicit(addr.clone()),
                        Some(key.clone()),
                        PeerFlags::empty(),
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
            PageInfo::Secondary(s) => Some(s.peer_id.clone()),
            PageInfo::Tertiary(t) => Some(t.service_id.clone()),
            PageInfo::Data(_) => None,
        };

        if let Some(id) = id {
            map.insert(id, p);
        }
    }

    // If there is no primary page, drop secondary pages
    // TODO: rethink / re-add this because it doesn't work for secondary 
    // or tertiary pages...
    #[cfg(nope)]
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
