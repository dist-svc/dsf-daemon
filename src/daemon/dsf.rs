use crate::sync::{Arc, Mutex};
use std::collections::HashMap;
use std::time::{Duration, SystemTime};

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

use log::{debug, error, info, trace, warn};
use tracing::{span, Level};

use futures::channel::mpsc;
use futures::prelude::*;

use async_std::future::timeout;

use dsf_core::prelude::*;
use dsf_core::service::Publisher;

use kad::prelude::*;
use kad::table::NodeTable;

use crate::core::data::DataManager;
use crate::core::peers::{Peer, PeerManager, PeerState};
use crate::core::replicas::ReplicaManager;
use crate::core::services::{ServiceInfo, ServiceManager, ServiceState};
use crate::core::subscribers::SubscriberManager;

use super::net::NetOp;
use crate::rpc::ops::RpcOperation;

use crate::error::Error;
use crate::store::Store;

use super::dht::{dht_reducer, DsfDhtMessage};
use super::Options;

/// Re-export of Dht type used for DSF
pub type DsfDht = Dht<Id, Peer, Data, RequestId>;

pub struct Dsf {
    /// Inernal storage for daemon service
    service: Service,

    /// Peer manager
    peers: PeerManager,

    /// Service manager
    services: ServiceManager,

    /// Subscriber manager
    subscribers: SubscriberManager,

    /// Replica manager
    replicas: ReplicaManager,

    /// Data manager
    data: DataManager,

    /// Distributed Database
    dht: Dht<Id, Peer, Data, RequestId>,

    dht_source: mpsc::Receiver<(RequestId, DhtEntry<Id, Peer>, DhtRequest<Id, Page>)>,

    store: Store,

    /// Tracking for RPC operations
    pub(crate) rpc_ops: HashMap<u64, RpcOperation>,

    /// Tracking for network operations (collections of requests with retries etc.)
    pub(crate) net_ops: HashMap<u16, NetOp>,

    /// Tracking for individual outgoing network requests
    pub(crate) net_requests: HashMap<(Address, RequestId), mpsc::Sender<NetResponse>>,

    pub(crate) net_sink: mpsc::Sender<(Address, Option<Id>, NetMessage)>,
    //pub(crate) net_source: Arc<Mutex<mpsc::Receiver<(Address, NetMessage)>>>,
    waker: Option<Waker>,

    pub(super) key_cache: HashMap<Id, Keys>,
}

impl Dsf {
    /// Create a new daemon
    pub fn new(
        config: Options,
        service: Service,
        store: Store,
        net_sink: mpsc::Sender<(Address, Option<Id>, NetMessage)>,
    ) -> Result<Self, Error> {
        debug!("Creating new DSF instance");

        // Create managers
        //let store = Arc::new(Mutex::new(Store::new(&config.database_file)?));
        let peers = PeerManager::new(store.clone());
        let services = ServiceManager::new(store.clone());
        let data = DataManager::new(store.clone());

        let replicas = ReplicaManager::new();
        let subscribers = SubscriberManager::new();

        let id = service.id();

        // Instantiate DHT
        let (dht_sink, dht_source) = mpsc::channel(100);
        let dht = Dht::<Id, Peer, Data, RequestId>::standard(id, config.dht, dht_sink);

        // Create DSF object
        let s = Self {
            service,

            peers,
            services,

            subscribers,
            replicas,
            data,

            dht,
            dht_source,

            store,

            rpc_ops: HashMap::new(),

            net_sink,
            net_requests: HashMap::new(),
            net_ops: HashMap::new(),

            waker: None,
            key_cache: HashMap::new(),
        };

        Ok(s)
    }

    /// Fetch the daemon ID
    pub fn id(&self) -> Id {
        self.service.id()
    }

    /// Fetch a reference to the daemon service
    pub fn service(&mut self) -> &mut Service {
        &mut self.service
    }

    pub(crate) fn peers(&mut self) -> &mut PeerManager {
        &mut self.peers
    }

    pub(crate) fn services(&mut self) -> &mut ServiceManager {
        &mut self.services
    }

    pub(crate) fn replicas(&mut self) -> &mut ReplicaManager {
        &mut self.replicas
    }

    pub(crate) fn subscribers(&mut self) -> &mut SubscriberManager {
        &mut self.subscribers
    }

    pub(crate) fn data(&mut self) -> &mut DataManager {
        &mut self.data
    }

    pub(crate) fn dht_mut(&mut self) -> &mut DsfDht {
        &mut self.dht
    }

    pub(crate) fn pub_key(&self) -> PublicKey {
        self.service.public_key()
    }

    pub(crate) fn wake(&self) {
        if let Some(w) = &self.waker {
            w.clone().wake();
        }
    }

    pub(crate) fn primary<T: AsRef<[u8]> + AsMut<[u8]>>(
        &mut self,
        buff: T,
    ) -> Result<(usize, Page), Error> {
        // TODO: this should generate a peer page / contain peer contact info
        self.service.publish_primary(buff).map_err(|e| e.into())
    }

    /// Store pages in the database at the provided ID
    pub fn store(
        &mut self,
        id: &Id,
        pages: Vec<Page>,
    ) -> Result<kad::dht::StoreFuture<Id, Peer>, Error> {
        let span = span!(Level::DEBUG, "store", "{}", self.id());
        let _enter = span.enter();

        // Pre-sign new pages so encoding works
        // TODO: this should not need to be here
        let mut pages = pages.clone();
        for p in &mut pages {
            // We can only sign our own pages...
            if p.id() != &self.id() {
                continue;
            }
            // And we don't need to worry if they're already signed
            if let Some(_s) = p.signature() {
                continue;
            }

            let mut b = Base::from(&*p);
            let mut buff = vec![0u8; 4096];

            let enc_keys = self.service().keys();
            let _n = b.encode(Some(&enc_keys), &mut buff).unwrap();

            p.set_signature(b.signature().clone().unwrap());
        }

        let (store, _req_id) = match self.dht.store(id.clone().into(), pages) {
            Ok(v) => v,
            Err(e) => {
                error!("Error starting DHT store: {:?}", e);
                return Err(Error::Unknown);
            }
        };

        Ok(store)
    }

    /// Search for pages in the database at the provided ID
    pub async fn search(&mut self, id: &Id) -> Result<Vec<Page>, Error> {
        let span = span!(Level::DEBUG, "search", "{}", self.id());
        let _enter = span.enter();

        let (search, _req_id) = match self.dht.search(id.clone().into()) {
            Ok(v) => v,
            Err(e) => {
                error!("Error starting DHT search: {:?}", e);
                return Err(Error::Unknown);
            }
        };

        match search.await {
            Ok(d) => {
                let data = dht_reducer(&d);

                info!("Search complete ({} entries found)", data.len());
                // TODO: use search results
                if data.len() > 0 {
                    Ok(data)
                } else {
                    Err(Error::NotFound)
                }
            }
            Err(e) => {
                error!("Search failed: {:?}", e);
                Err(Error::NotFound)
            }
        }
    }

    /// Run an update of the daemom and all managed services
    pub async fn update(&mut self, force: bool) -> Result<(), Error> {
        info!("DSF update (forced: {:?})", force);

        let interval = Duration::from_secs(10 * 60);

        // Sync data storage
        self.services.sync();

        // Fetch instance lists for each operation
        let register_ops =
            self.services
                .updates_required(ServiceState::Registered, interval, force);
        for _o in &register_ops {
            //self.register(rpc::RegisterOptions{service: rpc::ServiceIdentifier{id: Some(inst.id), index: None}, no_replica: false })
        }

        let update_ops = self
            .services
            .updates_required(ServiceState::Located, interval, force);
        for _o in &update_ops {
            //self.locate(rpc::LocateOptions{id: inst.id}).await;
        }

        let subscribe_ops =
            self.services
                .updates_required(ServiceState::Subscribed, interval, force);
        for _o in &subscribe_ops {
            //self.locate(rpc::LocateOptions{id: inst.id}).await;
            //self.subscribe(rpc::SubscribeOptions{service: rpc::ServiceIdentifier{id: Some(inst.id), index: None}}).await;
        }

        Ok(())
    }

    /// Initialise a DSF instance
    ///
    /// This bootstraps using known peers then updates all tracked services
    #[cfg(nope)]
    pub async fn bootstrap(&mut self) -> Result<(), Error> {
        let peers = self.peers.list();

        info!("DSF bootstrap ({} peers)", peers.len());

        //let mut handles = vec![];

        // Build peer connect requests
        // TODO: switched to serial due to locking issue somewhere,
        // however, it should be possible to execute this in parallel
        let mut success: usize = 0;

        for (id, p) in peers {
            let timeout = Duration::from_millis(200).into();

            if let Ok(_) = self
                .connect(dsf_rpc::ConnectOptions {
                    address: p.address().into(),
                    id: Some(id.clone()),
                    timeout,
                })?
                .await
            {
                success += 1;
            }
        }

        // We're not really worried about failures here
        //let _ = timeout(Duration::from_secs(3), future::join_all(handles)).await;

        info!("DSF bootstrap connect done (contacted {} peers)", success);

        // Run updates
        self.update(true).await?;

        info!("DSF bootstrap update done");

        Ok(())
    }

    pub fn find_public_key(&self, id: &Id) -> Option<PublicKey> {
        if let Some(s) = self.services.find(id) {
            return Some(s.public_key);
        }

        if let Some(p) = self.peers.find(id) {
            if let PeerState::Known(pk) = p.state() {
                return Some(pk);
            }
        }

        if let Some(e) = self.dht.nodetable().contains(id) {
            if let PeerState::Known(pk) = e.info().state() {
                return Some(pk);
            }
        }

        None
    }

    pub fn service_register(&mut self, id: &Id, pages: Vec<Page>) -> Result<ServiceInfo, Error> {
        debug!("Registering service: {}", id);

        debug!("found {} pages", pages.len());
        // Fetch primary page
        let primary_page = match pages.iter().find(|p| {
            let h = p.header();
            h.kind().is_page() && !h.flags().contains(Flags::SECONDARY) && p.id() == id
        }) {
            Some(p) => p.clone(),
            None => return Err(Error::NotFound),
        };

        // Fetch replica pages
        let replicas: Vec<(Id, &Page)> = pages
            .iter()
            .filter(|p| {
                let h = p.header();
                h.kind().is_page()
                    && h.flags().contains(Flags::SECONDARY)
                    && h.application_id() == 0
                    && h.kind() == PageKind::Replica.into()
            })
            .filter_map(|p| {
                let peer_id = match p.info().peer_id() {
                    Some(v) => v,
                    _ => return None,
                };

                Some((peer_id.clone(), p))
            })
            .collect();

        debug!("found {} replicas", replicas.len());

        if primary_page.id() == id {
            debug!("Registering service for matching peer");
        }

        // Fetch service instance
        let info = match self.services.known(id) {
            true => {
                info!("updating existing service");

                // Apply update to known instance
                self.services
                    .update_inst(id, |s| {
                        // Apply primary page update
                        if s.apply_update(&primary_page).unwrap() {
                            s.primary_page = Some(primary_page.clone());
                            s.last_updated = Some(SystemTime::now());
                        }
                    })
                    .unwrap()
            }
            false => {
                info!("creating new service entry");

                // Create instance from page
                let service = match Service::load(&primary_page) {
                    Ok(s) => s,
                    Err(e) => return Err(e.into()),
                };

                // Register in service tracking
                self.services
                    .register(
                        service,
                        &primary_page,
                        ServiceState::Located,
                        Some(SystemTime::now()),
                    )
                    .unwrap()
            }
        };

        debug!("Updating replicas");

        // Update listed replicas
        for (peer_id, page) in &replicas {
            self.replicas.create_or_update(id, peer_id, page);
        }

        debug!("Service registered!");

        Ok(info)
    }
}

impl dsf_core::KeySource for Dsf {
    fn keys(&self, id: &Id) -> Option<dsf_core::Keys> {
        // Check key cache for matching keys
        if let Some(keys) = self.key_cache.get(id) {
            return Some(keys.clone());
        }

        // Find public key from source
        let (pub_key, sec_key) = if let Some(s) = self.services.find(id) {
            (Some(s.public_key), s.secret_key)
        } else if let Some(p) = self.peers.find(id) {
            if let PeerState::Known(pk) = p.state() {
                (Some(pk), None)
            } else {
                (None, None)
            }
        } else if let Some(e) = self.dht.nodetable().contains(id) {
            if let PeerState::Known(pk) = e.info().state() {
                (Some(pk), None)
            } else {
                (None, None)
            }
        } else {
            (None, None)
        };

        match pub_key {
            Some(pk) => {
                let mut keys = Keys::new(pk);
                if let Some(sk) = sec_key {
                    keys.sec_key = Some(sk);
                }
                Some(keys)
            }
            None => None,
        }
    }
}

impl futures::future::FusedFuture for Dsf {
    fn is_terminated(&self) -> bool {
        false
    }
}

impl Future for Dsf {
    type Output = Result<(), DsfError>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
        // Poll for outgoing DHT messages
        if let Poll::Ready(Some((req_id, target, body))) = self.dht_source.poll_next_unpin(ctx) {
            let addr = target.info().address();
            let id = target.info().id();
            let body = self.dht_to_net_request(body);
            let mut flags = Flags::default();

            // Attach public key if required
            if target.info().pub_key().is_none() {
                flags |= Flags::PUB_KEY_REQUEST;
            }

            let mut req = NetRequest::new(self.id(), req_id, body, flags);
            // Attach public key to DHT requests
            req.common.public_key = Some(self.pub_key());

            debug!(
                "Issuing DHT {} request ({}) to {}",
                req.data,
                req_id,
                target.id()
            );

            // Add message to internal tracking
            // We drop the RX channel here because this gets intercepted
            // by net::handle_dht, not sure this is a _great_
            // approach but eh
            let (tx, _rx) = mpsc::channel(1);
            {
                self.net_requests.insert((addr.clone().into(), req_id), tx)
            };

            // Encode and enqueue them
            if let Err(e) = self
                .net_sink
                .try_send((addr, Some(id), NetMessage::Request(req)))
            {
                error!("Error sending outgoing DHT message: {:?}", e);
                return Poll::Ready(Err(DsfError::Unknown));
            }
        }

        // Poll on internal network operations
        self.poll_net_ops(ctx);

        // Poll on internal RPC operations
        // TODO: handle errors?
        let _ = self.poll_rpc(ctx);

        // Poll on internal DHT
        // TODO: handle errors?
        let _ = self.dht_mut().update();

        // TODO: poll on internal state / messages

        // Manage waking
        // TODO: propagate this, in a better manner

        // Always wake (terrible for CPU use)
        //ctx.waker().clone().wake();

        // Store waker
        self.waker = Some(ctx.waker().clone());

        // Indicate we're still running
        Poll::Pending
    }
}
