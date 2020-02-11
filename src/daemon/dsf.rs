

use std::sync::{Arc, Mutex};
use std::time::Duration;

use dsf_core::prelude::*;
use dsf_core::service::Publisher;

use kad::prelude::*;

use async_std::future::timeout;
use tracing::{Level, span};

use crate::core::peers::{Peer, PeerManager};
use crate::core::services::{ServiceManager};

use crate::io::Connector;
use crate::store::Store;
use crate::error::Error;

use super::Options;
use super::dht::{Ctx, DhtAdaptor, dht_reducer};

/// Re-export of Dht type used for DSF
pub type Dht<C> = StandardDht<Id, Peer, Data, RequestId, DhtAdaptor<C>, Ctx>;

#[derive(Clone)]
pub struct Dsf<C> {
    /// Inernal storage for daemon service 
    service: Service,

    /// Peer manager
    peers: PeerManager,

    /// Service manager
    services: ServiceManager,

    /// Distributed Database
    dht: StandardDht<Id, Peer, Data, RequestId, DhtAdaptor<C>, Ctx>,

    /// Backing store for DHT
    dht_store: HashMapStore<Id, Data>,

    store: Arc<Mutex<Store>>,

    /// Connector for external communication
    connector: C,
}


impl <C> Dsf <C> where C: Connector + Clone + Sync + Send + 'static
{
    /// Create a new daemon
    pub fn new(config: Options, service: Service, store: Arc<Mutex<Store>>, connector: C) -> Result<Self, Error> {

        debug!("Creating new DSF instance");

        // Create managers
        //let store = Arc::new(Mutex::new(Store::new(&config.database_file)?));
        let peers = PeerManager::new(store.clone());
        let services = ServiceManager::new(store.clone());

        let id = service.id();

        // Create DHT components
        let dht_conn = DhtAdaptor::new(service.id(), service.public_key(), peers.clone(), connector.clone());
        let table = KNodeTable::new(service.id(), config.dht.k, id.max_bits());
        let dht_store = HashMapStore::new_with_reducer(Box::new(dht_reducer));

        // Instantiate DHT
        let dht = StandardDht::<Id, Peer, Data, RequestId, _, Ctx>::new(id, config.dht, table, dht_conn, dht_store.clone());

        // Create DSF object
        let s = Self {
            service,
            peers,
            services,
            dht,
            dht_store,
            store,
            connector,
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

    pub(crate) fn peers(&self) -> PeerManager {
        self.peers.clone()
    }

    pub(crate) fn services(&mut self) -> ServiceManager {
        self.services.clone()
    }

    pub(crate) fn connector(&mut self) -> &mut C {
        &mut self.connector
    }

    pub(crate) fn dht(&mut self) -> &mut Dht<C> {
        &mut self.dht
    }

    pub(crate) fn datastore(&mut self) -> &mut HashMapStore<Id, Data> {
        &mut self.dht_store
    }
    
    pub(crate) fn pub_key(&self) -> PublicKey {
        self.service.public_key()
    }

    pub(crate) fn primary<T: AsRef<[u8]> + AsMut<[u8]>>(&mut self, buff: T) -> Result<(usize, Page), Error> {
        // TODO: this should generate a peer page / contain peer contact info
        self.service.publish_primary(buff).map_err(|e| e.into() )
    }

    /// Initialise a DSF instance
    pub async fn bootstrap(&mut self) -> Result<(), Error> {
        
        unimplemented!();
    }

    /// Store pages in the database at the provided ID
    pub async fn store(&mut self, id: &Id, pages: Vec<Page>) -> Result<(), Error> {

        let span = span!(Level::DEBUG, "store", "{}", self.id());
        let _enter = span.enter();

        // Pre-sign new pages so encoding works
        // TODO: this should not be here
        let mut pages = pages.clone();
        for p in &mut pages {
            if p.id() != &self.id() {
                continue
            }
            if let Some(_s) = p.signature() {
                continue
            }

            let mut b = Base::from(&*p);
            let mut buff = vec![0u8; 4096];
            let _n = b.encode(None, None, &mut buff).unwrap();
            p.set_signature(b.signature().clone().unwrap());
        }

        match timeout(Duration::from_secs(20), self.dht.store(id.clone().into(), pages, Ctx::PUB_KEY_REQUEST)).await? {
            Ok(n) => {
                debug!("Store complete ({} peers)", n);
                // TODO: use search results
                Ok(())
            },
            Err(e) => {
                error!("Store failed: {:?}", e);
                Err(Error::NotFound)
            }
        }
    }

    /// Search for pages in the database at the provided ID
    pub async fn search(&mut self, id: &Id) -> Result<Vec<Page>, Error> {

        let span = span!(Level::DEBUG, "search", "{}", self.id());
        let _enter = span.enter();
        
        match timeout(Duration::from_secs(20), self.dht.find(id.clone().into(), Ctx::PUB_KEY_REQUEST)).await? {
            Ok(d) => {
                let data = dht_reducer(&d);

                info!("Search complete ({} entries found)", data.len());
                // TODO: use search results
                if data.len() > 0 {
                    Ok(data)
                } else {
                    Err(Error::NotFound)
                }
            },
            Err(e) => {
                error!("Search failed: {:?}", e);
                    return Err(Error::NotFound)
            }
        }        
    }

    /// Look up a peer in the database
    pub async fn lookup(&mut self, id: &Id) -> Result<Peer, Error> {
        let span = span!(Level::DEBUG, "lookup", "{}", self.id());
        let _enter = span.enter();

        match timeout(Duration::from_secs(20), self.dht.lookup(id.clone().into(), Ctx::empty())).await? {
            Ok(n) => {
                debug!("Lookup complete: {:?}", n.info());
                // TODO: use search results
                Ok(n.info().clone())
            },
            Err(e) => {
                error!("Lookup failed: {:?}", e);
                Err(Error::NotFound)
            }
        }
    }

    /// Run an update of the daemom and all managed services
    pub async fn update(&mut self, force: bool) -> Result<(), Error> {
        use crate::core::services::ServiceState;
        
        let interval = Duration::from_secs(10 * 60);

        // Sync data storage
        self.services.sync();

        // Fetch instance lists for each operation
        let register_ops = self.services.updates_required(ServiceState::Registered, interval, force);
        for _o in &register_ops {
            //self.register(rpc::RegisterOptions{service: rpc::ServiceIdentifier{id: Some(inst.id), index: None}, no_replica: false })
        }

        let update_ops = self.services.updates_required(ServiceState::Located, interval, force);
        for _o in &update_ops {
            //self.locate(rpc::LocateOptions{id: inst.id}).await;
        }

        let subscribe_ops = self.services.updates_required(ServiceState::Subscribed, interval, force);
        for _o in &subscribe_ops {
            //self.locate(rpc::LocateOptions{id: inst.id}).await;
            //self.subscribe(rpc::SubscribeOptions{service: rpc::ServiceIdentifier{id: Some(inst.id), index: None}}).await;
        }

        Ok(())
    }
}



