
use structopt::StructOpt;

use dsf_core::prelude::*;

use kad::prelude::*;

use crate::core::peers::{Peer, PeerManager};
use crate::core::services::{ServiceManager};

use crate::io::Connector;
use crate::error::Error;

pub mod dht;
use dht::{DhtAdaptor, dht_reducer};

#[derive(Clone, Debug, PartialEq, StructOpt)]
pub struct Options {
    #[structopt(long = "database-dir", default_value = "/var/dsf/", env="DSF_DB_DIR")]
    /// [Legacy] database directory for storage by the daemon
    pub database_dir: String,

    #[structopt(flatten)]
    pub dht: DhtConfig,
}

/// Re-export of Dht type used for DSF
pub type Dht<C> = StandardDht<Id, Peer, Data, RequestId, DhtAdaptor<C>, ()>;

pub struct Dsf<C> {
    /// Inernal storage for daemon service 
    service: Service,

    /// Peer manager
    peers: PeerManager,

    /// Service manager
    services: ServiceManager,

    /// Distributed Database
    dht: StandardDht<Id, Peer, Data, RequestId, DhtAdaptor<C>, ()>,

    /// Backing store for DHT
    store: HashMapStore<Id, Data>,

    /// Connector for external communication
    connector: C,
}


impl <C> Dsf <C> where C: Connector + Clone + Sync + Send + 'static
{
    /// Create a new daemon
    pub fn new(config: Options, service: Service, connector: C) -> Result<Self, ()> {

        debug!("Creating new DSF instance");

        // Create managers
        let peers = PeerManager::new(&format!("{}/peers", config.database_dir));
        let services = ServiceManager::new(&format!("{}/services", config.database_dir));

        // Create DHT components
        let dht_conn = DhtAdaptor::new(service.id(), peers.clone(), connector.clone());
        let table = KNodeTable::new(service.id(), config.dht.k, config.dht.hash_size);
        let store = HashMapStore::new_with_reducer(Box::new(dht_reducer));

        // Instantiate DHT
        let dht = StandardDht::<Id, Peer, Data, RequestId, _, ()>::new(service.id(), config.dht, table, dht_conn, store.clone());

        // Create DSF object
        let s = Self {
            service,
            peers,
            services,
            dht,
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
    pub fn service(&self) -> &Service {
        &self.service
    }

    pub(crate) fn peers(&self) -> PeerManager {
        self.peers.clone()
    }

    pub(crate) fn dht(&mut self) -> &mut Dht<C> {
        &mut self.dht
    }


    /// Initialise a DSF instance
    pub async fn bootstrap(&mut self) -> Result<(), Error> {
        
        unimplemented!();
    }

    /// Store pages in the database at the provided ID
    pub async fn store(&mut self, id: &Id, pages: Vec<Page>) -> Result<(), Error> {
        unimplemented!();
    }

    /// Find pages in the database at the provided ID
    pub async fn find(&mut self, id: &Id) -> Result<Vec<Page>, Error> {
        unimplemented!();
    }

    /// Look up a peer in the database
    pub async fn lookup(&mut self, id: &Id) -> Result<Peer, Error> {
        unimplemented!();
    }

    /// Run an update of the daemom and all managed services
    pub async fn update(&mut self, force: bool) -> Result<(), Error> {
        unimplemented!();
    }
}



