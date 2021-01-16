use std::str::FromStr;
use std::time::SystemTime;

use log::{debug, error, warn};

use sqlx::{Connection, Any, Pool, Sqlite};
use chrono::{DateTime, Local, NaiveDateTime, TimeZone};

use dsf_core::prelude::*;
use dsf_core::service::Subscriber;

use dsf_rpc::{ServiceInfo, ServiceState, PeerInfo, PeerState, DataInfo};


pub struct Store {
    pool: Pool<Sqlite>,
}

fn to_dt(s: SystemTime) -> NaiveDateTime {
    DateTime::<Local>::from(s).naive_utc()
}

fn from_dt(n: &NaiveDateTime) -> SystemTime {
    let l = chrono::offset::Local;
    let dt = l.from_utc_datetime(n);
    dt.into()
}

impl Store {
    /// Create or connect to a store with the provided database name
    pub async fn new(db: &str) -> Result<Self, anyhow::Error> {
        let pool = Pool::connect(db).await?;

        Ok(Self{ pool })
    }

    pub fn load_peer_service(&self) -> Result<Option<Service>, anyhow::Error> {
        unimplemented!()
    }

    pub fn set_peer_service(&self, service: &Service, page: &Page) -> Result<(), anyhow::Error> {
        unimplemented!()
    }

    pub fn save_page(&self, page: &Page) -> Result<(), anyhow::Error> {
        unimplemented!()

    }

    pub fn find_pages(&self, id: &Id) -> Result<Vec<Page>, anyhow::Error> {
        unimplemented!()

    }

    pub fn delete_page(&self, sig: &Signature) -> Result<(), anyhow::Error> {
        unimplemented!()

    }

    pub fn load_page(
        &self,
        sig: &Signature,
        public_key: Option<PublicKey>,
    ) -> Result<Option<Page>, anyhow::Error> {
        unimplemented!()

    }


    pub fn save_service(&self, info: &ServiceInfo) -> Result<(), anyhow::Error> {
        unimplemented!()

    }
    // Find an item or items
    pub async fn find_service2(&self, id: &Id) -> Result<Option<ServiceInfo>, anyhow::Error> {
        let id_str = id.to_string();

        let row = sqlx::query!("SELECT * FROM services WHERE id = ?", id_str)
            .fetch_optional(&self.pool).await?;

        let row = match row {
            Some(v) => v,
            None => return Ok(None)
        };

        Ok(Some(ServiceInfo {
            id: Id::from_str(&row.id)?,
            index: row.index as usize,
            state: ServiceState::from_str(&row.state)?,

            primary_page: row.primary_page.as_ref().map(|v| Signature::from_str(&v).unwrap()),
            replica_page: row.replica_page.as_ref().map(|v| Signature::from_str(&v).unwrap()),

            body: Body::None,

            public_key: PublicKey::from_str(&row.public_key.unwrap())?,
            private_key: row.private_key
                .as_ref()
                .map(|v| PrivateKey::from_str(&v).unwrap()),
            secret_key: row.secret_key.as_ref().map(|v| SecretKey::from_str(&v).unwrap()),

            last_updated: row.last_updated.as_ref().map(|v| from_dt(v)),

            subscribers: row.subscribers.unwrap_or(0) as usize,
            replicas: row.replicas.unwrap_or(0) as usize,

            origin: row.origin.unwrap_or(false),
            subscribed: row.subscribed.unwrap_or(false),
        }))
    }

    pub fn find_service(&self, id: &Id) -> Result<Option<ServiceInfo>, anyhow::Error> {
        unimplemented!()
    }

    // Load all items
    pub fn load_services(&self) -> Result<Vec<ServiceInfo>, anyhow::Error> {
        unimplemented!()

    }

    pub fn delete_service(&self, info: &ServiceInfo) -> Result<(), anyhow::Error> {
        unimplemented!()

    }

    pub fn save_peer(&self, info: &PeerInfo) -> Result<(), anyhow::Error> {
        unimplemented!()

    }

    pub fn find_peer(&self, id: &Id) -> Result<Option<PeerInfo>, anyhow::Error> {
        unimplemented!()

    }

    pub fn load_peers(&self) -> Result<Vec<PeerInfo>, anyhow::Error> {
        unimplemented!()

    }

    pub fn delete_peer(&self, peer: &PeerInfo) -> Result<(), anyhow::Error>{
        unimplemented!()

    }

    pub fn save_data(&self, info: &DataInfo) -> Result<(), anyhow::Error> {
        unimplemented!()
    }

    pub fn find_data(&self, id: &Id) -> Result<Vec<DataInfo>, anyhow::Error> {
        unimplemented!()
    }

    pub fn delete_data(&self, sig: &Signature) -> Result<(), anyhow::Error> {
        unimplemented!()
    }

    pub fn load_data(&self, sig: &Signature) -> Result<Option<DataInfo>, anyhow::Error> {
        unimplemented!()
    }


}
