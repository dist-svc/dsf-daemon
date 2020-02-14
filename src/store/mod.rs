
use std::time::SystemTime;
use std::str::FromStr;

use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;
use diesel::dsl::sql_query;


use chrono::{DateTime, TimeZone, Local, NaiveDateTime};

use dsf_core::prelude::*;
use crate::dsf_core::service::Subscriber;

pub mod error;
pub mod schema;
pub mod peers;
pub mod services;
pub mod data;

pub mod page;

pub use error::StoreError;

pub struct Store {
    conn: SqliteConnection,
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
    /// Create or connect to a store with the provided filename
    pub fn new(path: &str) -> Result<Self, StoreError> {
        debug!("Connecting to store: {}", path);

        let conn = SqliteConnection::establish(path)?;

        let s = Self{conn};

        let _ = s.create();

        Ok(s)
    }

    /// Initialise the store
    /// 
    /// This is called automatically in the `new` function
    pub fn create(&self) -> Result<(), StoreError> {
        sql_query("CREATE TABLE services (
            service_id TEXT NOT NULL UNIQUE PRIMARY KEY, 
            service_index TEGER NOT NULL, 
            state TEXT NOT NULL, 

            public_key TEXT NOT NULL, 
            private_key TEXT, 
            secret_key TEXT, 

            primary_page BLOB, 
            replica_page BLOB, 
            
            last_updated TEXT, 
            subscribers INTEGER NOT NULL, 
            replicas INTEGER NOT NULL,
            original BOOLEAN NOT NULL
        );").execute(&self.conn)?;

        sql_query("CREATE TABLE peers (
            peer_id TEXT NOT NULL UNIQUE PRIMARY KEY, 
            peer_index INTEGER, 
            state TEXT NOT NULL, 

            public_key TEXT, 
            address TEXT, 
            address_mode TEXT, 
            last_seen TEXT, 
            
            sent INTEGER NOT NULL, 
            received INTEGER NOT NULL, 
            blocked BOOLEAN NOT NULL
        );").execute(&self.conn)?;

        sql_query("CREATE TABLE data (
            signature TEXT NOT NULL UNIQUE PRIMARY KEY, 
            service_id TEXT NOT NULL,

            object_index INTEGER NOT NULL,
            body_kind TEXT NOT NULL,
            body_value BLOB,

            previous TEXT
        );").execute(&self.conn)?;

        sql_query("CREATE TABLE object (
            service_id TEXT NOT NULL,

            raw_data BLOB NOT NULL,

            previous TEXT,
            signature TEXT NOT NULL PRIMARY KEY
        );").execute(&self.conn)?;

        sql_query("CREATE TABLE identity (
            service_id TEXT NOT NULL PRIMARY KEY,
            private_key TEXT NOT NULL,
            secret_key TEXT,
            last_page TEXT NOT NULL
        );").execute(&self.conn)?;

        Ok(())
    }

    pub fn delete(&self) -> Result<(), StoreError> {
        sql_query("DROP TABLE IF EXISTS services;").execute(&self.conn)?;

        sql_query("DROP TABLE IF EXISTS peers;").execute(&self.conn)?;

        sql_query("DROP TABLE IF EXISTS data;").execute(&self.conn)?;

        sql_query("DROP TABLE IF EXISTS object;").execute(&self.conn)?;

        sql_query("DROP TABLE IF EXISTS identity;").execute(&self.conn)?;

        Ok(())
    }

    pub fn load_peer_service(&self) -> Result<Option<Service>, StoreError> {
        use crate::store::schema::identity::dsl::*;
        
        // Find service id and last page
        let results = identity
            .select((service_id, private_key, secret_key, last_page))
            .load::<(String, String, Option<String>, String)>(&self.conn)?;

        if results.len() != 1 {
            return Ok(None)
        }

        let (_s_id, s_pri_key, s_sec_key, page_sig) = &results[0];

        // Load page
        let page = match self.load_page(&Signature::from_str(&page_sig).unwrap())? {
            Some(v) => v,
            None => return Ok(None),
        };

        // Generate service
        let mut service = Service::load(&page).unwrap();

        service.set_private_key(Some(PrivateKey::from_str(s_pri_key).unwrap()));
        let sec_key = s_sec_key.as_ref().map(|v| SecretKey::from_str(&v).unwrap());
        service.set_secret_key(sec_key);

        Ok(Some(service))
    }

    pub fn set_peer_service(&self, service: &Service, page: &Page) -> Result<(), StoreError> {
        use crate::store::schema::identity::dsl::*;

        let pri_key = service.private_key().map(|v| private_key.eq(v.to_string()) );
        let sec_key = service.secret_key().map(|v| secret_key.eq(v.to_string()) );
        let sig = page.signature.map(|v| last_page.eq(v.to_string()) );

        // Ensure the page has been written
        if let None = self.load_page(&page.signature.unwrap())? {
            self.save_page(page)?;
        }

        // Setup identity values
        let values = (
            service_id.eq(service.id().to_string()),
            pri_key,
            sec_key,
            sig,
        );

        // Check if the identity already exists
        let results = identity.select(service_id).load::<String>(&self.conn)?;

        // Create or update
        if results.len() != 0 {
            diesel::update(identity)
                .set(values)
                .execute(&self.conn)?;
        } else {
            diesel::insert_into(identity)
                .values(values)
                .execute(&self.conn)?;
        }

        Ok(())
    }
}

