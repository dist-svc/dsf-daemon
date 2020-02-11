
use std::time::SystemTime;

use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;
use diesel::dsl::sql_query;


use chrono::{DateTime, TimeZone, Local, NaiveDateTime};

pub mod error;
pub mod schema;
pub mod peers;
pub mod services;
pub mod data;

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
    pub fn new(path: &str) -> Result<Self, StoreError> {
        let conn = SqliteConnection::establish(path)?;

        Ok(Self{conn})
    }

    pub fn create(&self) -> Result<(), StoreError> {
        sql_query("CREATE TABLE services (
            service_id TEXT NOT NULL UNIQUE PRIMARY KEY, 
            service_index TEGER NOT NULL, 
            state TEXT NOT NULL, 

            public_key TEXT NOT NULL, 
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

        Ok(())
    }

    pub fn delete(&self) -> Result<(), StoreError> {
        sql_query("DROP TABLE IF EXISTS services;").execute(&self.conn)?;

        sql_query("DROP TABLE IF EXISTS peers;").execute(&self.conn)?;

        sql_query("DROP TABLE IF EXISTS data;").execute(&self.conn)?;

        Ok(())
    }
}

