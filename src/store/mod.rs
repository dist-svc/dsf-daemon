
use std::time::SystemTime;
use std::str::FromStr;

use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;
use diesel::result::{Error as DieselError};
use diesel::dsl::sql_query;

use strum::{ParseError as StrumError};

use chrono::{DateTime, TimeZone, Local, NaiveDateTime};

use dsf_core::prelude::*;
use dsf_rpc::{PeerInfo, ServiceInfo, ServiceState};

use base64::{DecodeError as B64Error};

pub mod schema;
use schema::services::dsl::*;

fn to_dt(s: SystemTime) -> NaiveDateTime {
    DateTime::<Local>::from(s).naive_utc()
}

fn from_dt(n: &NaiveDateTime) -> SystemTime {
    let l = chrono::offset::Local;
    let dt = l.from_utc_datetime(n);
    dt.into()
}

pub struct Store {
    conn: SqliteConnection,
}

#[derive(Debug, PartialEq)]
pub enum StoreError {
    Connection(ConnectionError),
    Diesel(DieselError),
    Strum(StrumError),
    B64(B64Error),
}


impl From<ConnectionError> for StoreError {
    fn from(e: ConnectionError) -> Self {
        Self::Connection(e)
    }
}

impl From<DieselError> for StoreError {
    fn from(e: DieselError) -> Self {
        Self::Diesel(e)
    }
}

impl From<StrumError> for StoreError {
    fn from(e: StrumError) -> Self {
        Self::Strum(e)
    }
}

impl From<B64Error> for StoreError {
    fn from(e: B64Error) -> Self {
        Self::B64(e)
    }
}


impl Store {
    pub fn new(path: &str) -> Result<Self, StoreError> {
        let conn = SqliteConnection::establish(path)?;

        Ok(Self{conn})
    }

    fn save_service(&mut self, info: &ServiceInfo) -> Result<(), StoreError> {

        let sk = info.secret_key.map(|v| secret_key.eq(v.to_string()) );
        let up = info.last_updated.map(|v| last_updated.eq(to_dt(v)) );

        let values = (
            id.eq(info.id.to_string()),
            index.eq(info.index as i32),
            state.eq(info.state.to_string()),
            public_key.eq(info.public_key.to_string()),
            sk,
            up,
            subscribers.eq(info.subscribers as i32),
            replicas.eq(info.replicas as i32),
            original.eq(info.origin),
        );

        let r = services.filter(id.eq(info.id.to_string()))
            .select(index).load::<i32>(&self.conn)?;
        
        if r.len() != 0 {
            diesel::update(services)
                .filter(id.eq(info.id.to_string()))
                .set(values)
                .execute(&self.conn)?;
        } else {
            diesel::insert_into(services)
                .values(values)
                .execute(&self.conn)?;
        }

        Ok(())
    }

    fn load_service(&mut self, service_id: &Id) -> Result<Option<ServiceInfo>, StoreError> {
        let result = services
            .filter(id.eq(service_id.to_string()))
            .select((id, index, state, public_key, secret_key, last_updated, subscribers, replicas, original))
            .load::<(String, i32, String, String, Option<String>, Option<NaiveDateTime>, i32, i32, bool)>(&self.conn)?;

        if result.len() == 0 {
            return Ok(None)
        }

        let (_r_id, r_index, r_state, r_pk, r_sk, r_upd, r_subs, r_reps, r_original) = &result[0];

        let s = ServiceInfo {
            id: service_id.clone(),
            index: *r_index as usize,
            state: ServiceState::from_str(r_state)?,
            public_key: PublicKey::from_str(r_pk)?,
            secret_key: r_sk.as_ref().map(|v| SecretKey::from_str(&v).unwrap() ),
            
            last_updated: r_upd.as_ref().map(|v| from_dt(v) ),

            subscribers: *r_subs as usize,
            replicas: *r_reps as usize,
            origin: *r_original,
        };

        Ok(Some(s))
    }

    fn delete_service(&mut self, service_id: &Id) -> Result<(), StoreError> {
        diesel::delete(services).filter(id.eq(service_id.to_string()))
            .execute(&self.conn)?;

        Ok(())
    }

    fn save_peer(&mut self, _id: &Id, _info: PeerInfo) -> Result<(), StoreError> {
        unimplemented!()
    }

    fn load_peer(&mut self, _id: &Id) -> Result<Option<PeerInfo>, StoreError> {
        unimplemented!()
    }

    fn create(&mut self) -> Result<(), StoreError> {
        sql_query("CREATE TABLE services (id TEXT NOT NULL UNIQUE PRIMARY KEY, 'index' INTEGER NOT NULL, state TEXT NOT NULL, public_key TEXT NOT NULL, secret_key TEXT, last_updated TEXT, subscribers INTEGER NOT NULL, replicas INTEGER NOT NULL, original BOOLEAN NOT NULL);").execute(&self.conn)?;

        Ok(())
    }

    fn delete(&mut self) -> Result<(), StoreError> {
        sql_query("DELETE * FROM services;").execute(&self.conn)?;
        sql_query("DROP TABLE IF EXISTS services;").execute(&self.conn)?;

        Ok(())
    }
}


#[cfg(test)]
mod test {
    use std::time::SystemTime;

    use super::Store;

    use dsf_rpc::{ServiceInfo, ServiceState};
    use dsf_core::crypto::{new_pk, new_sk, hash};

    #[test]
    fn store_service_inst() {
        let mut store = Store::new("/tmp/dsf-test-1.db")
            .expect("Error opening store");

        let _ = store.delete();

        let _ = store.create();

        let (public_key, _private_key) = new_pk().unwrap();
        let secret_key = new_sk().unwrap();
        let id = hash(&public_key).unwrap();

        let mut s = ServiceInfo {
            id,
            index: 10,
            state: ServiceState::Registered,
            public_key,
            secret_key: Some(secret_key),
            last_updated: Some(SystemTime::now()),
            subscribers: 14,
            replicas: 12,
            origin: true,
        };

        // Check no matching service exists
        assert_eq!(None, store.load_service(&s.id).unwrap());

        // Store service
        store.save_service(&s).unwrap();
        assert_eq!(Some(&s), store.load_service(&s.id).unwrap().as_ref());

        // update service
        s.subscribers = 0;
        store.save_service(&s).unwrap();
        assert_eq!(Some(&s), store.load_service(&s.id).unwrap().as_ref());

        // Delete service
        store.delete_service(&s.id).unwrap();
        assert_eq!(None, store.load_service(&s.id).unwrap());

    }

}