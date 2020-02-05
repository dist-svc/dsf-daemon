
use std::str::FromStr;

use diesel::prelude::*;

use chrono::NaiveDateTime;

use dsf_core::prelude::*;
use dsf_rpc::{ServiceInfo, ServiceState};

use super::{Store, StoreError, to_dt, from_dt};

type ServiceFields = (String, i32, String, String, Option<String>, Option<String>, Option<String>, Option<NaiveDateTime>, i32, i32, bool);

impl Store {

    pub fn save_service(&mut self, info: &ServiceInfo) -> Result<(), StoreError> {
        use crate::store::schema::services::dsl::*;

        let sk = info.secret_key.map(|v| secret_key.eq(v.to_string()) );
        let up = info.last_updated.map(|v| last_updated.eq(to_dt(v)) );

        let values = (
            service_id.eq(info.id.to_string()),
            service_index.eq(info.index as i32),
            state.eq(info.state.to_string()),
            public_key.eq(info.public_key.to_string()),
            sk,
            up,
            subscribers.eq(info.subscribers as i32),
            replicas.eq(info.replicas as i32),
            original.eq(info.origin),
        );

        let r = services.filter(service_id.eq(info.id.to_string()))
            .select(service_index).load::<i32>(&self.conn)?;
        
        if r.len() != 0 {
            diesel::update(services)
                .filter(service_id.eq(info.id.to_string()))
                .set(values)
                .execute(&self.conn)?;
        } else {
            diesel::insert_into(services)
                .values(values)
                .execute(&self.conn)?;
        }

        Ok(())
    }

    fn parse_service(v: &ServiceFields) -> Result<ServiceInfo, StoreError> {
        let (r_id, r_index, r_state, r_pk, r_sk, r_pp, r_rp, r_upd, r_subs, r_reps, r_original) = v;

        let s = ServiceInfo {
            id: Id::from_str(r_id)?,
            index: *r_index as usize,
            state: ServiceState::from_str(r_state)?,

            primary_page: r_pp.as_ref().map(|v| Signature::from_str(&v).unwrap() ),
            replica_page: r_rp.as_ref().map(|v| Signature::from_str(&v).unwrap() ),

            public_key: PublicKey::from_str(r_pk)?,
            secret_key: r_sk.as_ref().map(|v| SecretKey::from_str(&v).unwrap() ),
            
            last_updated: r_upd.as_ref().map(|v| from_dt(v) ),

            subscribers: *r_subs as usize,
            replicas: *r_reps as usize,
            origin: *r_original,
        };

        Ok(s)
    }

    pub fn load_service(&mut self, id: &Id) -> Result<Option<ServiceInfo>, StoreError> {
        use crate::store::schema::services::dsl::*;

        let results = services
            .filter(service_id.eq(id.to_string()))
            .select((service_id, service_index, state, public_key, secret_key, primary_page, replica_page, last_updated, subscribers, replicas, original))
            .load::<ServiceFields>(&self.conn)?;

        if results.len() == 0 {
            return Ok(None)
        }

        let s = Self::parse_service(&results[0])?;

        Ok(Some(s))
    }

    pub fn load_services(&mut self) -> Result<Vec<ServiceInfo>, StoreError> {
        use crate::store::schema::services::dsl::*;

        let results = services
            .select((service_id, service_index, state, public_key, secret_key, primary_page, replica_page, last_updated, subscribers, replicas, original))
            .load::<ServiceFields>(&self.conn)?;

        let mut v = vec![];

        for r in &results {
            v.push(Self::parse_service(r)?);
        }

        Ok(v)
    }

    pub fn delete_service(&mut self, id: &Id) -> Result<(), StoreError> {
        use crate::store::schema::services::dsl::*;

        diesel::delete(services).filter(service_id.eq(id.to_string()))
            .execute(&self.conn)?;

        Ok(())
    }
}


#[cfg(test)]
mod test {
    use std::time::SystemTime;

    extern crate tracing_subscriber;
    use tracing_subscriber::{FmtSubscriber, filter::LevelFilter};

    use super::Store;

    use dsf_rpc::{ServiceInfo, ServiceState};
    use dsf_core::crypto::{new_pk, new_sk, hash};

    #[test]
    fn store_service_inst() {
        let _ = FmtSubscriber::builder().with_max_level(LevelFilter::DEBUG).try_init();

        let mut store = Store::new("/tmp/dsf-test-1.db")
            .expect("Error opening store");

        store.delete().unwrap();

        store.create().unwrap();

        let (public_key, _private_key) = new_pk().unwrap();
        let secret_key = new_sk().unwrap();
        let id = hash(&public_key).unwrap();

        let mut s = ServiceInfo {
            id,
            index: 10,
            state: ServiceState::Registered,
            public_key,
            secret_key: Some(secret_key),
            primary_page: None,
            replica_page: None,
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