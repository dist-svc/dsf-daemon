
use std::str::FromStr;

use diesel::prelude::*;
use chrono::NaiveDateTime;

use dsf_core::prelude::*;
use dsf_rpc::{PeerInfo, PeerState, PeerAddress};

use super::{Store, StoreError, from_dt, to_dt};

const KNOWN:    &str = "known";
const UNKNOWN:  &str = "unknown";

const IMPLICIT: &str = "implicit";
const EXPLICIT: &str = "explicit";

type PeerFields = (String, String, Option<String>, String, String, Option<NaiveDateTime>, i32, i32, bool);

impl Store {

    pub fn save_peer(&mut self, info: &PeerInfo) -> Result<(), StoreError> {
        use crate::store::schema::peers::dsl::*;

        let (s, k) = match info.state {
            PeerState::Known(k) => {
                (state.eq(KNOWN.to_string()), Some(public_key.eq(k.to_string())))
            },
            PeerState::Unknown => {
                (state.eq(UNKNOWN.to_string()), None)
            }
        };

        let am = match info.address {
            PeerAddress::Implicit(_) => IMPLICIT.to_string(),
            PeerAddress::Explicit(_) => EXPLICIT.to_string(),
        };
        

        let seen = info.seen.map(|v| last_seen.eq(to_dt(v)) );

        let values = (
            peer_id.eq(info.id.to_string()),
            s,
            k,
            address.eq(info.address().to_string()),
            address_mode.eq(am),
            seen,
            sent.eq(info.sent as i32),
            received.eq(info.received as i32),
            blocked.eq(info.blocked),
        );

        let r = peers.filter(peer_id.eq(info.id.to_string()))
            .select(peer_id).load::<String>(&self.conn)?;

        if r.len() != 0 {
            diesel::update(peers)
                .filter(peer_id.eq(info.id.to_string()))
                .set(values)
                .execute(&self.conn)?;
        } else {
            diesel::insert_into(peers)
                .values(values)
                .execute(&self.conn)?;
        }

        Ok(())
    }

    fn parse_peer(v: &PeerFields) -> Result<PeerInfo, StoreError> {

        let (r_id, r_state, r_pk, r_address, r_address_mode, r_seen, r_sent, r_recv, r_blocked) = v;

        let s_state = match (r_state.as_ref(), &r_pk) {
            (KNOWN, Some(k)) => PeerState::Known(PublicKey::from_str(k)?),
            (UNKNOWN, _) => PeerState::Unknown,
            _ => unreachable!(),
        };

        let s_addr = match r_address_mode.as_ref() {
            IMPLICIT => PeerAddress::Implicit(r_address.parse()?),
            EXPLICIT => PeerAddress::Explicit(r_address.parse()?),
            _ => unreachable!(),
        };

        let p = PeerInfo {
            id: Id::from_str(r_id)?,
            state: s_state,
            address: s_addr,

            seen: r_seen.as_ref().map(|v| from_dt(v) ),

            sent: *r_sent as u64,
            received: *r_recv as u64,
            blocked: *r_blocked,
        };

        Ok(p)
    }

    pub fn load_peer(&mut self, id: &Id) -> Result<Option<PeerInfo>, StoreError> {
        use crate::store::schema::peers::dsl::*;

        let results = peers
            .filter(peer_id.eq(id.to_string()))
            .select((peer_id, state, public_key, address, address_mode, last_seen, sent, received, blocked))
            .load::<(String, String, Option<String>, String, String, Option<NaiveDateTime>, i32, i32, bool)>(&self.conn)?;

        if results.len() == 0 {
            return Ok(None)
        }

        let p = Self::parse_peer(&results[0])?;

        Ok(Some(p))
    }

    pub fn load_peers(&mut self) -> Result<Vec<PeerInfo>, StoreError> {
        use crate::store::schema::peers::dsl::*;

        let results = peers
            .select((peer_id, state, public_key, address, address_mode, last_seen, sent, received, blocked))
            .load::<(String, String, Option<String>, String, String, Option<NaiveDateTime>, i32, i32, bool)>(&self.conn)?;

        let mut v = vec![];

        for r in &results {
            v.push(Self::parse_peer(r)?);
        }

        Ok(v)
    }

    pub fn delete_peer(&mut self, id: &Id) -> Result<(), StoreError> {
        use crate::store::schema::peers::dsl::*;

        diesel::delete(peers).filter(peer_id.eq(id.to_string()))
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

    use dsf_rpc::{PeerInfo, PeerState, PeerAddress};
    use dsf_core::crypto::{new_pk, new_sk, hash};

    #[test]
    fn store_peer_inst() {
        let _ = FmtSubscriber::builder().with_max_level(LevelFilter::DEBUG).try_init();

        let mut store = Store::new("/tmp/dsf-test-2.db")
            .expect("Error opening store");

        store.delete().unwrap();

        store.create().unwrap();

        let (public_key, _private_key) = new_pk().unwrap();
        let _secret_key = new_sk().unwrap();
        let id = hash(&public_key).unwrap();

        let mut p = PeerInfo {
            id,
            address: PeerAddress::Explicit("127.0.0.1:8080".parse().unwrap()),
            state: PeerState::Known(public_key),
            seen: Some(SystemTime::now()),
            sent: 14,
            received: 12,
            blocked: false,
        };

        // Check no matching service exists
        assert_eq!(None, store.load_peer(&p.id).unwrap());

        // Store service
        store.save_peer(&p).unwrap();
        assert_eq!(Some(&p), store.load_peer(&p.id).unwrap().as_ref());

        // update service
        p.sent = 16;
        store.save_peer(&p).unwrap();
        assert_eq!(Some(&p), store.load_peer(&p.id).unwrap().as_ref());

        // Delete service
        store.delete_peer(&p.id).unwrap();
        assert_eq!(None, store.load_peer(&p.id).unwrap());
    }

}
