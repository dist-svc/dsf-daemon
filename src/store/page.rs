
use diesel::prelude::*;

use dsf_core::prelude::*;

use super::{Store, StoreError};

use crate::store::schema::object::dsl::*;

pub type PageFields = (String, Vec<u8>, Option<String>, String);

impl Store {
    pub fn save_page(&self, page: &Page) -> Result<(), StoreError> {

        let sig = match page.signature {
            Some(v) => signature.eq(v.to_string()),
            None => return Err(StoreError::MissingSignature),
        };

        let prev = page.previous_sig.map(|v| previous.eq(v.to_string()) );
        let raw = page.raw.as_ref().map(|v| raw_data.eq(v.clone()) );

        let values = (
            service_id.eq(page.id.to_string()),
            raw,
            prev,
            sig.clone(),
        );

        let r = object.filter(sig.clone())
            .select(service_id).load::<String>(&self.conn)?;

        if r.len() != 0 {
            diesel::update(object)
                .filter(sig)
                .set(values)
                .execute(&self.conn)?;
        } else {
            diesel::insert_into(object)
                .values(values)
                .execute(&self.conn)?;
        }

        Ok(())
    }

    pub fn load_page(&self, sig: &Signature) -> Result<Option<Page>, StoreError>  {
        let results = object
            .filter(signature.eq(sig.to_string()))
            .select((service_id, raw_data, previous, signature))
            .load::<PageFields>(&self.conn)?;

        if results.len() == 0 {
            return Ok(None)
        }

        let (_r_id, r_raw, _r_previous, _r_signature) = &results[0];

        let mut v = Page::decode_pages(&r_raw, |_id| None ).unwrap();

        Ok(Some(v.remove(0)))
    }

    pub fn load_service_pages(&self, svc_id: &Id) -> Result<Vec<Page>, StoreError>  {
        let results = object
            .filter(service_id.eq(svc_id.to_string()))
            .select((service_id, raw_data, previous, signature))
            .load::<PageFields>(&self.conn)?;

        let (_r_id, r_raw, _r_previous, _r_signature) = &results[0];

        let v = Page::decode_pages(&r_raw, |_id| None ).unwrap();

        Ok(v)
    }

    pub fn delete_page(&self, sig: &Signature) -> Result<(), StoreError> {

        diesel::delete(object).filter(signature.eq(sig.to_string()))
            .execute(&self.conn)?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    extern crate tracing_subscriber;
    use tracing_subscriber::{FmtSubscriber, filter::LevelFilter};

    use super::Store;

    use dsf_rpc::{DataInfo};
    use dsf_core::base::Body;
    use dsf_core::crypto::{new_pk, new_sk, hash, pk_sign};

    #[test]
    fn store_page_inst() {
        let _ = FmtSubscriber::builder().with_max_level(LevelFilter::DEBUG).try_init();

        let store = Store::new("/tmp/dsf-test-3.db")
            .expect("Error opening store");

        store.delete().unwrap();

        store.create().unwrap();

        let (public_key, private_key) = new_pk().unwrap();
        let _secret_key = new_sk().unwrap();
        let id = hash(&public_key).unwrap();
        let sig = pk_sign(&private_key, &[0xaa, 0xaa, 0xaa, 0xaa]).unwrap();

        let d = DataInfo {
            service: id,
            index: 10,
            body: Body::Cleartext(vec![0xaa, 0xbb, 0xcc, 0xdd]),

            previous: Some(sig.clone()),
            signature: sig,
        };

        // Check no matching service exists
        assert_eq!(0, store.load_service_page(&id).unwrap().len());

        // Store data
        store.save_page(&d).unwrap();
        assert_eq!(Some(&d), store.load_page(&d.signature).unwrap().as_ref());
        assert_eq!(vec![d.clone()], store.load_service_page(&id).unwrap());

        // Delete data
        store.delete_page(&d.signature).unwrap();
        assert_eq!(0, store.load_service_page(&id).unwrap().len());
    }

}
