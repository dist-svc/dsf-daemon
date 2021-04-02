use diesel::prelude::*;

use dsf_core::{prelude::*, KeySource};

use super::{Store, StoreError};

use crate::store::schema::object::dsl::*;

pub type PageFields = (String, Vec<u8>, Option<String>, String);

impl Store {
    // Store an item
    pub fn save_page(&self, page: &Page) -> Result<(), StoreError> {
        let sig = match &page.signature {
            Some(v) => signature.eq(v.to_string()),
            None => return Err(StoreError::MissingSignature),
        };

        let raw = match &page.raw {
            Some(v) => raw_data.eq(v),
            None => return Err(StoreError::MissingRawData),
        };

        let prev = page
            .previous_sig
            .as_ref()
            .map(|v| previous.eq(v.to_string()));
        let values = (service_id.eq(page.id.to_string()), raw, prev, sig.clone());

        let r = object
            .filter(sig.clone())
            .select(service_id)
            .load::<String>(&self.pool.get().unwrap())?;

        if r.len() != 0 {
            diesel::update(object)
                .filter(sig)
                .set(values)
                .execute(&self.pool.get().unwrap())?;
        } else {
            diesel::insert_into(object)
                .values(values)
                .execute(&self.pool.get().unwrap())?;
        }

        Ok(())
    }

    // Find an item or items
    pub fn find_pages<K: KeySource>(
        &self,
        id: &Id,
        key_source: &K,
    ) -> Result<Vec<Page>, StoreError> {
        let results = object
            .filter(service_id.eq(id.to_string()))
            .select((service_id, raw_data, previous, signature))
            .load::<PageFields>(&self.pool.get().unwrap())?;

        let (_r_id, r_raw, _r_previous, _r_signature) = &results[0];

        let v = Page::decode_pages(&r_raw, key_source).unwrap();

        Ok(v)
    }

    // Delete an item
    pub fn delete_page(&self, sig: &Signature) -> Result<(), StoreError> {
        diesel::delete(object)
            .filter(signature.eq(sig.to_string()))
            .execute(&self.pool.get().unwrap())?;

        Ok(())
    }

    pub fn load_page<K: KeySource>(
        &self,
        sig: &Signature,
        key_source: &K,
    ) -> Result<Option<Page>, StoreError> {
        let results = object
            .filter(signature.eq(sig.to_string()))
            .select((service_id, raw_data, previous, signature))
            .load::<PageFields>(&self.pool.get().unwrap())?;

        if results.len() == 0 {
            return Ok(None);
        }

        let (_r_id, r_raw, _r_previous, _r_signature) = &results[0];

        let mut v = Page::decode_pages(&r_raw, key_source).unwrap();

        Ok(Some(v.remove(0)))
    }
}

#[cfg(test)]
mod test {
    extern crate tracing_subscriber;
    use tracing_subscriber::{filter::LevelFilter, FmtSubscriber};

    use super::Store;

    use dsf_core::service::{Publisher, Service};

    #[test]
    fn store_page_inst() {
        let _ = FmtSubscriber::builder()
            .with_max_level(LevelFilter::DEBUG)
            .try_init();

        let store = Store::new("/tmp/dsf-test-4.db").expect("Error opening store");

        store.drop_tables().unwrap();
        store.create_tables().unwrap();

        let mut s = Service::default();
        let keys = s.keys();

        let mut buff = vec![0u8; 1024];
        let (n, mut page) = s.publish_primary(&mut buff).expect("Error creating page");
        let sig = page.signature.clone().unwrap();
        page.raw = Some(buff[..n].to_vec());

        // Check no matching service exists
        assert_eq!(None, store.load_page(&sig, &keys).unwrap());

        // Store data
        store.save_page(&page).unwrap();
        assert_eq!(Some(&page), store.load_page(&sig, &keys).unwrap().as_ref());
        assert_eq!(
            vec![page.clone()],
            store.find_pages(&s.id(), &keys).unwrap()
        );

        // Delete data
        store.delete_page(&sig).unwrap();
        assert_eq!(None, store.load_page(&sig, &keys).unwrap());
    }
}
