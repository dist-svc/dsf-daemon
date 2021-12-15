//! Registry search implementation for querying from nameservers
//! 
//! 

use futures::{Future, future, FutureExt};
use log::{debug, error, warn};
use serde::{Serialize, Deserialize};

use dsf_core::prelude::{Page, Options, PageInfo};
use dsf_core::types::{Id, CryptoHash, Flags};
use dsf_core::error::{Error as CoreError};
use dsf_core::service::Registry;
use dsf_core::options::{self, Filters};

use dsf_rpc::{ServiceIdentifier, Response};

use crate::daemon::Dsf;
use crate::error::Error;
use crate::rpc::ops::Res;

use super::ops::{RpcKind, OpKind, Engine};


#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
pub enum SearchFilter {
    /// Name option for searching
    Name(String),
    /// Pre-computed hash
    Hash(CryptoHash),
}

#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
pub struct SearchOptions {
    /// NameServer filter / selection
    pub ns: Option<ServiceIdentifier>,

    /// Name to search for
    pub search: SearchFilter,
}

#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
pub struct RegisterOptions {
    /// NameServer filter / selection
    pub ns: Option<ServiceIdentifier>,

    /// Service for registration
    pub target: ServiceIdentifier,
}



#[async_trait::async_trait]
pub trait NameService {
    async fn search(&self, opts: SearchOptions) -> Result<Vec<Page>, Error>;

    async fn register(&self, opts: RegisterOptions) -> Result<(), Error>;
}

#[async_trait::async_trait]
impl <T: Engine> NameService for T {
    /// Search for a matching service using the provided (or relevant) nameserver
    async fn search(&self, opts: SearchOptions) -> Result<Vec<Page>, Error> {

        debug!("Locating nameservice for search: {:?}", opts);
        
        // Resolve nameserver using provided options
        let ns = if let Some(service) = opts.ns {
            // Lookup by ID or index
            self.service_resolve(service).await
        } else {
            // Lookup by service prefix
            todo!()
        };

        // Check we found a viable name service
        let ns = match ns {
            Ok(ns) => ns,
            Err(_e) => {
                error!("No matching nameservice found");
                return Err(Error::NotFound);
            }
        };

        // Generate search query
        let lookup = match opts.search {
            SearchFilter::Name(n) => {
                ns.resolve(&options::Name::new(&n))?
            },
            SearchFilter::Hash(_h) => {
                todo!()
            }
        };

        debug!("Using ns: {} query hash: {}", ns.id(), lookup);


        // Issue query
        let pages = match self.dht_get(lookup).await {
            Ok(p) => p,
            Err(e) => {
                error!("DHT lookup failed: {:?}", e);
                return Err(e.into())
            }
        };

        debug!("Located pages: {:?}", pages);

        // Collapse resolved pages
        let matches = pages.iter().filter_map(|p| {
            // Check page is of tertiary kind
            let i = match p.info() {
                PageInfo::Tertiary(t) => t,
                _ => return None,
            };
            
            // Check page matches nameserver
            let peer_id = p.public_options().iter().peer_id();
            match peer_id {
                Some(peer_id) if peer_id == ns.id() => (),
                _ => return None,
            };

            // TODO: check response matches query

            Some(i)
        });

        // TODO: this should be an operation shared between components


        let mut matched_services = vec![];

        // Perform service lookup(s)
        for m in matches {
            // Lookup service pages
            let pages = match self.dht_get(m.target_id.clone()).await {
                Ok(p) => p,
                Err(e) => {
                    error!("Failed to fetch pages for id {}: {:?}", m.target_id, e);
                    continue;
                }
            };

            // Fetch primary page
            let primary_page = match pages.iter().find(|p| {
                let h = p.header();
                h.kind().is_page() && !h.flags().contains(Flags::SECONDARY) && p.id() == &m.target_id
            }) {
                Some(p) => p.clone(),
                None => {
                    error!("No primary page found for service: {}", m.target_id);
                    continue;
                },
            };

            matched_services.push(primary_page)
        }
        

        Ok(matched_services)
    }

    async fn register(&self, opts: RegisterOptions) -> Result<(), Error> {

        debug!("Locating nameserver for register: {:?}", opts);

        // Resolve nameserver using provided options
        let ns = if let Some(service) = &opts.ns {
            // Lookup by ID or index
            self.service_resolve(service.clone()).await
        } else {
            // Lookup by name prefix if provided?
            todo!()
        };

        // Check we found a viable name service
        // TODO: ensure this _is_ a name service
        let ns = match ns {
            Ok(ns) if ns.is_origin() => ns,
            Ok(_ns) => {
                error!("Cannot (yet) publish to remote name services");
                return Err(Error::Unimplemented);
            }
            Err(_e) => {
                error!("No matching name service found");
                return Err(Error::NotFound);
            }
        };

        debug!("Locating target for register: {:?}", opts);

        // Lookup service for registering
        let t = match self.service_resolve(opts.target).await {
            Ok(s) => s,
            Err(e) => {
                error!("No matching target service found: {:?}", e);
                return Err(Error::NotFound);
            },
        };


        // Check we're able to publish using the nameservice
        // TODO: if _we're_ the origin of the nameserver, just execute?


        debug!("Using ns: {} for target: {}", ns.id(), t.id());

        // Generate pages for registration
        let pages = self.service_update(ns.id(), Box::new(move |s| {
            let mut pages = vec![];

            for o in t.public_options().iter() {
                // Match queryable options and generate pages
                // TODO: how to make this more generic..?
                let p = match o {
                    Options::Name(n) => s.publish_tertiary::<256, _>(t.id(), Default::default(), n).ok(),
                    _ => None,
                };

                // Push created pages to vector
                if let Some(p) = p {
                    pages.push(p);
                }
            }

            Ok(Res::Pages(pages))
        })).await?;

        let pages = match pages {
            Res::Pages(p) => p,
            _ => unreachable!(),
        };

        // Publish pages to database
        for p in pages {
            if let Err(e) = self.dht_put(p.id.clone(), vec![p]).await {
                error!("Failed to publish pages to DHT: {:?}", e);
                return Err(e.into());
            }
        }
        
        // TODO: return result

        Ok(())
    }
}



#[cfg(test)]
mod test {
    use std::collections::hash_map::Entry;
    use std::convert::{TryInto, TryFrom};
    use std::sync::{Arc, Mutex};
    use std::collections::{HashMap, VecDeque};

    use dsf_core::options::{PeerId, Filters};
    use dsf_core::page::Tertiary;
    use futures::future;

    use dsf_core::prelude::*;
    use super::*;

    struct MockEngine {
        inner: Arc<Mutex<Inner>>,
    }

    struct Inner {
        pub ns: Service, 
        pub target: Service, 
        pub expect: VecDeque<Expect>,
    }

    type Expect = Box<dyn Fn(OpKind, &mut Service, &mut Service)->Result<Res, CoreError> + Send + 'static>;

    impl MockEngine {
        pub fn setup() -> (Self, Id, Id) {
            let _ = simplelog::SimpleLogger::init(simplelog::LevelFilter::Debug, simplelog::Config::default());

            let ns = ServiceBuilder::ns("test.com").build().unwrap();
            let target = ServiceBuilder::default()
                .public_options(vec![Options::name("something")]).build().unwrap();

            let inner = Inner{
                ns: ns.clone(),
                target: target.clone(),
                expect: VecDeque::new(),
            };

            let e = MockEngine{
                inner: Arc::new(Mutex::new(inner)),
            };

            (e, ns.id(), target.id())
        }

        pub fn expect(&self, ops: Vec<Expect>) {
            let mut e = self.inner.lock().unwrap();
            e.expect = ops.into();
        }

        pub fn with<R, F: Fn(&mut Service, &mut Service) -> R>(&self, f: F) -> R {
            let mut i = self.inner.lock().unwrap();
            let Inner{ref mut ns, ref mut target, ..} = *i;

            f(ns, target)
        }
    }

    #[async_trait::async_trait]
    impl Engine for MockEngine {
        async fn exec(&self, op: OpKind) -> Result<Res, CoreError> {
            let mut i = self.inner.lock().unwrap();

            let Inner{ref mut ns, ref mut target, ref mut expect} = *i;

            debug!("Exec op: {:?}", op);

            match expect.pop_front() {
                Some(f) => f(op, ns, target),
                None => panic!("No remaining expectations"),
            }
        }
    }

    #[async_std::test]
    async fn test_register() {
        let (e, ns_id, target_id) = MockEngine::setup();

        e.expect(vec![
            // Lookup NS
            Box::new(|op, ns, _t| {
                match op {
                    OpKind::ServiceResolve(ServiceIdentifier{id, ..}) if id == Some(ns.id()) => Ok(Res::Service(ns.clone())),
                    _ => panic!("Unexpected operation: {:?}, expected get {}", op, ns.id()),
                }
            }),
            // Lookup target
            Box::new(|op, _ns, t| {
                match op {
                    OpKind::ServiceResolve(ServiceIdentifier{id, ..}) if id == Some(t.id()) => Ok(Res::Service(t.clone())),
                    _ => panic!("Unexpected operation: {:?}, expected get {}", op, t.id()),
                }
            }),
            // Attempt NS registration
            Box::new(|op, ns, _t| {
                match op {
                    OpKind::ServiceUpdate(id, f) if id == ns.id() => f(ns),
                    _ => panic!("Unexpected operation: {:?}, expected update {}", op, ns.id()),
                }
            }),
            // Publish pages to DHT
            Box::new(|op, ns, t| {
                match op {
                    OpKind::DhtPut(_id, pages) => {
                        // Check tertiary page info
                        let p = &pages[0];
                        let n = t.public_options().iter().name().unwrap();

                        assert_eq!(p.id, ns.resolve(&n).unwrap());
                        assert_eq!(p.info, PageInfo::Tertiary(Tertiary{target_id: t.id() }));
                        assert_eq!(p.public_options().iter().peer_id(), Some(ns.id()));

                        Ok(Res::Ids(vec![]))
                    },
                    _ => panic!("Unexpected operation: {:?}, expected update {}", op, ns.id()),
                }
            }),
        ]);

        let _r = e.register(RegisterOptions{ns: Some(ServiceIdentifier::id(ns_id)), target: ServiceIdentifier::id(target_id) }).await.unwrap();
    }

    #[async_std::test]
    async fn test_search() {
        let (e, ns_id, target_id) = MockEngine::setup();

        // Pre-generate registration page
        let (name, primary, tertiary) = e.with(|ns, t| {

            let (_n, primary) = t.publish_primary_buff(Default::default()).unwrap();

            let name = t.public_options().iter().name().unwrap();
            let tertiary = ns.publish_tertiary::<256, _>(t.id(), Default::default(), &name).unwrap();

            (name, Page::try_from(primary).unwrap(), tertiary)
        });
        let p = primary.clone();

        e.expect(vec![
            // Lookup NS
            Box::new(|op, ns, _t| {
                match op {
                    OpKind::ServiceResolve(ServiceIdentifier{id, ..}) if id == Some(ns.id()) => Ok(Res::Service(ns.clone())),
                    _ => panic!("Unexpected operation: {:?}, expected get {}", op, ns.id()),
                }
            }),
            // Lookup tertiary pages in dht
            Box::new(move |op, ns, _t| {
                match op {
                    OpKind::DhtGet(_id) => Ok(Res::Pages(vec![tertiary.clone()])),
                    _ => panic!("Unexpected operation: {:?}, expected update {}", op, ns.id()),
                }
            }),
            // Lookup primary pages in dht
            Box::new(move |op, ns, _t| {
                match op {
                    OpKind::DhtGet(id) if id == target_id => Ok(Res::Pages(vec![primary.clone()])),
                    _ => panic!("Unexpected operation: {:?}, expected update {}", op, ns.id()),
                }
            }),
        ]);

        let r = e.search(SearchOptions{ns: Some(ServiceIdentifier::id(ns_id)), search: SearchFilter::Name(name.value) }).await.unwrap();

        assert_eq!(&r, &[p]);
    }
}
