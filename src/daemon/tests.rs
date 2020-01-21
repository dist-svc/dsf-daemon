use std::net::{SocketAddr, IpAddr, Ipv4Addr};
use std::time::Duration;

extern crate async_std;
use async_std::task;

extern crate tracing_subscriber;
use tracing_subscriber::FmtSubscriber;
use tracing_subscriber::filter::LevelFilter;


extern crate tempdir;
use tempdir::TempDir;

use futures::prelude::*;

use kad::prelude::*;
use kad::store::Datastore;

//use rr_mux::mock::{MockConnector, MockTransaction};

use dsf_core::prelude::*;
use dsf_core::types::{Error, Address, Flags};
use dsf_core::service::{ServiceBuilder, Publisher};
use dsf_core::net::{self, Request, RequestKind, Response, ResponseKind};
use dsf_rpc::{self as rpc};

use crate::core::peers::{PeerState};
use crate::io::mock::{MockConnector, MockTransaction};
use super::{Dsf, Options};

#[test]
fn test_manager() {

    let mut buff = vec![0u8; 1024];

    // Initialise logging
    let _ = FmtSubscriber::builder().with_max_level(LevelFilter::DEBUG).try_init();

    let d = TempDir::new("/tmp/").unwrap(); 

    let mut config = Options::default();
    config.database_dir = d.path().to_str().unwrap().to_string();
    let mut mux = MockConnector::new();

    let mut service = Service::default();
    let mut dsf = Dsf::new(config, service, mux.clone()).unwrap();
    let id1 = dsf.id().clone();
    let _addr1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 0, 0, 1)), 8111);

    let (a2, s2) = (SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 0, 0, 3)), 8112), ServiceBuilder::default().generic().build().unwrap());
    let (a3, s3) = (SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 0, 0, 3)), 8113), ServiceBuilder::default().generic().build().unwrap());
    let (a4, mut s4) = (SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 0, 0, 3)), 8114), ServiceBuilder::default().generic().build().unwrap());
    let mut peers = vec![(&a2, &s2), (&a3, &s3), (&a4, &s4)];
    peers.sort_by_key(|(_, s)| { DhtDatabaseId::xor(&id1.clone(), &s.id()) });

    task::block_on(async {

        println!("Responds to pings");

        assert_eq!(
            dsf.handle(a2, Request::new(s2.id(), RequestKind::Ping, Flags::ADDRESS_REQUEST)).unwrap(),
            Response::new(id1.clone(), rand::random(), ResponseKind::NoResult, Flags::default()),
        );

        println!("Connect function");

        mux.expect(vec![
            MockTransaction::request(a2, 
                Request::new(id1.clone(), RequestKind::FindNode(id1.clone()), Flags::ADDRESS_REQUEST | Flags::PUB_KEY_REQUEST).with_public_key(dsf.pub_key()),
                Ok( Response::new(s2.id(), rand::random(), ResponseKind::NodesFound(id1.clone(), vec![(s3.id(), a3, s3.public_key())]), Flags::default()).with_public_key(s2.public_key()) )
            ),
            MockTransaction::request(a3, 
                Request::new(id1.clone(), RequestKind::FindNode(id1.clone()), Flags::PUB_KEY_REQUEST).with_public_key(dsf.pub_key()),
                Ok( Response::new(s3.id(), rand::random(), ResponseKind::NodesFound(id1.clone(), vec![]), Flags::default()) )
            ),
        ]);

        // Run connect function
        dsf.connect(rpc::ConnectOptions{address: a2, id: None, timeout: Duration::from_secs(10).into()}).await.unwrap();

        // Check messages have been sent
        mux.finalise();

        // Check peer and public key have been registered
        let peer = dsf.peers.find(&s2.id()).unwrap();
        assert_eq!(peer.state(), PeerState::Known(s2.public_key()));
        assert!(!peer.seen().is_none());

        let peer = dsf.peers.find(&s3.id()).unwrap();
        assert_eq!(peer.state(), PeerState::Known(s3.public_key()));
        // TODO: seen not updated because MockConnector bypasses .handle() :-/
        //assert!(!peer.seen().is_none());

        println!("Responds to find_nodes");

        let mut nodes = vec![(s2.id(), a2, s2.public_key()), (s3.id(), a3, s3.public_key())];
        nodes.sort_by_key(|(id, _, _)| { DhtDatabaseId::xor(&s4.id(), &id) });

        assert_eq!(
            dsf.handle(a2, Request::new(s2.id().clone(), RequestKind::FindNode(s4.id().clone()), Flags::default())).unwrap(),
            Response::new(id1.clone(), rand::random(), ResponseKind::NodesFound(s4.id().clone(), nodes), Flags::default()),
        );

        println!("Handles store requests");

        let (_n, p4) = s4.publish_primary(&mut buff).unwrap();

        assert_eq!(
            dsf.handle(a4, Request::new(s4.id().clone(), RequestKind::Store(s4.id().clone(), vec![p4.clone()]), Flags::default())).unwrap(),
            Response::new(id1.clone(), rand::random(), ResponseKind::ValuesFound(s4.id().clone(), vec![p4.clone()]), Flags::default()),
        );

        println!("Responds to page requests");

        assert_eq!(
            dsf.handle(a4, Request::new(s4.id().clone(), RequestKind::FindValue(s4.id().clone()), Flags::default())).unwrap(),
            Response::new(id1.clone(), rand::random(), ResponseKind::ValuesFound(s4.id().clone(), vec![p4.clone()]), Flags::default()),
        );

        println!("Register function");

        let (_n, p1) = dsf.primary(&mut buff).unwrap();

        // Sort peers by distance from peer 1
        let mut peers = vec![(a2, s2.id(), s2.public_key()), (a3, s3.id(), s3.public_key()), (a4, s4.id().clone(), s4.public_key())];
        peers.sort_by_key(|(_addr, id, _pk)| { DhtDatabaseId::xor(&id1, &id) });

        // Generate expectation vector
        let mut searches: Vec<_> = peers.iter().map(|(addr, id, _pk)| {
            MockTransaction::request(addr.clone(), 
                Request::new(id1.clone(), RequestKind::FindNode(id1.clone()), Flags::PUB_KEY_REQUEST).with_public_key(dsf.pub_key()),
                Ok( Response::new(id.clone(), rand::random(), ResponseKind::NoResult, Flags::default()) )
            )
        }).collect();

        let mut stores: Vec<_> = peers.iter().map(|(addr, id, pk)| {
            MockTransaction::request(addr.clone(), 
                Request::new(id1.clone(), RequestKind::Store(id1.clone(), vec![p1.clone()]), Flags::PUB_KEY_REQUEST).with_public_key(dsf.pub_key()),
                Ok( Response::new(id.clone(), rand::random(), ResponseKind::ValuesFound(id1.clone(), vec![p1.clone()]), Flags::default()).with_public_key(pk.clone()) )
            )
        }).collect();

        let mut transactions = vec![];
        transactions.append(&mut searches);
        transactions.append(&mut stores);

        // Run register function
        mux.expect(transactions.clone());
        dsf.store(&id1, vec![p1.clone()]).await.unwrap();
        mux.finalise();

        // Repeated registration has no effect (page not duplicated)
        mux.expect(transactions);
        dsf.store(&id1, vec![p1.clone()]).await.unwrap();
        mux.finalise();


        println!("Generates services");

        mux.expect(vec![]);
        let info = dsf.create(rpc::CreateOptions::default()).await.expect("error creating service");
        mux.finalise();

        let pages = dsf.store.find(&info.id).expect("no internal store entry found");
        let page = &pages[0];

        println!("Registers services");
        
        peers.sort_by_key(|(_addr, id, _pk)| { DhtDatabaseId::xor(&info.id, &id) });

        let mut searches: Vec<_> = peers.iter().map(|(addr, id, _pk)| {
            MockTransaction::request(addr.clone(), 
                Request::new(id1.clone(), RequestKind::FindNode(info.id.clone()), Flags::PUB_KEY_REQUEST).with_public_key(dsf.pub_key()),
                Ok( Response::new(id.clone(), rand::random(), ResponseKind::NoResult, Flags::default()) )
            )
        }).collect();

        let mut stores: Vec<_> = peers.iter().map(|(addr, id, pk)| {
            MockTransaction::request(addr.clone(), 
                Request::new(id1.clone(), RequestKind::Store(info.id.clone(), vec![page.clone()]), Flags::PUB_KEY_REQUEST).with_public_key(dsf.pub_key()),
                Ok( Response::new(id.clone(), rand::random(), ResponseKind::ValuesFound(id1.clone(), vec![page.clone()]), Flags::default()).with_public_key(pk.clone()) )
            )
        }).collect();

        let mut transactions = vec![];
        transactions.append(&mut searches);
        transactions.append(&mut stores);

        mux.expect(transactions.clone());
        dsf.register(rpc::RegisterOptions{service: rpc::ServiceIdentifier{id: Some(info.id.clone()), index: None}, no_replica: true}).await.expect("Registration error");
        mux.finalise();

        println!("Publishes data");

        mux.expect(vec![]);
        dsf.publish(rpc::PublishOptions::new(info.id.clone())).await.expect("publishing error");
        mux.finalise();

        println!("Responds to subscribe requests");

        assert_eq!(
            dsf.handle(a4, Request::new(s4.id().clone(), RequestKind::Subscribe(info.id.clone()), Flags::default())).unwrap(),
            Response::new(id1.clone(), rand::random(), ResponseKind::Status(net::Status::Ok), Flags::default()),
        );

        let service_inst = dsf.services.find(&info.id).unwrap();
        let service_inst = service_inst.write().unwrap();
        assert_eq!(service_inst.subscribers.len(), 1);
        let _subscriber = service_inst.subscribers.get(&s4.id()).expect("subscriber entry not found for service");


        println!("Publishes data to subscribers");

        mux.expect(vec![
            MockTransaction::request(a4.clone(), 
                Request::new(s4.id().clone(), RequestKind::PushData(info.id.clone(), vec![page.clone()]), Flags::PUB_KEY_REQUEST).with_public_key(dsf.pub_key()),
                Ok( Response::new(id1.clone(), rand::random(), ResponseKind::ValuesFound(id1.clone(), vec![page.clone()]), Flags::default()) )
            )
        ]);


    });
}