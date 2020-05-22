use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{atomic::AtomicBool, Arc};
use std::time::Duration;

use rand::random;

extern crate futures;

extern crate async_std;
use async_std::task;

#[macro_use]
extern crate tracing;

extern crate tracing_subscriber;
use tracing_subscriber::{filter::LevelFilter, FmtSubscriber};

extern crate tracing_futures;
use tracing_futures::Instrument;

extern crate indicatif;
use indicatif::ProgressBar;
extern crate tempdir;
use tempdir::TempDir;

extern crate dsf_rpc;
use dsf_rpc::{self as rpc};

extern crate dsf_daemon;
use dsf_daemon::engine::{Engine, Options as EngineOptions};

extern crate dsf_client;
use dsf_client::{Client, Options as ClientOptions};

#[test]
fn smol_scale() {
    scale(10, LevelFilter::WARN);
}

#[test]
#[ignore]
fn large_scale() {
    scale(100, LevelFilter::WARN);
}

fn scale(n: usize, level: LevelFilter) {
    let d = TempDir::new("dsf-scale").unwrap();
    let d = d.path().to_str().unwrap().to_string();

    let _ = FmtSubscriber::builder().with_max_level(level).try_init();

    let mut daemons = vec![];
    let mut ids = vec![];

    // Create the runtime
    task::block_on(async move {
        // Set common configuration
        let mut config = EngineOptions::default();
        config.database_file = format!("{}/dsf-scale.db", d);
        config.bind_addresses = vec![SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0)];

        

        let running = Arc::new(AtomicBool::new(true));

        // Create instances
        info!("Creating {} virtual daemon instances", n);
        let bar = ProgressBar::new(n as u64);
        for i in 0..n {
            let c = config.with_suffix(i);
            let c1 = c.clone();

            // Ensure database does not exist
            let _ = std::fs::remove_file(c.database_file);

            let addr = c.daemon_socket.clone();
            let mut e = Engine::new(c1).await.expect("Error creating engine");

            let net_addr = e.addrs()[0];

            // Build and run daemon
            let r = running.clone();
            let handle = task::spawn(
                async move {
                    e.run(r).await.unwrap();
                }
                .instrument(tracing::debug_span!("instance", "{}", addr)),
            );

            // Create client
            let mut client =
                Client::new(&ClientOptions::new(&addr, Duration::from_secs(3))).expect("Error connecting to client");

            // Fetch client status and ID
            let status = client.status().await.expect("Error fetching client info");
            let id = status.id;

            // Add the new daemon to the list
            ids.push(id.clone());
            daemons.push((id, net_addr, client, handle));

            bar.inc(1);
        }
        bar.finish();
        info!("created daemons");

        let base_addr = daemons[0].1;

        info!("Connecting daemons via {}", d);
        let bar = ProgressBar::new((daemons.len() - 1) as u64);
        for (_id, _addr, client, _handle) in &mut daemons[1usize..] {
            client
                .connect(rpc::ConnectOptions {
                    address: base_addr,
                    id: None,
                    timeout: Some(Duration::from_secs(10)),
                })
                .await
                .expect("connecting failed");
            bar.inc(1);
        }
        bar.finish();
        info!("Daemons connected");

        info!("Attempting peer searches");
        let bar = ProgressBar::new((daemons.len()) as u64);
        for i in 0..daemons.len() {
            let mut j = random::<usize>() % daemons.len();
            if i == j {
                j = random::<usize>() % daemons.len();
            }

            let (_id, _addr, client, _handle) = &mut daemons[i];
            let id = ids[j].clone();

            client
                .find(rpc::peer::SearchOptions { id, timeout: None })
                .await
                .expect("search failed");
            bar.inc(1);
        }
        bar.finish();
        info!("Completed searches");
    });
}
