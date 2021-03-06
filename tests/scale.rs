use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use log::info;
use rand::random;

use async_std::task;

use tracing_subscriber::{filter::LevelFilter, FmtSubscriber};

use indicatif::ProgressBar;
use tempdir::TempDir;

use dsf_client::{Client, Options as ClientOptions};
use dsf_daemon::engine::{Engine, Options as EngineOptions};
use dsf_rpc::{self as rpc};

#[test]
fn smol_scale() {
    scale(3, LevelFilter::DEBUG);
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
        config.bind_addresses = vec![SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            11200,
        )];

        // Create instances
        info!("Creating {} virtual daemon instances", n);
        let bar = ProgressBar::new(n as u64);
        for i in 0..n {
            let c = config.with_suffix(i);
            let c1 = c.clone();

            // Ensure database does not exist
            let _ = std::fs::remove_file(c.database_file);

            let addr = c.daemon_socket.clone();
            let net_addr = c.bind_addresses[0].clone();
            let e = Engine::new(c1).await.expect("Error creating engine");

            // Build and run daemon
            let handle = e.start().await.unwrap();

            // Create client
            let mut client = Client::new(&ClientOptions::new(&addr, Duration::from_secs(3)))
                .expect("Error connecting to client");

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
