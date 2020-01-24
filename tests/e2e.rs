use std::time::Duration;

extern crate futures;
use futures::{Future};

extern crate async_std;
use async_std::prelude::*;
use async_std::task;

extern crate tracing_subscriber;
use tracing_subscriber::{FmtSubscriber, filter::LevelFilter};

extern crate indicatif;
use indicatif::{ProgressBar};

extern crate tempdir;
use tempdir::TempDir;

extern crate dsf_core;
use dsf_core::api::*;

extern crate dsf_daemon;
use dsf_daemon::error::Error;
use dsf_daemon::engine::{Engine, Options};

extern crate dsf_client;
use dsf_client::Client;

extern crate dsf_rpc;
use dsf_rpc::{self as rpc};


#[macro_use]
extern crate log;

const NUM_DAEMONS: usize = 4;

#[test]
fn end_to_end() {
    let d = TempDir::new("dsf-e2e").unwrap();

    let _ = FmtSubscriber::builder().with_max_level(LevelFilter::DEBUG).try_init();

    let mut daemons = vec![];

    let res: Result<(), Error> = task::block_on(async move {

        let mut config = Options::default();
        config.daemon_options.database_dir = d.path().to_str().unwrap().to_string();

        // Create daemons
        error!("Creating daemons");
        let bar = ProgressBar::new(NUM_DAEMONS as u64);
        for i in 0..NUM_DAEMONS {
            
            let c = config.with_suffix(i);
            let c1 = c.clone();

            let addr = c.daemon_socket.clone();

            // Build daemon instance
            let mut daemon = Engine::new(c1).await.unwrap();

            // Run daemon
            let handle = task::spawn(async move {
                // Launch the daemon
                daemon.run().await
            });

            // Create client
            let mut client = Client::new(&addr, Duration::from_secs(1)).unwrap();

            // Fetch client status and ID
            let status = client.status().await.unwrap();
            let id = status.id;

            // Add the new daemon to the list
            daemons.push((id, c, client, handle));

            bar.inc(1);
        }
        bar.finish();
        error!("created daemons");

        error!("connecting to peers");
        let base_config = daemons[0].1.clone();

        let bar = ProgressBar::new((NUM_DAEMONS-1) as u64);
        for (id, config, client, _) in &mut daemons[1..] {

            client.connect(rpc::ConnectOptions{address: base_config.bind_addresses[0], id: None, timeout: Some(Duration::from_secs(10)) }).await.expect("connecting failed");

            bar.inc(1);
        }
        bar.finish();
        error!("connecting complete");

        let mut services = vec![];

        error!("creating services");
        let bar = ProgressBar::new(NUM_DAEMONS as u64);
        for (id, config, client, _) in &mut daemons[..] {
            
            let s = client.create(&rpc::CreateOptions::default().and_register()).await.expect("creation failed");
            services.push(s);

            bar.inc(1);
        }
        bar.finish();
        error!("created services");

        error!("searching for services");
        let bar = ProgressBar::new(NUM_DAEMONS as u64);
        for i in 0..NUM_DAEMONS {

            let id = &daemons[NUM_DAEMONS - i - 1].0.clone();

            let (_id, _config, client, _handle) = &mut daemons[i];

            let _service_handle = client.locate(&id).await.expect("search failed");

            bar.inc(1);
        }
        bar.finish();
        error!("searching for services");

        error!("test complete, exiting");

        Ok(())

    });

    res.unwrap();
}