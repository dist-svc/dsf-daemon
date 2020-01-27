use std::time::Duration;

extern crate futures;

extern crate async_std;
use async_std::task;

extern crate tracing_subscriber;
use tracing_subscriber::{FmtSubscriber, filter::LevelFilter};

extern crate tracing_futures;
use tracing_futures::Instrument;

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

const NUM_DAEMONS: usize = 3;

#[test]
fn end_to_end() {
    let d = TempDir::new("dsf-e2e").unwrap();

    let _ = FmtSubscriber::builder().with_max_level(LevelFilter::DEBUG).try_init();

    let mut daemons = vec![];

    let res: Result<(), Error> = task::block_on(async move {

        let mut config = Options::default();
        config.daemon_options.database_dir = d.path().to_str().unwrap().to_string();

        // Create daemons
        info!("Creating daemons");
        let bar = ProgressBar::new(NUM_DAEMONS as u64);
        for i in 0..NUM_DAEMONS {
            
            let c = config.with_suffix(i);
            let c1 = c.clone();

            let addr = c.daemon_socket.clone();
            let mut e = Engine::new(c1).await.expect("Error creating engine");

            // Build and run daemon
            let handle = task::spawn(async move {
                e.run().await.unwrap();
            }.instrument(tracing::debug_span!("instance", "{}", addr)) );

            // Create client
            let mut client = Client::new(&addr, Duration::from_secs(1)).expect("Error connecting to client");

            // Fetch client status and ID
            let status = client.status().await.expect("Error fetching client info");
            let id = status.id;

            // Add the new daemon to the list
            daemons.push((id, c, client, handle));

            bar.inc(1);
        }
        bar.finish();
        info!("created daemons");

        info!("connecting to peers");
        let base_config = daemons[0].1.clone();

        let bar = ProgressBar::new((NUM_DAEMONS-1) as u64);
        for (_id, _config, client, _) in &mut daemons[1..] {

            client.connect(rpc::ConnectOptions{address: base_config.bind_addresses[0], id: None, timeout: Some(Duration::from_secs(10)) }).await.expect("connecting failed");

            bar.inc(1);
        }
        bar.finish();
        info!("connecting complete");


        let mut services = vec![];

        error!("creating services");
        let bar = ProgressBar::new(NUM_DAEMONS as u64);
        for (_id, _config, client, _) in &mut daemons[..] {
            
            let s = client.create(rpc::CreateOptions::default().and_register()).await.expect("creation failed");
            services.push(s);

            bar.inc(1);
        }
        bar.finish();
        info!("created services");

        info!("searching for services");
        let bar = ProgressBar::new(NUM_DAEMONS as u64);
        for i in 0..NUM_DAEMONS {

            let id = &daemons[NUM_DAEMONS - i - 1].0.clone();

            let (_id, _config, client, _handle) = &mut daemons[i];

            let _service_handle = client.locate(&id).await.expect("search failed");

            bar.inc(1);
        }
        bar.finish();
        info!("searching for services");

        info!("test complete, exiting");

        Ok(())

    });

    res.unwrap();
}