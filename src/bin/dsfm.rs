
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

extern crate futures;
use futures::future::try_join_all;

extern crate async_std;
use async_std::task;

extern crate structopt;
use structopt::StructOpt;

#[macro_use]
extern crate tracing;
use tracing_futures::Instrument;

extern crate tracing_subscriber;
use tracing_subscriber::FmtSubscriber;
use tracing_subscriber::filter::LevelFilter;


use dsf_daemon::engine::{Engine, Options};

#[derive(Debug, StructOpt)]
#[structopt(name = "DSF Daemon Multi-runner")]
/// Distributed Service Framework (DSF) daemon multi-runner
struct Config {
    #[structopt(long = "count", default_value = "3")]
    /// Number of instances to run
    count: usize,

    #[structopt(flatten)]
    daemon_opts: Options,

    #[structopt(long = "log-level", default_value = "debug")]
    /// Enable verbose logging
    level: LevelFilter,
}

fn main() {
    // Fetch arguments
    let opts = Config::from_args();

    // Initialise logging
    let _ = FmtSubscriber::builder().with_max_level(opts.level.clone()).try_init();

    // Create running flag
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();

    // Bind exit handler
    ctrlc::set_handler(move || {
        r.store(false, Ordering::SeqCst);
    }).expect("Error setting Ctrl-C handler");

    // Create async task
    let res = task::block_on(async move {
        let mut handles = vec![];

        for i in 0..opts.count {
            let r = running.clone();
            let o = opts.daemon_opts.with_suffix(i+1);

            // Initialise daemon
            let mut d = match Engine::new(o).await {
                Ok(d) => d,
                Err(e) => {
                    error!("Error running daemon: {:?}", e);
                    return Err(e)
                }
            };


            let handle = task::spawn(async move {
                // Run daemon
                d.run(r).instrument(tracing::debug_span!("engine", i)).await
            });

            handles.push(handle);
        }


        if let Err(e) = try_join_all(handles).await {
            error!("Daemon runtime error: {:?}", e);
            return Err(e)
        }

        Ok(())
    });

    // Return error on failure
    if let Err(_e) = res {
        std::process::exit(-1);
    }
}
