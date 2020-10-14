use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

extern crate async_std;
use async_std::task;

extern crate futures;

extern crate ctrlc;

extern crate structopt;
use structopt::StructOpt;

#[macro_use]
extern crate tracing;

#[cfg(feature = "profile")]
extern crate flame;

extern crate tracing_subscriber;
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::FmtSubscriber;

use dsf_daemon::engine::{Engine, Options};

#[derive(Debug, StructOpt)]
#[structopt(name = "DSF Daemon")]
/// Distributed Service Framework (DSF) daemon
struct Config {
    #[structopt(flatten)]
    daemon_opts: Options,

    #[structopt(long = "profile")]
    profile: Option<String>,

    #[structopt(long = "log-level", default_value = "debug")]
    /// Enable verbose logging
    level: LevelFilter,
}

fn main() {
    // Fetch arguments
    let opts = Config::from_args();

    // Initialise logging
    let _ = FmtSubscriber::builder()
        .with_max_level(opts.level.clone())
        .try_init();

    // Create running flag
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();

    // Bind exit handler
    ctrlc::set_handler(move || {
        r.store(false, Ordering::SeqCst);
    })
    .expect("Error setting Ctrl-C handler");

    // Create async task
    let res = task::block_on(async move {
        // Initialise daemon
        let mut d = match Engine::new(opts.daemon_opts).await {
            Ok(d) => d,
            Err(e) => {
                error!("Error running daemon: {:?}", e);
                return Err(e);
            }
        };

        // Run daemon
        if let Err(e) = d.run(running).await {
            error!("Daemon runtime error: {:?}", e);
            return Err(e);
        }

        Ok(())
    });

    info!("Exiting");

    // Return error on failure
    if let Err(_e) = res {
        std::process::exit(-1);
    }
}
