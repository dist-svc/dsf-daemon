
use async_std::task;

use futures::prelude::*;
use futures::select;
use futures::channel::oneshot;

use log::{info, error};

use structopt::StructOpt;

use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::FmtSubscriber;

use async_signals::Signals;

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

    // Bind exit handler
    let mut exit_rx = Signals::new(vec![libc::SIGINT]).expect("Error setting Ctrl-C handler");

    // Create async task
    let res = task::block_on(async move {
        // Initialise daemon
        let mut d = match Engine::new(opts.daemon_opts).await {
            Ok(d) => d,
            Err(e) => {
                error!("Daemon creation error: {:?}", e);
                return Err(e);
            }
        };

        // Run daemon
        let h = match d.start().await {
            Ok(i) => i,
            Err(e) => {
                error!("Daemon launch error: {:?}", e);
                return Err(e);
            }
        };

        // Await exit signal
        // TODO: this means we can't _currently_ abort on internal daemon errors?!
        let _ = exit_rx.next().await;

        // Block and return exit code
        if let Err(e) = h.exit().await {
            error!("Daemon error: {:?}", e);
        }

        Ok(())
    });

    info!("Exiting");

    // Return error on failure
    if let Err(_e) = res {
        std::process::exit(-1);
    }
}
