use async_std::task;

use futures::prelude::*;

use log::{error, info};

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

    #[structopt(long = "log-level", default_value = "debug", env="LOG_LEVEL")]
    /// Enable verbose logging
    log_level: LevelFilter,
}

fn main() {
    // Fetch arguments
    let opts = Config::from_args();

    // Initialise logging
    let _ = FmtSubscriber::builder()
        .with_max_level(opts.log_level.clone())
        .try_init();

    // Create async task
    let res = task::block_on(async move {
        // Bind exit handler
        let mut exit_rx = Signals::new(vec![libc::SIGINT]).expect("Error setting Ctrl-C handler");

        // Initialise daemon
        let d = match Engine::new(opts.daemon_opts).await {
            Ok(d) => d,
            Err(e) => {
                error!("Daemon creation error: {:?}", e);
                return Err(e);
            }
        };

        // Spawn daemon instance
        let h = match d.start().await {
            Ok(i) => i,
            Err(e) => {
                error!("Daemon launch error: {:?}", e);
                return Err(e);
            }
        };

        // Setup exit task
        let mut exit_tx = h.exit_tx();
        task::spawn(async move {
            let _ = exit_rx.next().await;
            let _ = exit_tx.send(()).await;
        });

        // Execute daemon / await completion
        if let Err(e) = h.join().await {
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
