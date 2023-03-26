use async_std::task;

use futures::prelude::*;

use log::info;

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

    #[structopt(long = "log-level", default_value = "debug", env = "LOG_LEVEL")]
    /// Enable verbose logging
    log_level: LevelFilter,
}

#[async_std::main]
async fn main() -> Result<(), anyhow::Error> {
    // Fetch arguments
    let opts = Config::from_args();

    // Initialise logging
    let _ = FmtSubscriber::builder()
        .with_max_level(opts.log_level)
        .try_init();

    // Bind exit handler
    let mut exit_rx = Signals::new(vec![libc::SIGINT])?;

    // Initialise daemon
    let d = match Engine::new(opts.daemon_opts).await {
        Ok(d) => d,
        Err(e) => {
            return Err(anyhow::anyhow!("Daemon creation error: {:?}", e));
        }
    };

    // Spawn daemon instance
    let h = match d.start().await {
        Ok(i) => i,
        Err(e) => {
            return Err(anyhow::anyhow!("Daemon launch error: {:?}", e));
        }
    };

    // Setup exit task
    let mut exit_tx = h.exit_tx();
    task::spawn(async move {
        let _ = exit_rx.next().await;
        let _ = exit_tx.send(()).await;
    });

    // Execute daemon / await completion
    let res = h.join().await;

    info!("Exiting");

    // Return error on failure
    if let Err(e) = res {
        return Err(anyhow::anyhow!("Daemon error: {:?}", e));
    }

    Ok(())
}
