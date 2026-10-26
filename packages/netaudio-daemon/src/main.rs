use anyhow::{Context, Result};
use clap::Parser;
use netaudio_daemon::configuration::Configuration;
use netaudio_daemon::runtime::{self, RuntimeOptions};
use std::path::PathBuf;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::prelude::*;

#[derive(Parser)]
#[command(version, about = "NetAudio native daemon")]
struct Arguments {
    #[arg(long, env = "NETAUDIO_CONFIG")]
    config: Option<PathBuf>,
    #[arg(long, env = "NETAUDIO_PROFILE")]
    profile: Option<String>,
    #[arg(long, env = "NETAUDIO_DAEMON_PORT")]
    port: Option<u16>,
    #[arg(long)]
    interface: Option<String>,
    #[arg(long)]
    capture: bool,
    #[arg(long)]
    dissect: bool,
}

fn main() -> Result<()> {
    let arguments = Arguments::parse();
    initialize_logging()?;
    let mut runtime = tokio::runtime::Builder::new_multi_thread();
    if std::env::var_os("TOKIO_WORKER_THREADS").is_none() {
        let available = std::thread::available_parallelism()
            .context("determine available async executor parallelism")?;
        runtime.worker_threads(available.get().min(4));
    }
    runtime
        .enable_all()
        .build()
        .context("initialize async executor")?
        .block_on(run(arguments))
}

async fn run(arguments: Arguments) -> Result<()> {
    let configuration =
        Configuration::load(arguments.config.as_deref(), arguments.profile.as_deref())?;
    let cancellation = CancellationToken::new();
    let daemon = runtime::run(
        configuration,
        RuntimeOptions {
            port: arguments.port,
            interface: arguments.interface,
            capture: arguments.capture,
            dissect: arguments.dissect,
        },
        cancellation.clone(),
    );
    tokio::pin!(daemon);

    tokio::select! {
        result = &mut daemon => result,
        signal = shutdown_signal() => {
            cancellation.cancel();
            let result = daemon.await;
            signal?;
            result
        }
    }
}

fn initialize_logging() -> Result<()> {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let subscriber = tracing_subscriber::registry().with(filter);

    #[cfg(target_os = "linux")]
    {
        if std::env::var_os("JOURNAL_STREAM").is_some() {
            subscriber
                .with(tracing_journald::layer().context("connect structured journal logging")?)
                .try_init()?;
            return Ok(());
        }
    }

    subscriber
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
        .try_init()?;
    Ok(())
}

async fn shutdown_signal() -> Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut terminate = signal(SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result.context("receive interrupt signal"),
            result = terminate.recv() => result.context("termination signal stream closed"),
        }
    }

    #[cfg(not(unix))]
    tokio::signal::ctrl_c()
        .await
        .context("receive interrupt signal")
}
