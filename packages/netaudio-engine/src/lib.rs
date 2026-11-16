#![allow(clippy::missing_safety_doc)]

use anyhow::{Context, Result};
use netaudio_daemon::configuration::Configuration;
use netaudio_daemon::runtime::{self, RuntimeOptions};
use std::ffi::{c_char, CStr};
use std::net::SocketAddr;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Mutex, Once};
use std::thread::JoinHandle;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

const SETTINGS: &str = "[daemon]\nevent_journal_path = \"event_journal.sqlite\"\nlevel_history_path = \"level_history.sqlite\"\nshure_memory_path = \"shure_devices.json\"\n";
const START_DEADLINE: Duration = Duration::from_secs(30);
const WORKER_THREADS: usize = 2;

static ENGINE: Mutex<Option<Engine>> = Mutex::new(None);
static LOGGING: Once = Once::new();

#[repr(C)]
pub enum NetaudioEngineStatus {
    Ok = 0,
    InvalidArgument = 1,
    Failed = 2,
}

struct Engine {
    cancellation: CancellationToken,
    thread: JoinHandle<()>,
}

impl Engine {
    fn finish(self) {
        self.cancellation.cancel();

        if self.thread.join().is_err() {
            tracing::error!("the engine thread panicked while stopping");
        }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn netaudio_engine_start(
    directory: *const c_char,
    port: *mut u16,
    message: *mut c_char,
    capacity: usize,
) -> NetaudioEngineStatus {
    if directory.is_null() || port.is_null() {
        return NetaudioEngineStatus::InvalidArgument;
    }

    let Ok(directory) = unsafe { CStr::from_ptr(directory) }.to_str() else {
        return NetaudioEngineStatus::InvalidArgument;
    };
    let directory = PathBuf::from(directory);

    match catch_unwind(AssertUnwindSafe(|| start(&directory))) {
        Ok(Ok(address)) => {
            unsafe { port.write(address.port()) };
            NetaudioEngineStatus::Ok
        }
        Ok(Err(error)) => {
            unsafe { describe(&format!("{error:#}"), message, capacity) };
            NetaudioEngineStatus::Failed
        }
        Err(_) => {
            unsafe { describe("The engine panicked while starting", message, capacity) };
            NetaudioEngineStatus::Failed
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn netaudio_engine_stop() {
    let engine = ENGINE.lock().ok().and_then(|mut engine| engine.take());

    if let Some(engine) = engine {
        engine.finish();
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn netaudio_engine_running() -> bool {
    ENGINE.lock().is_ok_and(|engine| {
        engine
            .as_ref()
            .is_some_and(|engine| !engine.thread.is_finished())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn netaudio_engine_network_changed() {
    #[cfg(target_os = "ios")]
    netaudio_daemon::network::interfaces_changed();
}

fn start(directory: &Path) -> Result<SocketAddr> {
    let mut engine = ENGINE
        .lock()
        .map_err(|_| anyhow::anyhow!("The engine state is unusable after a panic"))?;

    if let Some(previous) = engine.take() {
        previous.finish();
    }

    LOGGING.call_once(|| {
        use tracing_subscriber::prelude::*;

        let filter = tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
        let _ = tracing_subscriber::registry()
            .with(filter)
            .with(
                tracing_subscriber::fmt::layer()
                    .with_ansi(false)
                    .with_writer(std::io::stderr),
            )
            .try_init();
    });
    std::fs::create_dir_all(directory)
        .with_context(|| format!("create the engine directory {}", directory.display()))?;
    let settings = directory.join("config.toml");

    if !settings.exists() {
        std::fs::write(&settings, SETTINGS)
            .with_context(|| format!("write the engine settings {}", settings.display()))?;
    }

    let configuration = Configuration::load(Some(&settings), None)?;
    let cancellation = CancellationToken::new();
    let (report, reported) = mpsc::channel();
    let thread = std::thread::Builder::new()
        .name("netaudio-engine".to_owned())
        .spawn({
            let cancellation = cancellation.clone();
            move || serve(configuration, cancellation, report)
        })
        .context("start the engine thread")?;

    match reported.recv_timeout(START_DEADLINE) {
        Ok(Ok(address)) => {
            *engine = Some(Engine {
                cancellation,
                thread,
            });
            Ok(address)
        }
        Ok(Err(error)) => {
            let _ = thread.join();
            Err(error)
        }
        Err(_) => {
            cancellation.cancel();
            let _ = thread.join();
            anyhow::bail!("The engine did not start listening in time")
        }
    }
}

fn serve(
    configuration: Configuration,
    cancellation: CancellationToken,
    report: mpsc::Sender<Result<SocketAddr>>,
) {
    let executor = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(WORKER_THREADS)
        .enable_all()
        .build()
    {
        Ok(executor) => executor,
        Err(error) => {
            let _ = report.send(Err(error).context("initialize the engine executor"));
            return;
        }
    };

    executor.block_on(async {
        let (listening, listened) = tokio::sync::oneshot::channel();
        let engine = runtime::run(
            configuration,
            RuntimeOptions {
                embedded: true,
                listening: Some(listening),
                ..RuntimeOptions::default()
            },
            cancellation,
        );
        tokio::pin!(engine);

        tokio::select! {
            biased;
            address = listened => {
                if let Ok(address) = address {
                    let _ = report.send(Ok(address));
                }
            }
            result = &mut engine => {
                let _ = report.send(Err(result
                    .err()
                    .unwrap_or_else(|| anyhow::anyhow!("The engine stopped before it was listening"))));
                return;
            }
        }

        if let Err(error) = engine.await {
            tracing::error!(error = %format_args!("{error:#}"), "the engine stopped after a failure");
        }
    });
}

unsafe fn describe(text: &str, message: *mut c_char, capacity: usize) {
    if message.is_null() || capacity == 0 {
        return;
    }

    let mut length = text.len().min(capacity - 1);

    while !text.is_char_boundary(length) {
        length -= 1;
    }

    unsafe {
        std::ptr::copy_nonoverlapping(text.as_ptr(), message.cast::<u8>(), length);
        message.add(length).write(0);
    }
}
