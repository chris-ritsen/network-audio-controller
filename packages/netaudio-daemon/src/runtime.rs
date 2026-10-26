use crate::authorization::AuthorizationStore;
use crate::configuration::{Configuration, DEFAULT_HTTP_PORT, http_address};
use crate::devices::{discovery, registry::DeviceRegistry};
use crate::http::{ApiError, AuthorizationService, ConnectionContext, inventory};
use crate::monitoring::multicast::MulticastReceiver;
use crate::network::{NetworkMonitor, NetworkSnapshot};
use crate::preferences::Preferences;
use crate::state::ReadModel;
use crate::transport::{Listener, ListenerOptions, TlsIdentity, generated};
use anyhow::{Context, Result, bail, ensure};
use axum::extract::{Extension, State};
use axum::middleware;
use axum::routing::{get, post};
use axum::{Json, Router};
use netaudio_core::protocol::{
    MULTICAST_GROUP_CONTROL_MONITORING, MULTICAST_GROUP_HEARTBEAT, NetaudioPort,
};
use serde_json::{Value, json};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

const DISCOVERY_QUEUE_CAPACITY: usize = 128;
const MONITORING_QUEUE_CAPACITY: usize = 256;
const SHUTDOWN_DEADLINE: Duration = Duration::from_secs(30);

#[derive(Default)]
pub struct RuntimeOptions {
    pub port: Option<u16>,
    pub interface: Option<String>,
    pub capture: bool,
    pub dissect: bool,
}

struct ServerState {
    information: Arc<Value>,
    cancellation: CancellationToken,
}

pub async fn run(
    configuration: Configuration,
    options: RuntimeOptions,
    cancellation: CancellationToken,
) -> Result<()> {
    let mcp_enabled = configuration.mcp_enabled()?;
    let network = NetworkMonitor::new()?;
    let snapshots = network.subscribe();
    let initial_network = snapshots.borrow().clone();
    let local_address = interface_address(&initial_network, options.interface.as_deref())?;
    let port = options.port.map(Ok).unwrap_or_else(|| {
        configuration
            .daemon
            .get("port")
            .map_or(Ok(DEFAULT_HTTP_PORT), |port| {
                port.as_integer()
                    .and_then(|port| u16::try_from(port).ok())
                    .filter(|port| *port != 0)
                    .context("[daemon] port must be an integer from 1 through 65535")
            })
    })?;
    ensure!(port != 0, "daemon port must be nonzero");
    let settings = configuration.tls()?;
    let identity = if let Some(settings) = &settings {
        ensure!(
            settings.port != port,
            "TLS port must differ from the HTTP port"
        );

        if settings.generated {
            generated::ensure(&settings.primary.certificate, &initial_network)?;
        }

        Some(TlsIdentity::open(
            settings.primary.clone(),
            settings.named.clone(),
        )?)
    } else {
        None
    };
    let preferences = Arc::new(Preferences::open(configuration.preferences_path()?)?);
    let configured_metering_port = std::env::var("NETAUDIO_METERING_PORT")
        .ok()
        .map(|port| port.parse::<u16>())
        .transpose()
        .context("NETAUDIO_METERING_PORT must be an integer port")?
        .unwrap_or(NetaudioPort::MulticastMetering as u16);
    let metering_port = preferences
        .snapshot()?
        .get("monitoring_port")
        .map(|value| {
            value
                .as_u64()
                .and_then(|value| u16::try_from(value).ok())
                .context("monitoring_port must be an integer port")
        })
        .transpose()?
        .unwrap_or(configured_metering_port);
    ensure!(
        metering_port >= 1024,
        "monitoring_port must be between 1024 and 65535"
    );
    let model = Arc::new(ReadModel::new()?);
    let capture = crate::capture::open(
        &configuration,
        options.capture,
        options.dissect,
        Arc::clone(&model),
    )
    .await?;
    let (wire_observer, capture_worker) = match capture {
        Some((observer, worker)) => (Some(observer), Some(worker)),
        None => (None, None),
    };
    let publication = crate::publication::Publisher::new(&configuration, Arc::clone(&model))?;
    let sap = crate::sap::Discovery::bind(
        crate::sap::Settings::from_environment()?,
        Arc::clone(&model),
        &initial_network,
        local_address,
    )?;
    let journal_limit = configuration
        .daemon
        .get("event_history_limit")
        .map(|value| {
            value
                .as_integer()
                .and_then(|value| usize::try_from(value).ok())
                .filter(|value| *value != 0)
                .context("daemon.event_history_limit must be a positive integer")
        })
        .transpose()?
        .unwrap_or(crate::monitoring::journal::model::DEFAULT_VIEW_LIMIT);
    let (journal, journal_worker) = crate::monitoring::journal::service::Journal::open(
        &configuration.event_journal_path()?,
        Arc::clone(&model),
        journal_limit,
        crate::monitoring::journal::model::Thresholds::from_configuration(&configuration.daemon)?,
    )
    .await?;
    let devices = DeviceRegistry::new(Arc::clone(&model), local_address, Arc::clone(&journal));
    if let Some(observer) = &wire_observer {
        devices.configure_capture(observer.clone())?;
    }
    let shure = crate::shure::Registry::new(
        Arc::clone(&model),
        Arc::clone(&devices.metering.history),
        Arc::clone(&journal),
        configuration.shure_memory_path()?,
        configuration.path.clone(),
    );
    let (managed, managed_worker) = crate::managed::registry::Registry::open(
        &configuration,
        Arc::clone(&model),
        Arc::clone(&journal),
    )
    .await?;
    devices.configure_locking(crate::devices::locking::Settings::load(&configuration)?)?;
    devices.configure_managed(Arc::clone(&managed))?;
    let journal_observations = model.subscribe_device_observations()?;
    let history_store = crate::monitoring::level_history::store::HistoryStore::open(
        &configuration.level_history_path()?,
        Arc::clone(&devices.metering.history),
    )
    .await?;
    let information = Arc::new(json!({
        "hostname": hostname::get()?.to_str().context("hostname is not UTF-8")?.trim_end_matches(".local"),
        "product": "netaudio",
        "version": env!("CARGO_PKG_VERSION"),
        "implementation": "rust",
        "build_id": env!("NETAUDIO_BUILD_ID"),
        "public_api": crate::http::versioning::descriptor(),
        "started_at": chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        "mcp": if mcp_enabled { json!({
            "enabled": true,
            "path": "/mcp",
            "protocol_version": crate::mcp::protocol::LEGACY_VERSION,
            "transport": "streamable-http",
        }) } else { json!({"enabled": false}) },
    }));
    let relay = crate::relay::Advertisement::new(
        &information,
        settings.as_ref().map_or(port, |settings| settings.port),
        identity.is_some(),
        identity.as_ref().map(TlsIdentity::subscribe_fingerprint),
    )?;
    let host_audio = crate::host_audio::manager::Manager::open(
        &configuration,
        information["hostname"]
            .as_str()
            .context("Daemon hostname is unavailable")?
            .to_owned(),
        Arc::clone(&journal),
        Arc::clone(&model),
        (
            Arc::clone(&history_store),
            Arc::clone(&devices.metering.history),
        ),
    )?;
    let (host_audio, host_audio_worker) = match host_audio {
        Some((manager, worker)) => (Some(manager), Some(worker)),
        None => (None, None),
    };
    let assets = crate::http::web::WebAssets::open(configuration.directory()?.join("web-assets"))?;
    let lifetime = cancellation.child_token();
    let _cancel_on_drop = lifetime.clone().drop_guard();
    let meter_streams = crate::monitoring::metering_streams::MeteringStreams::new(
        Arc::clone(&devices),
        Arc::clone(&model),
        metering_port,
        lifetime.clone(),
    );
    let (monitoring_settings, monitoring_worker) = crate::monitoring::settings::Settings::bind(
        (metering_port, configured_metering_port),
        Arc::clone(&preferences),
        Arc::clone(&meter_streams),
        Arc::clone(&model),
        snapshots.clone(),
        local_address,
        wire_observer.clone(),
    )?;
    let presets = Arc::new(crate::presets::Service::new(
        configuration.preset_directory()?,
    ));
    let observations = crate::host_audio::observations::Observations::new(&configuration)?;
    let signal = host_audio.as_ref().map(|manager| {
        crate::host_audio::signal::Service::new(
            manager.clone(),
            model.clone(),
            journal.clone(),
            meter_streams.clone(),
            shure.clone(),
            observations.clone(),
            lifetime.clone(),
        )
    });
    let mcp = if mcp_enabled {
        Some(crate::mcp::service::Service::new(
            crate::mcp::service::Dependencies {
                host_audio: host_audio.clone(),
                signal: signal.clone(),
                observations,
                model: Arc::clone(&model),
                information: Arc::clone(&information),
                registry: Arc::clone(&devices),
                metering: Arc::clone(&meter_streams),
                cancellation: lifetime.clone(),
                journal: Arc::clone(&journal),
                presets: Arc::clone(&presets),
                managed: Arc::clone(&managed),
                shure: Arc::clone(&shure),
            },
        )?)
    } else {
        None
    };
    let apps = mcp.as_ref().map(|service| Arc::clone(&service.apps));
    let state = Arc::new(ServerState {
        information,
        cancellation: cancellation.clone(),
    });
    let mut router = inventory::router(Arc::clone(&model))
        .merge(crate::http::host_audio::router(host_audio.clone()))
        .merge(crate::http::host_audio::signal_router(signal))
        .merge(crate::http::shure::router(Arc::clone(&shure)))
        .merge(crate::http::managed::router(
            Arc::clone(&managed),
            Arc::clone(&model),
        ))
        .merge(crate::http::documentation::router()?)
        .merge(crate::http::presets::router(
            presets,
            Arc::clone(&model),
            Arc::clone(&devices),
        ))
        .merge(crate::http::settings::router(monitoring_settings))
        .merge(crate::http::external_flows::router(
            Arc::clone(&model),
            sap.diagnostics(),
        ))
        .merge(crate::http::devices::router(
            Arc::clone(&devices),
            Arc::clone(&model),
            Arc::clone(&meter_streams),
        ))
        .merge(crate::http::journal::router(
            Arc::clone(&journal),
            Arc::clone(&devices),
        ));
    if let Some(mcp) = mcp {
        let authorization = Arc::new(AuthorizationStore::new(preferences));
        let oauth = AuthorizationService::new(Arc::clone(&authorization));
        router = router.merge(oauth.router()).merge(crate::http::mcp::router(
            mcp,
            authorization,
            oauth,
            lifetime.clone(),
        ));
    }
    let router = router
        .merge(
            Router::new()
                .route("/server-info", get(server_info))
                .route("/shutdown", post(shutdown))
                .with_state(state),
        )
        .fallback(|| async { ApiError::not_found("not found") })
        .layer(middleware::from_fn_with_state(
            assets,
            crate::http::web::serve,
        ))
        .layer(middleware::from_fn(crate::http::versioning::select));
    let plain = Listener::bind(
        http_address(port, identity.is_some()),
        None,
        ListenerOptions::default(),
    )
    .await?;
    let encrypted = match (&settings, &identity) {
        (Some(settings), Some(identity)) => Some(
            Listener::bind(
                SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), settings.port),
                Some(identity.acceptor()),
                ListenerOptions::default(),
            )
            .await?,
        ),
        _ => None,
    };
    let interfaces = initial_network
        .ipv4_interface_indices(local_address)?
        .into_iter()
        .collect::<Vec<_>>();
    let monitoring = MulticastReceiver::bind(
        MULTICAST_GROUP_CONTROL_MONITORING.parse()?,
        NetaudioPort::Info as u16,
        &interfaces,
    )?
    .observe(wire_observer.clone());
    let (discovered, discovery_events) = mpsc::channel(DISCOVERY_QUEUE_CAPACITY);
    let (packets, observations) = mpsc::channel(MONITORING_QUEUE_CAPACITY);
    let heartbeats = MulticastReceiver::bind(
        MULTICAST_GROUP_HEARTBEAT.parse()?,
        NetaudioPort::Heartbeat as u16,
        &interfaces,
    )?
    .observe(wire_observer);
    let (heartbeat_packets, heartbeat_observations) = mpsc::channel(MONITORING_QUEUE_CAPACITY);
    let (meter_packets, meter_observations) = mpsc::channel(MONITORING_QUEUE_CAPACITY);
    let mut tasks = JoinSet::new();
    let mut journal_worker = tokio::task::spawn_blocking(move || journal_worker.run());
    let mut journal_finished = false;
    tasks.spawn(Arc::clone(&journal).observe(Arc::clone(&model), journal_observations));
    tasks.spawn(managed_worker.run(lifetime.clone()));
    if let Some(worker) = capture_worker {
        tasks.spawn(worker.run(lifetime.clone()));
    }
    if let Some(publication) = publication {
        tasks.spawn(publication.run(lifetime.clone()));
    }
    if std::env::var("NETAUDIO_DBUS")
        .is_ok_and(|value| matches!(value.to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
    {
        #[cfg(unix)]
        tasks.spawn(crate::publication::bus::run(
            Arc::clone(&model),
            lifetime.clone(),
        ));
        #[cfg(not(unix))]
        bail!("D-Bus publication is unavailable on this platform");
    }
    if let Some(worker) = host_audio_worker {
        tasks.spawn(worker.run(lifetime.clone()));
    }
    tasks.spawn(
        Arc::clone(&managed).monitor_signals(Arc::clone(&devices.metering), lifetime.clone()),
    );
    tasks.spawn(Arc::clone(&devices.metering).run(lifetime.clone()));
    tasks.spawn(
        Arc::clone(&history_store).run(Arc::clone(&devices.metering.history), lifetime.clone()),
    );
    tasks.spawn(meter_streams.run());
    if let Some(apps) = apps {
        tasks.spawn(apps.run());
    }
    tasks.spawn(network.run(lifetime.clone()));
    let (shure_initialized, shure_ready) = tokio::sync::oneshot::channel();
    tasks.spawn(shure.run(lifetime.clone(), shure_initialized));
    tasks.spawn(sap.run(snapshots.clone(), local_address, lifetime.clone()));
    tasks.spawn(discovery::run(
        local_address,
        discovered,
        lifetime.clone(),
        snapshots.clone(),
        Some(relay),
        host_audio.map(|manager| Arc::clone(&manager.peers)),
    ));
    tasks.spawn(Arc::clone(&devices).run(discovery_events));
    tasks.spawn(Arc::clone(&devices).monitor(
        observations,
        heartbeat_observations,
        meter_observations,
    ));
    tasks.spawn(monitoring_worker.run(meter_packets, lifetime.clone()));
    tasks.spawn(monitoring.run(packets, lifetime.clone(), snapshots.clone(), local_address));
    tasks.spawn(heartbeats.run(
        heartbeat_packets,
        lifetime.clone(),
        snapshots.clone(),
        local_address,
    ));
    tracing::info!(address = %plain.local_address()?, "native HTTP listener ready");
    tasks.spawn(plain.serve(router.clone(), lifetime.clone()));

    if let Some(encrypted) = encrypted {
        tracing::info!(address = %encrypted.local_address()?, "native HTTPS listener ready");
        tasks.spawn(encrypted.serve(router, lifetime.clone()));
    }

    if let Some(settings) = settings
        && settings.generated
    {
        tasks.spawn(generated::maintain(
            settings.primary.certificate,
            snapshots,
            lifetime.clone(),
        ));
    }

    let initialization = tokio::time::timeout(std::time::Duration::from_secs(15), async {
        shure_ready
            .await
            .context("Wireless initialization ended without a result")?
            .map_err(|error| anyhow::anyhow!("Wireless initialization failed: {error}"))?;
        notify_ready()
    })
    .await
    .context("Daemon initialization exceeded its deadline")
    .and_then(|result| result);
    let outcome = match initialization {
        Err(error) => Err(error),
        Ok(()) => {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => Ok(()),
                completed = &mut journal_worker => {
                    journal_finished = true;
                    match completed {
                        Ok(Err(error)) => Err(error.context("event journal worker failed")),
                        Err(error) => Err(error.into()),
                        Ok(Ok(())) => Err(anyhow::anyhow!("event journal worker stopped unexpectedly")),
                    }
                }
                completed = tasks.join_next() => {
                    match completed {
                        Some(Ok(Err(error))) => Err(error.context("daemon component failed")),
                        Some(Err(error)) => Err(error.into()),
                        _ => Err(anyhow::anyhow!("daemon component stopped unexpectedly")),
                    }
                }
            }
        }
    };
    lifetime.cancel();
    notify_stopping();
    let cleanup = tokio::time::timeout(SHUTDOWN_DEADLINE, async {
        devices.stop().await;
        model.close()?;

        while let Some(result) = tasks.join_next().await {
            match result {
                Ok(Err(error)) => tracing::debug!(%error, "component ended during shutdown"),
                Err(error) => tracing::error!(%error, "component task failed during shutdown"),
                Ok(Ok(())) => {}
            }
        }

        history_store
            .flush(Arc::clone(&devices.metering.history))
            .await?;
        if !journal_finished {
            journal.stop().await?;
            (&mut journal_worker).await??;
        }
        Ok::<(), anyhow::Error>(())
    })
    .await;
    drop(identity);
    cleanup.context("daemon shutdown exceeded its deadline")??;
    outcome
}

fn interface_address(
    network: &NetworkSnapshot,
    selected: Option<&str>,
) -> Result<Option<Ipv4Addr>> {
    let Some(selected) = selected else {
        return Ok(None);
    };
    let matches = network
        .interfaces
        .iter()
        .filter(|interface| {
            interface.name == selected || interface.network.addr().to_string() == selected
        })
        .filter_map(|interface| match interface.network.addr() {
            IpAddr::V4(address) => Some(address),
            IpAddr::V6(_) => None,
        })
        .collect::<std::collections::BTreeSet<_>>();

    match matches.len() {
        0 => bail!("interface {selected} has no IPv4 address"),
        1 => Ok(matches.first().copied()),
        _ => bail!("interface {selected} has several IPv4 addresses; select an address explicitly"),
    }
}

async fn server_info(State(state): State<Arc<ServerState>>) -> axum::response::Response {
    inventory::json_value(Arc::clone(&state.information))
}

async fn shutdown(
    State(state): State<Arc<ServerState>>,
    Extension(connection): Extension<ConnectionContext>,
) -> Result<Json<Value>, ApiError> {
    if !connection.peer.ip().is_loopback() {
        return Err(ApiError {
            status: axum::http::StatusCode::FORBIDDEN,
            message: "forbidden".to_owned(),
        });
    }

    state.cancellation.cancel();
    Ok(Json(json!({"success": true})))
}

fn notify_ready() -> Result<()> {
    #[cfg(target_os = "linux")]
    sd_notify::notify(&[
        sd_notify::NotifyState::Ready,
        sd_notify::NotifyState::Status("Serving native device discovery and HTTP requests"),
    ])?;

    Ok(())
}

fn notify_stopping() {
    #[cfg(target_os = "linux")]
    if let Err(error) = sd_notify::notify(&[sd_notify::NotifyState::Stopping]) {
        tracing::warn!(%error, "could not notify service manager of shutdown");
    }
}
