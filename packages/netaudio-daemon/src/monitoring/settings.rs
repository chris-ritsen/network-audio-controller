use super::metering_streams::MeteringStreams;
use super::multicast::{MAXIMUM_DATAGRAM_BYTES, MulticastReceiver, ReceivedPacket};
use crate::network::NetworkSnapshot;
use crate::preferences::Preferences;
use crate::state::ReadModel;
use anyhow::{Context, Result, ensure};
use netaudio_core::protocol::MULTICAST_GROUP_CONTROL_MONITORING;
use serde_json::{Value, json};
use std::net::Ipv4Addr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, Ordering};
use tokio::sync::{mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;

const REQUEST_CAPACITY: usize = 16;
const PORT_FAILURE: &str =
    "That port could not be opened or saved. Check whether another application is using it.";

pub struct Settings {
    preferences: Arc<Preferences>,
    default_port: u16,
    active_port: AtomicU16,
    commands: mpsc::Sender<Change>,
}

struct Change {
    port: u16,
    result: oneshot::Sender<Result<Value>>,
}

pub struct Worker {
    settings: Arc<Settings>,
    streams: Arc<MeteringStreams>,
    model: Arc<ReadModel>,
    network: watch::Receiver<NetworkSnapshot>,
    selected: Option<Ipv4Addr>,
    receiver: MulticastReceiver,
    commands: mpsc::Receiver<Change>,
    observer: Option<crate::capture::Observer>,
}

impl Settings {
    pub fn bind(
        ports: (u16, u16),
        preferences: Arc<Preferences>,
        streams: Arc<MeteringStreams>,
        model: Arc<ReadModel>,
        network: watch::Receiver<NetworkSnapshot>,
        selected: Option<Ipv4Addr>,
        observer: Option<crate::capture::Observer>,
    ) -> Result<(Arc<Self>, Worker)> {
        let (active, default) = ports;
        let interfaces = network.borrow().ipv4_interface_indices(selected)?;
        let receiver = MulticastReceiver::bind_exclusive(
            MULTICAST_GROUP_CONTROL_MONITORING.parse()?,
            active,
            &interfaces.into_iter().collect::<Vec<_>>(),
        )?
        .observe(observer.clone());
        let (commands, requests) = mpsc::channel(REQUEST_CAPACITY);
        let settings = Arc::new(Self {
            preferences,
            default_port: default,
            active_port: AtomicU16::new(active),
            commands,
        });
        let worker = Worker {
            settings: Arc::clone(&settings),
            streams,
            model,
            network,
            selected,
            receiver,
            commands: requests,
            observer,
        };
        Ok((settings, worker))
    }

    pub fn snapshot(&self) -> Result<Value> {
        let saved = self.preferences.snapshot()?;
        let desired = saved
            .get("monitoring_port")
            .cloned()
            .unwrap_or_else(|| json!(self.default_port));
        Ok(json!({
            "monitoring_port": desired,
            "active_monitoring_port": self.active_port.load(Ordering::Acquire),
        }))
    }

    pub async fn configure(&self, port: u16) -> Result<Value> {
        ensure!(
            port >= 1024,
            "Monitoring port must be an integer between 1024 and 65535"
        );
        let (result, received) = oneshot::channel();
        self.commands
            .try_send(Change { port, result })
            .context("monitoring settings are busy or unavailable")?;
        received
            .await
            .context("monitoring settings owner stopped")?
    }
}

impl Worker {
    pub async fn run(
        mut self,
        packets: mpsc::Sender<ReceivedPacket>,
        cancellation: CancellationToken,
    ) -> Result<()> {
        let mut buffer = vec![0; MAXIMUM_DATAGRAM_BYTES];

        loop {
            tokio::select! {
                _ = cancellation.cancelled() => return Ok(()),
                changed = self.network.changed() => {
                    changed.context("network monitor stopped")?;
                    self.receiver.update_interfaces(self.network.borrow_and_update().ipv4_interface_indices(self.selected)?)?;
                }
                command = self.commands.recv() => {
                    let Some(command) = command else {
                        return Ok(());
                    };
                    let result = self.configure(command.port).await;

                    if let Err(error) = &result {
                        tracing::warn!(port = command.port, %error, "monitoring port change failed");
                    }

                    let _ = command.result.send(result);
                }
                packet = self.receiver.receive(&mut buffer) => {
                    let packet = packet?;

                    tokio::select! {
                        _ = cancellation.cancelled() => return Ok(()),
                        result = packets.send(packet) => result.context("monitoring consumer stopped")?,
                    }
                }
            }
        }
    }

    async fn configure(&mut self, port: u16) -> Result<Value> {
        let previous = self.settings.active_port.load(Ordering::Acquire);
        let replacement = if previous != port {
            let interfaces = self
                .network
                .borrow()
                .ipv4_interface_indices(self.selected)?;
            let receiver = MulticastReceiver::bind_exclusive(
                MULTICAST_GROUP_CONTROL_MONITORING.parse()?,
                port,
                &interfaces.into_iter().collect::<Vec<_>>(),
            )
            .context(PORT_FAILURE)?
            .observe(self.observer.clone());

            if let Err(error) = self.streams.suspend().await {
                self.streams.resume(previous)?;
                return Err(error);
            }

            Some(receiver)
        } else {
            None
        };
        let preferences = Arc::clone(&self.settings.preferences);
        let saved = tokio::task::spawn_blocking(move || {
            preferences.set("monitoring_port".to_owned(), json!(port))
        })
        .await;
        let saved = saved
            .context("monitoring preferences writer failed")
            .and_then(|result| result);

        if let Err(error) = saved {
            if replacement.is_some() {
                self.streams.resume(previous)?;
            }

            return Err(error.context(PORT_FAILURE));
        }

        if let Some(receiver) = replacement {
            self.receiver = receiver;
            self.settings.active_port.store(port, Ordering::Release);
            self.streams.resume(port)?;
        }

        let settings = self.settings.snapshot()?;
        self.model
            .publish(json!({"event": "settings_updated", "settings": settings}))?;
        Ok(settings)
    }
}
