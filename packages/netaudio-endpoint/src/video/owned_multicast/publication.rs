use std::net::IpAddr;
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use mdns_sd::{IfKind, ServiceDaemon, ServiceInfo, TxtProperty};
use netaudio_core::video::discovery::{self, ChannelPublication, FlowPublication};

use super::{Configuration, Source};

pub(super) struct Publication {
    daemon: ServiceDaemon,
    names: Vec<String>,
    stopped: bool,
}

impl Publication {
    pub(super) async fn start(configuration: &Configuration, source: &Source) -> Result<Self> {
        let channel = ChannelPublication {
            source_service_channel: source.service_channel,
            channel_count: source.channel_count,
            codec_identifier: source.channel_codec_identifier,
            transport_peer_metadata: None,
            multicast_flow_label: Some(source.flow_name.as_bytes()),
        }
        .encode()?;
        let flow = FlowPublication {
            identifier: source.local_flow,
            latency_nanoseconds: source.latency_nanoseconds,
            destination: source.configuration.destination,
        }
        .encode()?;
        let daemon = ServiceDaemon::new()?;
        let mut publication = Self {
            daemon,
            names: Vec::new(),
            stopped: false,
        };
        let setup = (|| -> Result<()> {
            publication.daemon.disable_interface(IfKind::All)?;
            publication.daemon.enable_interface(IfKind::Addr(IpAddr::V4(
                source.configuration.interface_address,
            )))?;
            publication.daemon.set_ip_check_interval(0)?;
            let host = format!("{}.local.", configuration.device_name);
            for (service_type, label, port, text) in [
                (discovery::CHANNEL_SERVICE, &source.channel_name, 4321, channel),
                (discovery::FLOW_SERVICE, &source.flow_name, discovery::FLOW_SERVICE_PORT, flow),
            ] {
                let service = ServiceInfo::new(
                    service_type,
                    &format!("{label}@{}", configuration.device_name),
                    &host,
                    IpAddr::V4(source.configuration.interface_address),
                    port,
                    properties(&text)?,
                )?;
                let name = service.get_fullname().to_owned();
                publication.daemon.register(service)?;
                publication.names.push(name);
            }
            Ok(())
        })();
        match setup {
            Ok(()) => Ok(publication),
            Err(error) => match publication.stop().await {
                Ok(()) => Err(error),
                Err(retirement) => Err(error
                    .context(format!("publication setup retirement also failed: {retirement:#}"))),
            },
        }
    }

    pub(super) async fn stop(mut self) -> Result<()> {
        let retirement = async {
            let mut failures = Vec::new();
            for name in &self.names {
                let result = async {
                    let status = self.daemon.unregister(name)?.recv_async().await?;
                    ensure!(
                        matches!(status, mdns_sd::UnregisterStatus::OK),
                        "video publication was not registered during withdrawal"
                    );
                    Ok::<_, anyhow::Error>(())
                }
                .await;
                if let Err(error) = result {
                    failures.push(format!("withdraw {name}: {error:#}"));
                }
            }
            self.daemon.shutdown()?.recv_async().await?;
            self.stopped = true;
            ensure!(failures.is_empty(), "{}", failures.join("; "));
            Ok::<_, anyhow::Error>(())
        };
        let result = tokio::time::timeout(Duration::from_secs(5), retirement)
            .await
            .context("video discovery withdrawal timed out")?;
        if result.is_ok() {
            self.names.clear();
        }
        result
    }
}

impl Drop for Publication {
    fn drop(&mut self) {
        if !self.stopped {
            for name in &self.names {
                let _ = self.daemon.unregister(name);
            }
            let _ = self.daemon.shutdown();
        }
    }
}

fn properties(bytes: &[u8]) -> Result<Vec<TxtProperty>> {
    let mut properties = Vec::new();
    let mut remaining = bytes;
    while let Some((&length, rest)) = remaining.split_first() {
        let value = std::str::from_utf8(
            rest.get(..usize::from(length)).context("video TXT extent is invalid")?,
        )?;
        properties.push(match value.split_once('=') {
            Some((key, value)) => TxtProperty::from((key, value)),
            None => TxtProperty::from(value),
        });
        remaining = &rest[usize::from(length)..];
    }
    Ok(properties)
}
