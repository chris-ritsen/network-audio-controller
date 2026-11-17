use std::net::{IpAddr, Ipv4Addr};

use anyhow::{Result, bail};
use mdns_sd::{DaemonEvent, IfKind, ServiceDaemon};
use netaudio_core::native_audio::endpoint_control::Endpoint;
use netaudio_core::native_audio::endpoint_control::monitoring::Identity;

use crate::advertisement::Advertisement;

const MANUFACTURER: &str = "NetAudio";
const MODEL: &str = "Virtual";

pub struct Publication {
    pub daemon: ServiceDaemon,
    names: Vec<String>,
}

impl Publication {
    pub async fn start(
        endpoint: &Endpoint,
        address: Ipv4Addr,
        identity: Identity,
        routing_port: u16,
        monitoring_port: u16,
        source_port: u16,
    ) -> Result<Self> {
        let daemon = super::platform::spawn_background(|| Ok(ServiceDaemon::new()?)).await??;
        daemon.disable_interface(IfKind::All)?;
        daemon.enable_interface(IfKind::Addr(IpAddr::V4(address)))?;
        daemon.set_ip_check_interval(0)?;
        let services = Advertisement {
            address,
            endpoint,
            identity,
            manufacturer: MANUFACTURER,
            model: MODEL,
            monitoring_port,
            routing_port,
            source_port,
        }
        .services()?;
        let mut publication = Self {
            daemon,
            names: Vec::new(),
        };

        for service in services {
            publication.names.push(service.get_fullname().to_owned());
            publication.daemon.register(service)?;
        }

        Ok(publication)
    }

    pub fn check_event(&self, event: DaemonEvent) -> Result<()> {
        match event {
            DaemonEvent::Error(error) => bail!("discovery provider failed: {error}"),
            DaemonEvent::NameChange(change) => {
                bail!("discovery identity changed after a collision: {change:?}")
            }
            _ => Ok(()),
        }
    }

    pub async fn stop(mut self) -> Result<()> {
        for name in self.names.drain(..) {
            self.daemon.unregister(&name)?.recv_async().await?;
        }

        self.daemon.shutdown()?.recv_async().await?;
        Ok(())
    }
}

impl Drop for Publication {
    fn drop(&mut self) {
        let _ = self.daemon.shutdown();
    }
}
