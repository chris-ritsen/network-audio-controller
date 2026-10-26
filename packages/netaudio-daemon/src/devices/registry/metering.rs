use super::{DeviceRegistry, ReceivedPacket};
use crate::devices::transport::{CommandIntent, DeviceAddress, DeviceTransport};
use anyhow::{Context, Result, ensure};
use netaudio_core::commands::{OPCODE_CMC_METERING, PROTOCOL_CMC};
use netaudio_core::protocol::{RESULT_CODE_SUCCESS, response_envelope};
use netaudio_core::responses::parse_metering_frame;
use serde_json::{Value, json};
use std::net::Ipv4Addr;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MeteringTarget {
    pub server_name: String,
    name: String,
    identity: Option<String>,
    address: DeviceAddress,
    local_address: Option<Ipv4Addr>,
}

pub struct MeteringLease {
    pub target: MeteringTarget,
    pub port: u16,
    pub started_at: tokio::time::Instant,
    transport: DeviceTransport,
    start: Value,
    stop: Value,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl MeteringLease {
    pub async fn open(
        target: MeteringTarget,
        port: u16,
        permit: tokio::sync::OwnedSemaphorePermit,
        observer: Option<crate::capture::Observer>,
    ) -> Result<Self> {
        let transport = DeviceTransport::connect(target.address, target.local_address)
            .await?
            .with_observer(observer);
        let std::net::IpAddr::V4(address) = transport.local_address()?.ip() else {
            anyhow::bail!("metering requires an IPv4 source address");
        };
        let identity = netaudio_core::netif::host_mac_for_ipv4(address)
            .context("metering source interface has no hardware identity")?;
        let identity = hex::encode(identity);
        let start = json!({
            "command": "metering_start",
            "device_name": target.name,
            "ipv4": address.to_string(),
            "mac": identity,
            "port": port,
        });
        let stop = json!({
            "command": "metering_stop",
            "device_name": target.name,
            "mac": identity,
            "port": port,
        });
        let mut lease = Self {
            target,
            port,
            started_at: tokio::time::Instant::now(),
            transport,
            start,
            stop,
            _permit: permit,
        };
        lease.renew().await?;
        Ok(lease)
    }

    pub async fn renew(&mut self) -> Result<()> {
        let reply = self
            .transport
            .request_command(&self.start, CommandIntent::Write)
            .await?;
        metering_acknowledgement(&reply)
    }

    pub async fn close(mut self) -> Result<()> {
        let reply = self
            .transport
            .request_command(&self.stop, CommandIntent::Write)
            .await?;
        metering_acknowledgement(&reply)
    }
}

impl DeviceRegistry {
    pub fn metering_target(&self, reference: &str) -> Result<MeteringTarget> {
        let device = self.controlled_device(reference)?;
        let state = device
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;
        ensure!(state.record.online, "device is offline");
        ensure!(
            state
                .record
                .properties
                .get("requires_managed_control")
                .and_then(Value::as_bool)
                != Some(true),
            "Detailed metering through DDM is not implemented yet. DDM login enables inventory and supported device controls, not this meter stream."
        );
        ensure!(
            state
                .record
                .properties
                .get("detailed_metering_supported")
                .and_then(Value::as_bool)
                != Some(false),
            "device does not support detailed metering"
        );
        Ok(MeteringTarget {
            server_name: state.record.key.clone(),
            name: state.record.name.clone(),
            identity: state
                .record
                .properties
                .get("mac_address")
                .and_then(Value::as_str)
                .map(str::to_owned),
            address: state
                .record
                .control_address()
                .context("device has no monitoring service")?,
            local_address: self.local_address,
        })
    }

    pub(super) fn observe_metering(&self, packet: &ReceivedPacket) -> Result<Option<String>> {
        let frame =
            parse_metering_frame(&packet.payload).context("invalid detailed metering frame")?;
        let Some((device, generation)) =
            self.observation_device(packet, Some(&frame.source_eui64))?
        else {
            return Ok(None);
        };
        let mut state = device
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;
        if state.record.retired
            || state.identity_generation != generation
            || packet.received_monotonic < state.associated_at
        {
            return Ok(None);
        }

        let identity = frame.source_eui64.clone();

        if !self
            .metering
            .observe_detailed(&state.record.key, packet, frame)?
        {
            return Ok(None);
        }

        let identity_changed = state
            .diagnostics
            .observe_presence(&identity, packet.received_monotonic);
        state.record.properties.insert(
            "last_seen".to_owned(),
            json!(packet.received_at.timestamp_micros() as f64 / 1_000_000.0),
        );
        let previously_online = state.record.online;
        state.record.online = true;
        let previous = state
            .record
            .properties
            .insert("detailed_metering_supported".to_owned(), json!(true));

        if identity_changed || !previously_online || previous != Some(json!(true)) {
            self.publish_diagnostics(&mut state, true)?;
        }

        Ok(Some(state.record.key.clone()))
    }
}

fn metering_acknowledgement(reply: &[u8]) -> Result<()> {
    let envelope = response_envelope(reply).context("malformed metering acknowledgement")?;
    ensure!(
        envelope.protocol_id == PROTOCOL_CMC && envelope.opcode == OPCODE_CMC_METERING,
        "unexpected metering acknowledgement"
    );
    ensure!(
        envelope.result_code == RESULT_CODE_SUCCESS,
        "metering command rejected with result {}",
        envelope.result_code
    );
    Ok(())
}
