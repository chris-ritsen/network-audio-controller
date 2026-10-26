use super::Provider;
use anyhow::{Context, Result, bail, ensure};
use netaudio_core::host_audio::snapshot::{Direction, JackPort, JackProperty, JackSnapshot};
use std::{collections::BTreeMap, sync::atomic::Ordering};

const PRETTY_NAME: &str = "http://jackaudio.org/metadata/pretty-name";

pub(super) fn read(provider: &Provider) -> Result<(u64, JackSnapshot)> {
    for _ in 0..3 {
        let revision = provider.state.wake.revision();
        let snapshot = read_ports(provider);
        if revision == provider.state.wake.revision() {
            return snapshot.map(|snapshot| (revision, snapshot));
        }
        ensure!(
            !provider.state.stopped(),
            "the JACK server stopped during observation"
        );
    }
    bail!("the JACK graph changed during each bounded snapshot attempt")
}

fn read_ports(provider: &Provider) -> Result<JackSnapshot> {
    let client = provider.client.as_client();
    let metadata = client.property_get_all();
    let mut ports = BTreeMap::new();
    for name in client.ports(None, None, jack::PortFlags::empty()) {
        if name.starts_with("netaudio-meter") {
            continue;
        }
        let port = client
            .port_by_name(&name)
            .context("a JACK port disappeared while reading its graph")?;
        let flags = port.flags();
        let direction = match (
            flags.contains(jack::PortFlags::IS_OUTPUT),
            flags.contains(jack::PortFlags::IS_INPUT),
        ) {
            (true, false) => Direction::Output,
            (false, true) => Direction::Input,
            _ => bail!("JACK port {name} has an invalid direction"),
        };
        let identity = unsafe { jack_sys::jack_port_uuid(port.raw()) };
        let native_type = port.port_type()?;
        let port_type = match native_type.as_str() {
            "32 bit float mono audio" => "audio",
            "8 bit raw midi" => "midi",
            _ => "other",
        }
        .into();
        let properties: BTreeMap<_, _> = metadata
            .get(&identity)
            .into_iter()
            .flat_map(|map| map.iter())
            .map(|(key, property)| {
                (
                    key.clone(),
                    JackProperty {
                        value: property.value().to_owned(),
                        property_type: property.typ().map(str::to_owned),
                    },
                )
            })
            .collect();
        let mut connections = port.get_connections();
        connections.retain(|name| !name.starts_with("netaudio-meter"));
        connections.sort();
        ports.insert(
            name.clone(),
            JackPort {
                identity,
                client: name
                    .split_once(':')
                    .map(|(client, _)| client)
                    .unwrap_or(&name)
                    .into(),
                short_name: port.short_name()?,
                direction,
                port_type,
                native_type,
                physical: flags.contains(jack::PortFlags::IS_PHYSICAL),
                terminal: flags.contains(jack::PortFlags::IS_TERMINAL),
                aliases: port.aliases()?,
                connections,
                pretty_name: properties
                    .get(PRETTY_NAME)
                    .map(|property| property.value.clone()),
                properties,
            },
        );
    }
    for (name, port) in &ports {
        for destination in &port.connections {
            let other = ports
                .get(destination)
                .context("a connected JACK port disappeared during observation")?;
            ensure!(
                other.connections.contains(name),
                "JACK connection readback is inconsistent"
            );
        }
    }
    let cpu_load = f64::from(client.cpu_load());
    let last_xrun = provider.state.last_xrun.load(Ordering::Acquire);
    Ok(JackSnapshot {
        available: true,
        reason: None,
        seen_running: true,
        server: provider.server.clone(),
        client_name: Some(client.name().into()),
        connected_at: Some(provider.connected_at.clone()),
        sample_rate: Some(client.sample_rate()),
        buffer_size: Some(client.buffer_size()),
        realtime: Some(unsafe { jack_sys::jack_is_realtime(client.raw()) } != 0),
        cpu_load: cpu_load
            .is_finite()
            .then_some((cpu_load * 10.0).round() / 10.0),
        xruns: provider.state.xruns.load(Ordering::Acquire),
        last_xrun: (last_xrun != 0)
            .then(|| netaudio_host_worker::timestamp_milliseconds(last_xrun))
            .flatten(),
        xruns_since: Some(provider.connected_at.clone()),
        ports,
        changes: Vec::new(),
    })
}
