use libpulse_binding::{
    channelmap::{Map, Position},
    context::introspect::{ServerInfo, SinkInfo, SinkInputInfo, SourceInfo, SourceOutputInfo},
    proplist::Proplist,
    sample::Spec,
    volume::ChannelVolumes,
};
use netaudio_core::host_audio::snapshot::{PulseEntry, PulseServer};

pub(super) fn server(info: &ServerInfo<'_>) -> PulseServer {
    PulseServer {
        name: info.server_name.as_deref().map(str::to_owned),
        version: info.server_version.as_deref().map(str::to_owned),
        host: info.host_name.as_deref().map(str::to_owned),
        default_sink: info.default_sink_name.as_deref().map(str::to_owned),
        default_source: info.default_source_name.as_deref().map(str::to_owned),
    }
}

fn base(index: u32, properties: &Proplist, specification: Spec, channels: &Map) -> PulseEntry {
    PulseEntry {
        index,
        channels: channels
            .get()
            .iter()
            .map(|position| {
                Position::to_string(*position)
                    .map(|value| value.into_owned())
                    .unwrap_or_else(|| format!("{position:?}"))
            })
            .collect(),
        format: Some(format!(
            "{} ch {} Hz",
            specification.channels, specification.rate
        )),
        jack_client: properties.get_str("jack.client_name"),
        device: properties
            .get_str("device.description")
            .or_else(|| properties.get_str("alsa.card_name")),
        application: properties.get_str("application.name"),
        binary: properties.get_str("application.process.binary"),
        process_id: properties.get_str("application.process.id"),
        media: properties.get_str("media.name"),
        role: properties.get_str("media.role"),
        properties: properties
            .iter()
            .filter_map(|key| properties.get(&key).map(|value| (key, value.to_vec())))
            .collect(),
        ..Default::default()
    }
}

fn volume(entry: &mut PulseEntry, volumes: &ChannelVolumes, writable: bool) {
    entry.volume_raw = volumes.get().iter().map(|value| value.0).collect();
    entry.volume_percent = entry
        .volume_raw
        .iter()
        .map(|value| (*value as f64 / 65536.0 * 100.0).round() as u32)
        .collect();
    entry.volume_db = entry
        .volume_raw
        .iter()
        .map(|value| {
            (*value > 0).then(|| (60.0 * (*value as f64 / 65536.0).log10() * 10.0).round() / 10.0)
        })
        .collect();
    entry.volume_writable = writable;
}

pub(super) fn sink(info: &SinkInfo<'_>) -> PulseEntry {
    let mut entry = base(
        info.index,
        &info.proplist,
        info.sample_spec,
        &info.channel_map,
    );
    entry.name = info.name.as_deref().map(str::to_owned);
    entry.description = info.description.as_deref().map(str::to_owned);
    entry.driver = info.driver.as_deref().map(str::to_owned);
    entry.state = Some(format!("{:?}", info.state).to_lowercase());
    entry.mute = Some(info.mute);
    entry.monitor_source = info.monitor_source_name.as_deref().map(str::to_owned);
    volume(&mut entry, &info.volume, true);
    entry
}

pub(super) fn source(info: &SourceInfo<'_>) -> PulseEntry {
    let mut entry = base(
        info.index,
        &info.proplist,
        info.sample_spec,
        &info.channel_map,
    );
    entry.name = info.name.as_deref().map(str::to_owned);
    entry.description = info.description.as_deref().map(str::to_owned);
    entry.driver = info.driver.as_deref().map(str::to_owned);
    entry.state = Some(format!("{:?}", info.state).to_lowercase());
    entry.mute = Some(info.mute);
    entry.monitor_of = info.monitor_of_sink_name.as_deref().map(str::to_owned);
    volume(&mut entry, &info.volume, true);
    entry
}

pub(super) fn sink_input(info: &SinkInputInfo<'_>) -> PulseEntry {
    let mut entry = base(
        info.index,
        &info.proplist,
        info.sample_spec,
        &info.channel_map,
    );
    entry.name = info.name.as_deref().map(str::to_owned);
    entry.driver = info.driver.as_deref().map(str::to_owned);
    entry.media = entry.media.or_else(|| entry.name.clone());
    entry.mute = Some(info.mute);
    entry.destination_index = Some(info.sink);
    entry.corked = Some(info.corked);
    if info.has_volume {
        volume(&mut entry, &info.volume, info.volume_writable);
    }
    entry
}

pub(super) fn source_output(info: &SourceOutputInfo<'_>) -> PulseEntry {
    let mut entry = base(
        info.index,
        &info.proplist,
        info.sample_spec,
        &info.channel_map,
    );
    entry.name = info.name.as_deref().map(str::to_owned);
    entry.driver = info.driver.as_deref().map(str::to_owned);
    entry.media = entry.media.or_else(|| entry.name.clone());
    entry.mute = Some(info.mute);
    entry.destination_index = Some(info.source);
    entry.corked = Some(info.corked);
    if info.has_volume {
        volume(&mut entry, &info.volume, info.volume_writable);
    }
    entry
}
