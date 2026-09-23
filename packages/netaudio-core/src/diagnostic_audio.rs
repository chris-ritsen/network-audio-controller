use std::collections::{BTreeMap, HashSet};
use std::num::NonZeroU32;
use std::str::FromStr;

use serde::Serialize;

use crate::sample_rate_topology::ChannelCapacity;

#[derive(Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct DiagnosticAudioCapabilities {
    license_signature_length_bytes: Option<u32>,
    licensed_receive_channel_count: Option<u16>,
    licensed_transmit_channel_count: Option<u16>,
    licensed_redundancy_enabled: Option<bool>,
    default_sample_rate_hertz: Option<NonZeroU32>,
    current_sample_rate_hertz: Option<NonZeroU32>,
    channel_capacities: Vec<ChannelCapacity>,
}

fn number<T: FromStr>(value: &str) -> Option<T> {
    (!value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()))
        .then(|| value.parse().ok())
        .flatten()
}

fn unique<T>(values: HashSet<T>) -> Option<T> {
    if values.len() == 1 {
        values.into_iter().next()
    } else {
        None
    }
}

/// Interpret observed audio-log records. Missing or contradictory facts stay unknown.
pub fn parse(payload: &[u8]) -> Option<DiagnosticAudioCapabilities> {
    let mut signatures = HashSet::new();
    let mut licenses = HashSet::new();
    let mut redundancy = HashSet::new();
    let mut defaults = HashSet::new();
    let mut current = HashSet::new();
    let mut capacities: BTreeMap<NonZeroU32, HashSet<(u16, u16)>> = BTreeMap::new();

    for line in String::from_utf8_lossy(payload).lines() {
        let fields: Vec<_> = line.split_whitespace().collect();

        match fields.as_slice() {
            ["rsa.len", "=", size] => {
                if let Some(size) = number::<u32>(size) {
                    signatures.insert(size);
                }
            }
            ["license", "rx", "chans", receive, "tx", "chans", transmit] => {
                if let (Some(receive), Some(transmit)) =
                    (number::<u16>(receive), number::<u16>(transmit))
                {
                    licenses.insert((receive, transmit));
                }
            }
            ["is", "redundant"] => {
                redundancy.insert(true);
            }
            ["is", "non-redundant"] => {
                redundancy.insert(false);
            }
            ["audio", "config", "dynamic", dynamic, "audio_frame", frame, "audio_align", align, "audio", "start", start, "default", "srate", rate, "default", "enc", encoding, "num", "srates", rates, "num", "enc", encodings]
                if [dynamic, frame, align, start, encoding, rates, encodings]
                    .iter()
                    .all(|value| number::<u32>(value).is_some()) =>
            {
                if let Some(rate) = number::<NonZeroU32>(rate) {
                    defaults.insert(rate);
                }
            }
            ["MCLK", "=", clock, "sample_rate", "=", rate, "TDM", "=", channels]
                if number::<u32>(clock).is_some() && number::<u32>(channels).is_some() =>
            {
                if let Some(rate) = number::<NonZeroU32>(rate) {
                    current.insert(rate);
                }
            }
            ["srate", rate, "rxchan", receive, "txchan", transmit, "mclk", clock, "tdm_chan", channels]
                if number::<u32>(clock).is_some() && number::<u32>(channels).is_some() =>
            {
                if let (Some(rate), Some(receive), Some(transmit)) = (
                    number::<NonZeroU32>(rate),
                    number::<u16>(receive),
                    number::<u16>(transmit),
                ) {
                    capacities
                        .entry(rate)
                        .or_default()
                        .insert((receive, transmit));
                }
            }
            _ => {}
        }
    }

    let license = unique(licenses);
    let result = DiagnosticAudioCapabilities {
        license_signature_length_bytes: unique(signatures),
        licensed_receive_channel_count: license.map(|(receive, _)| receive),
        licensed_transmit_channel_count: license.map(|(_, transmit)| transmit),
        licensed_redundancy_enabled: unique(redundancy),
        default_sample_rate_hertz: unique(defaults),
        current_sample_rate_hertz: unique(current),
        channel_capacities: capacities
            .into_iter()
            .filter_map(|(sample_rate_hertz, counts)| {
                unique(counts).map(|(receive_channel_count, transmit_channel_count)| {
                    ChannelCapacity {
                        sample_rate_hertz,
                        receive_channel_count,
                        transmit_channel_count,
                    }
                })
            })
            .collect(),
    };

    (result.license_signature_length_bytes.is_some()
        || result.licensed_receive_channel_count.is_some()
        || result.licensed_redundancy_enabled.is_some()
        || result.default_sample_rate_hertz.is_some()
        || result.current_sample_rate_hertz.is_some()
        || !result.channel_capacities.is_empty())
    .then_some(result)
}
