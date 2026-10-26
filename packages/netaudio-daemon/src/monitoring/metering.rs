use super::multicast::ReceivedPacket;
use crate::state::ReadModel;
use anyhow::{Context, Result, ensure};
use netaudio_core::heartbeat::parse_heartbeat_records;
use netaudio_core::metering::{MeteringScales, metering_scale};
use netaudio_core::responses::MeteringFrame;
use netaudio_core::signal_presence::{SignalPresenceRecord, parse_signal_presence_packet};
use serde_json::{Map, Value, json};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tokio::sync::{Notify, watch};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

pub const FRESHNESS: Duration = Duration::from_secs(2);
const BROADCAST_INTERVAL: Duration = Duration::from_millis(50);
const SIGNAL_PRESENCE_RECORD: u16 = 0x8002;
const PASSIVE_SOURCE: &str = "signal_presence";
const DETAILED_SOURCE: &str = "detailed";
static SCALES: OnceLock<MeteringScales> = OnceLock::new();

pub struct Sample {
    observed: Instant,
    owner: Option<u64>,
    raw: Arc<[u8]>,
    value: Arc<Value>,
}

struct Receipt<'a> {
    payload: &'a Arc<[u8]>,
    source: std::net::SocketAddr,
    wall: chrono::DateTime<chrono::Utc>,
    monotonic: Instant,
}

impl<'a> From<&'a ReceivedPacket> for Receipt<'a> {
    fn from(packet: &'a ReceivedPacket) -> Self {
        Self {
            payload: &packet.payload,
            source: packet.source,
            wall: packet.received_at,
            monotonic: packet.received_monotonic,
        }
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Source {
    Detailed,
    Passive,
    Managed,
}

impl Sample {
    fn new(receipt: Receipt<'_>, mut fields: Map<String, Value>, source: Source) -> Result<Self> {
        let passive = source != Source::Detailed;
        let scales = SCALES.get_or_init(metering_scale);
        let scale = if passive {
            &scales.signal_presence
        } else {
            &scales.detailed
        };

        for direction in ["tx", "rx"] {
            let channels = fields
                .get(direction)
                .and_then(Value::as_object)
                .context("metering sample has no channel map")?;
            let mut normalized = Map::new();
            let mut indications = Map::new();

            for (channel, value) in channels {
                let raw = value
                    .as_u64()
                    .and_then(|value| u8::try_from(value).ok())
                    .context("metering level is not one byte")?;
                let reading = &scale[usize::from(raw)];
                normalized.insert(channel.clone(), serde_json::to_value(reading)?);

                if passive {
                    indications.insert(channel.clone(), json!(reading.state));
                }
            }

            fields.insert(format!("{direction}_normalized"), Value::Object(normalized));

            if passive {
                fields.insert(
                    format!("{direction}_signal_presence"),
                    Value::Object(indications),
                );
            }
        }

        fields.extend([
            (
                "observed_at".to_owned(),
                json!(
                    receipt
                        .wall
                        .to_rfc3339_opts(chrono::SecondsFormat::Micros, true)
                ),
            ),
            (
                "wall_time".to_owned(),
                json!(receipt.wall.timestamp_micros() as f64 / 1_000_000.0),
            ),
            ("source_ip".to_owned(), json!(receipt.source.ip())),
            ("source_port".to_owned(), json!(receipt.source.port())),
            (
                "metering_source".to_owned(),
                json!(if passive {
                    PASSIVE_SOURCE
                } else {
                    DETAILED_SOURCE
                }),
            ),
        ]);
        if source == Source::Managed {
            fields.insert("transport".to_owned(), json!("ddm"));
        }
        Ok(Self {
            observed: receipt.monotonic,
            owner: None,
            raw: Arc::clone(receipt.payload),
            value: Arc::new(Value::Object(fields)),
        })
    }
}

#[derive(Default)]
struct DeviceMeters {
    detailed: Option<Sample>,
    passive: Option<Sample>,
    managed: Option<Sample>,
    published: Option<Instant>,
    broadcast_at: Option<Instant>,
    observed: watch::Sender<Option<Instant>>,
}

impl DeviceMeters {
    fn selected(&self, now: Instant) -> Option<&Sample> {
        self.detailed
            .as_ref()
            .filter(|sample| now < sample.observed + FRESHNESS)
            .or_else(|| {
                [&self.passive, &self.managed]
                    .into_iter()
                    .filter_map(Option::as_ref)
                    .filter(|sample| now < sample.observed + FRESHNESS)
                    .max_by_key(|sample| sample.observed)
            })
    }
}

pub struct Metering {
    pub history: Arc<super::level_history::LevelHistory>,
    devices: Mutex<BTreeMap<String, DeviceMeters>>,
    changed: Notify,
    model: Arc<ReadModel>,
}

impl Metering {
    pub fn new(model: Arc<ReadModel>) -> Arc<Self> {
        Arc::new(Self {
            history: Arc::new(super::level_history::LevelHistory::default()),
            devices: Mutex::new(BTreeMap::new()),
            changed: Notify::new(),
            model,
        })
    }

    pub fn observe_passive(&self, name: &str, packet: &ReceivedPacket) -> Result<bool> {
        let Some(records) = parse_heartbeat_records(&packet.payload) else {
            return Ok(false);
        };
        let expected = records
            .iter()
            .filter(|record| record.record_type == SIGNAL_PRESENCE_RECORD)
            .count();

        if expected == 0 {
            return Ok(false);
        }

        let records = parse_signal_presence_packet(&packet.payload)
            .context("invalid signal presence packet")?;
        ensure!(
            records.len() == expected,
            "signal presence packet contains a malformed record"
        );
        self.observe(
            name,
            Sample::new(packet.into(), signal_fields(&records)?, Source::Passive)?,
            Source::Passive,
        )
    }

    pub fn observe_managed(
        &self,
        name: &str,
        signal: &crate::managed::controller::Signal,
        owner: u64,
    ) -> Result<bool> {
        let receipt = Receipt {
            payload: &signal.frame,
            source: signal.source,
            wall: signal.received_at,
            monotonic: signal.received_monotonic,
        };
        let mut sample = Sample::new(
            receipt,
            signal_fields(&signal.publication.records)?,
            Source::Managed,
        )?;
        sample.owner = Some(owner);
        self.observe(name, sample, Source::Managed)
    }

    pub fn observe_detailed(
        &self,
        name: &str,
        packet: &ReceivedPacket,
        frame: MeteringFrame,
    ) -> Result<bool> {
        let mut fields = Map::new();

        for (direction, levels) in [("tx", &frame.tx_levels), ("rx", &frame.rx_levels)] {
            let mut channels = Map::new();
            merge_channels(&mut channels, 0, levels)?;
            fields.insert(direction.to_owned(), Value::Object(channels));
        }

        fields.extend([
            ("sequence".to_owned(), json!(frame.sequence)),
            ("tx_count".to_owned(), json!(frame.tx_count)),
            ("rx_count".to_owned(), json!(frame.rx_count)),
            ("frame".to_owned(), serde_json::to_value(frame)?),
        ]);
        self.observe(
            name,
            Sample::new(packet.into(), fields, Source::Detailed)?,
            Source::Detailed,
        )
    }

    fn observe(&self, name: &str, sample: Sample, source: Source) -> Result<bool> {
        let mut devices = self
            .devices
            .lock()
            .map_err(|_| anyhow::anyhow!("metering state lock poisoned"))?;
        let device = devices.entry(name.to_owned()).or_default();
        let previous = match source {
            Source::Passive => &mut device.passive,
            Source::Detailed => &mut device.detailed,
            Source::Managed => &mut device.managed,
        };

        if source != Source::Managed
            && previous
                .as_ref()
                .is_some_and(|previous| previous.raw == sample.raw)
        {
            return Ok(false);
        }

        *previous = Some(sample);
        if let Some(sample) = device.selected(Instant::now()) {
            self.history.record_meter(name, &sample.value)?;
        }

        device.observed.send_replace(Some(Instant::now()));
        self.changed.notify_one();
        Ok(true)
    }

    pub fn cached(&self) -> Result<Value> {
        let devices = self
            .devices
            .lock()
            .map_err(|_| anyhow::anyhow!("metering state lock poisoned"))?;
        let now = Instant::now();
        let records: Map<_, _> = devices
            .iter()
            .filter_map(|(name, device)| {
                device
                    .selected(now)
                    .map(|sample| (name.clone(), sample.value.as_ref().clone()))
            })
            .collect();
        Ok(Value::Object(records))
    }

    pub fn sample(&self, name: &str) -> Result<Option<Arc<Value>>> {
        let devices = self
            .devices
            .lock()
            .map_err(|_| anyhow::anyhow!("metering state lock poisoned"))?;
        Ok(devices
            .get(name)
            .and_then(|device| device.selected(Instant::now()))
            .map(|sample| Arc::clone(&sample.value)))
    }

    pub fn subscribe(&self, name: &str) -> Result<watch::Receiver<Option<Instant>>> {
        let mut devices = self
            .devices
            .lock()
            .map_err(|_| anyhow::anyhow!("metering state lock poisoned"))?;
        Ok(devices
            .entry(name.to_owned())
            .or_default()
            .observed
            .subscribe())
    }

    pub fn detailed_observed(&self, name: &str) -> Result<Option<Instant>> {
        let devices = self
            .devices
            .lock()
            .map_err(|_| anyhow::anyhow!("metering state lock poisoned"))?;
        Ok(devices
            .get(name)
            .and_then(|device| device.detailed.as_ref())
            .map(|sample| sample.observed))
    }

    pub fn invalidate(&self, name: &str) -> Result<()> {
        self.invalidate_sources(name, true, true, None)
    }

    pub fn invalidate_direct(&self, name: &str) -> Result<()> {
        self.invalidate_sources(name, true, false, None)
    }

    pub fn invalidate_managed(&self, name: &str, owner: u64) -> Result<()> {
        self.invalidate_sources(name, false, true, Some(owner))
    }

    fn invalidate_sources(
        &self,
        name: &str,
        direct: bool,
        managed: bool,
        owner: Option<u64>,
    ) -> Result<()> {
        let mut devices = self
            .devices
            .lock()
            .map_err(|_| anyhow::anyhow!("metering state lock poisoned"))?;

        if let Some(device) = devices.get_mut(name) {
            let mut changed = false;
            if direct {
                changed |= device.detailed.is_some() || device.passive.is_some();
                device.detailed = None;
                device.passive = None;
            }
            if managed
                && owner.is_none_or(|owner| {
                    device
                        .managed
                        .as_ref()
                        .is_some_and(|sample| sample.owner == Some(owner))
                })
            {
                changed |= device.managed.is_some();
                device.managed = None;
            }
            if !changed {
                return Ok(());
            }
            let sample = device.selected(Instant::now());
            let selected = sample.map(|sample| sample.observed);
            self.model
                .metering(name, sample.map(|sample| Arc::clone(&sample.value)))?;
            device.published = selected;
            device.observed.send_replace(selected);
        }

        self.changed.notify_one();
        Ok(())
    }

    pub async fn run(self: Arc<Self>, cancellation: CancellationToken) -> Result<()> {
        loop {
            let changed = self.changed.notified();
            let deadline = self.publish_due(Instant::now())?;
            let expires = async {
                match deadline {
                    Some(deadline) => tokio::time::sleep_until(deadline).await,
                    None => std::future::pending().await,
                }
            };

            tokio::select! {
                _ = cancellation.cancelled() => return Ok(()),
                _ = changed => {}
                _ = expires => {}
            }
        }
    }

    fn publish_due(&self, now: Instant) -> Result<Option<Instant>> {
        let mut devices = self
            .devices
            .lock()
            .map_err(|_| anyhow::anyhow!("metering state lock poisoned"))?;
        let mut deadlines = Vec::new();

        for (name, device) in devices.iter_mut() {
            let sample = device.selected(now);
            let selected = sample.map(|sample| sample.observed);

            if let Some(sample) = sample {
                deadlines.push(sample.observed + FRESHNESS);
            }

            if selected == device.published {
                continue;
            }

            let broadcast = device
                .broadcast_at
                .map(|previous| previous + BROADCAST_INTERVAL);

            if let Some(broadcast) = broadcast
                && broadcast > now
                && selected.is_some()
            {
                deadlines.push(broadcast);
                continue;
            }

            self.model
                .metering(name, sample.map(|sample| Arc::clone(&sample.value)))?;
            device.published = selected;
            device.broadcast_at = Some(now);
        }

        Ok(deadlines.into_iter().min())
    }
}

fn signal_fields(records: &[SignalPresenceRecord]) -> Result<Map<String, Value>> {
    let mut transmitters = Map::new();
    let mut receivers = Map::new();
    for record in records {
        merge_channels(
            &mut transmitters,
            record.tx_first_channel_index,
            &record.tx_levels,
        )?;
        merge_channels(
            &mut receivers,
            record.rx_first_channel_index,
            &record.rx_levels,
        )?;
    }
    let mut fields = Map::from_iter([
        ("tx".to_owned(), Value::Object(transmitters)),
        ("rx".to_owned(), Value::Object(receivers)),
    ]);
    if let [record] = records {
        let mut raw = serde_json::to_value(record)?
            .as_object()
            .context("signal presence record is not an object")?
            .clone();
        for direction in ["tx", "rx"] {
            let levels = raw
                .remove(&format!("{direction}_levels"))
                .context("signal presence record has no level vector")?;
            raw.insert(format!("{direction}_raw"), levels);
        }
        fields.extend(raw);
    } else {
        fields.insert("records".to_owned(), serde_json::to_value(records)?);
    }
    Ok(fields)
}

fn merge_channels(
    channels: &mut Map<String, Value>,
    first_index: u16,
    levels: &[u8],
) -> Result<()> {
    for (offset, level) in levels.iter().enumerate() {
        let number = usize::from(first_index)
            .checked_add(offset)
            .and_then(|index| index.checked_add(1))
            .context("meter channel index overflow")?;
        let number = number.to_string();
        let value = json!(level);
        ensure!(
            channels
                .get(&number)
                .is_none_or(|previous| *previous == value),
            "conflicting signal presence blocks for channel {number}"
        );
        channels.insert(number, value);
    }

    Ok(())
}
