use super::observation::{Reading, Readings};
use super::*;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum State {
    Signal,
    Quiet,
    Silent,
    Unknown,
}

impl State {
    pub fn has_signal(self) -> bool {
        matches!(self, Self::Signal | Self::Quiet)
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Level {
    pub state: State,
    pub text: String,
    pub peak_dbfs: Option<f64>,
    pub measured_at: Option<String>,
}

pub type Levels = BTreeMap<String, Level>;
pub type MeterTargets = BTreeMap<String, BTreeMap<String, Vec<String>>>;

impl Level {
    pub fn unknown(text: impl Into<String>) -> Self {
        Self {
            state: State::Unknown,
            text: text.into(),
            peak_dbfs: None,
            measured_at: None,
        }
    }

    fn reported(state: State, text: impl Into<String>) -> Self {
        Self {
            state,
            ..Self::unknown(text)
        }
    }

    pub fn jack(reading: Option<&Reading>) -> Self {
        match reading {
            None => Self::unknown("not measured"),
            Some(Reading::Unavailable { error }) => Self::unknown(error),
            Some(Reading::Disconnected { connected: false }) => {
                Self::reported(State::Silent, "nothing connected")
            }
            Some(Reading::Disconnected { connected: true }) => {
                Self::unknown("no level reading was returned")
            }
            Some(Reading::Level {
                peak_dbfs,
                rms_dbfs,
                measured_at,
                frames,
                sample_rate,
                duration_seconds,
                recorded_second,
            }) => {
                if frames.is_some_and(|frames| frames == 0)
                    || sample_rate.is_some_and(|rate| rate == 0)
                    || duration_seconds
                        .is_some_and(|duration| !duration.is_finite() || duration <= 0.0)
                    || (frames.is_none() && recorded_second.is_none())
                {
                    return Self::unknown("the meter returned no valid captured window");
                }
                if peak_dbfs.is_some_and(|value| !value.is_finite())
                    || rms_dbfs.is_some_and(|value| !value.is_finite())
                {
                    return Self::unknown("the level reading is not finite");
                }
                let mut text = peak_dbfs.map_or_else(
                    || "digital silence".to_owned(),
                    |peak| {
                        rms_dbfs.map_or_else(
                            || format!("peak {peak} dBFS"),
                            |rms| format!("peak {peak} dBFS, RMS {rms} dBFS"),
                        )
                    },
                );
                if measured_at.is_none() {
                    if let Some(second) = recorded_second {
                        text.push_str(&format!(", recorded at Unix second {second}"));
                    } else {
                        text.push_str(", capture timestamp not reported");
                    }
                }
                Self {
                    state: if peak_dbfs.is_some_and(|peak| peak >= -80.0) {
                        State::Signal
                    } else {
                        State::Silent
                    },
                    text,
                    peak_dbfs: *peak_dbfs,
                    measured_at: measured_at.clone(),
                }
            }
        }
    }

    pub fn dante(reading: Option<&Value>) -> Self {
        let Some(reading) = reading.filter(|reading| reading.is_object()) else {
            return Self::unknown("not metered");
        };
        let level = reading["dbfs"].as_f64();
        match reading["state"].as_str() {
            Some("signal_present" | "clipping") => {
                let text =
                    level.map_or_else(|| "signal".to_owned(), |level| format!("{level} dBFS"));
                Self::reported(
                    State::Signal,
                    format!(
                        "{text}{}",
                        if reading["state"] == "clipping" {
                            " (clipping)"
                        } else {
                            ""
                        }
                    ),
                )
            }
            Some("below_threshold") => Self::reported(
                State::Quiet,
                level.map_or_else(
                    || "very quiet".to_owned(),
                    |level| format!("very quiet ({level} dBFS)"),
                ),
            ),
            Some("mute_or_floor" | "muted") => Self::reported(State::Silent, "silent"),
            Some(state) => Self::unknown(state),
            None => Self::unknown("unknown"),
        }
    }
}

pub fn wireless_text(channel: &Value) -> Option<String> {
    if let Some(rms) = channel["audio_level_rms"].as_i64() {
        let peak = channel["audio_level_peak"]
            .as_i64()
            .map(|peak| format!(", peak {} dBFS", peak - 120))
            .unwrap_or_default();
        return Some(format!("RMS {} dBFS{peak}", rms - 120));
    }
    let left = channel["audio_in_level_l"].as_i64();
    let right = channel["audio_in_level_r"].as_i64();
    if left.is_some() || right.is_some() {
        let text = |value: Option<i64>| {
            value
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unknown".to_owned())
        };
        return Some(format!(
            "input meter left {}, right {} in the P10T's own units, not dBFS; larger is louder",
            text(left),
            text(right)
        ));
    }
    None
}

fn wireless(node: &WirelessNode) -> Level {
    if node.device["online"] == false {
        return Level::unknown("device offline");
    }
    if node.channel["active"] == false {
        return Level::reported(State::Silent, "no transmitter");
    }
    if node.channel["rf_mute"] == true {
        return Level::reported(State::Silent, "RF muted");
    }
    if node.channel["audio_mute"] == true {
        return Level::reported(State::Silent, "muted");
    }
    let text = wireless_text(&node.channel);
    if let Some(rms) = node.channel["audio_level_rms"].as_i64() {
        return Level::reported(
            if rms - 120 >= -80 {
                State::Signal
            } else {
                State::Silent
            },
            text.expect("integer RMS produces level text"),
        );
    }
    Level::unknown(text.unwrap_or_else(|| "not metered".to_owned()))
}

impl Graph {
    fn meter_nodes<'a>(&'a self, identity: &'a str) -> Vec<&'a JackNode> {
        let Some(node) = self.nodes.get(identity) else {
            return Vec::new();
        };
        match &node.details {
            Details::Jack(node) => vec![node],
            Details::Pulse(node) if matches!(node.kind, PulseKind::Sink | PulseKind::Source) => {
                let edges = if node.kind == PulseKind::Sink {
                    &self.downstream
                } else {
                    &self.upstream
                };
                edges
                    .get(identity)
                    .into_iter()
                    .flatten()
                    .filter_map(|edge| match &self.nodes[&edge.identity].details {
                        Details::Jack(node) => Some(node),
                        _ => None,
                    })
                    .collect()
            }
            _ => Vec::new(),
        }
    }

    pub fn meter_targets(
        &self,
        identities: &[String],
        hosts: &BTreeMap<String, HostSnapshot>,
    ) -> MeterTargets {
        let mut targets = MeterTargets::new();
        for identity in identities {
            for node in self.meter_nodes(identity) {
                let Some(port) = hosts
                    .get(&node.host)
                    .filter(|host| host.jack.available)
                    .and_then(|host| host.jack.ports.get(&node.port))
                else {
                    continue;
                };
                let mut sources = match port.direction {
                    PortDirection::Output => vec![node.port.clone()],
                    PortDirection::Input => port.connections.clone(),
                };
                sources.sort();
                sources.dedup();
                targets
                    .entry(node.host.clone())
                    .or_default()
                    .insert(node.port.clone(), sources);
            }
        }
        targets
    }

    pub fn levels(
        &self,
        identities: &[String],
        jack: &BTreeMap<String, Readings>,
        dante: &BTreeMap<String, Value>,
    ) -> Levels {
        identities
            .iter()
            .filter_map(|identity| {
                self.level(identity, jack, dante)
                    .map(|level| (identity.clone(), level))
            })
            .collect()
    }

    pub fn level(
        &self,
        identity: &str,
        jack: &BTreeMap<String, Readings>,
        dante: &BTreeMap<String, Value>,
    ) -> Option<Level> {
        let node = self.nodes.get(identity)?;
        Some(match &node.details {
            Details::Jack(node) => {
                let mut level = Level::jack(
                    jack.get(&node.host)
                        .and_then(|levels| levels.get(&node.port)),
                );
                if node.direction == PortDirection::Output {
                    if let Some(stage) = &node.stage {
                        if stage.muted == Some(true) {
                            level.text.push_str(", volume stage reports mute");
                        }
                        if let Some(value) = stage.level.filter(|level| *level != 100.0) {
                            level.text.push_str(&format!(", volume stage at {value}%"));
                        }
                    }
                }
                level
            }
            Details::Dante(node) => {
                if node.online == Some(false) {
                    Level::unknown("device offline")
                } else if let Some(reason) = &node.unresolved {
                    Level::unknown(reason)
                } else {
                    let reading = node
                        .record
                        .as_ref()
                        .and_then(|key| dante.get(key))
                        .and_then(|value| {
                            node.channel
                                .map(|number| &value[node.direction.name()][number.to_string()])
                        });
                    Level::dante(reading)
                }
            }
            Details::Wireless(node) => wireless(node),
            Details::Pulse(node) => {
                let entry = &node.entry;
                let volume = if entry.volume_percent.is_empty() {
                    String::new()
                } else {
                    format!(
                        "volume {}%",
                        entry
                            .volume_percent
                            .iter()
                            .map(u32::to_string)
                            .collect::<Vec<_>>()
                            .join("/")
                    )
                };
                if matches!(node.kind, PulseKind::Playback | PulseKind::Recording) {
                    let mut parts = Vec::new();
                    if entry.corked == Some(true) {
                        parts.push("paused".to_owned());
                    }
                    if entry.mute == Some(true) {
                        parts.push("muted".to_owned());
                    }
                    if !volume.is_empty() {
                        parts.push(volume);
                    }
                    let text = if parts.is_empty() {
                        "running; audio level not measured".to_owned()
                    } else {
                        parts.join(", ")
                    };
                    Level::reported(
                        if entry.corked == Some(true) || entry.mute == Some(true) {
                            State::Silent
                        } else {
                            State::Unknown
                        },
                        text,
                    )
                } else {
                    let readings = self
                        .meter_nodes(identity)
                        .into_iter()
                        .map(|node| {
                            Level::jack(
                                jack.get(&node.host)
                                    .and_then(|levels| levels.get(&node.port)),
                            )
                        })
                        .collect::<Vec<_>>();
                    let mut level = readings
                        .iter()
                        .filter(|level| level.state != State::Unknown)
                        .max_by(|first, second| {
                            first
                                .peak_dbfs
                                .unwrap_or(f64::NEG_INFINITY)
                                .total_cmp(&second.peak_dbfs.unwrap_or(f64::NEG_INFINITY))
                        })
                        .cloned()
                        .unwrap_or_else(|| {
                            Level::unknown(entry.state.as_deref().unwrap_or("not measured"))
                        });
                    if readings.len() > 1 && level.state != State::Unknown {
                        level.text = format!("loudest measured channel {}", level.text);
                    }
                    if readings
                        .iter()
                        .any(|reading| reading.state == State::Unknown)
                    {
                        if level.state == State::Silent {
                            level.state = State::Unknown;
                        }
                        level.text.push_str("; some channel levels are unavailable");
                    }
                    if !volume.is_empty() {
                        level.text.push_str(&format!(", {volume}"));
                    }
                    if entry.mute == Some(true) {
                        level.text.push_str(", muted");
                    }
                    level
                }
            }
        })
    }
}
