use super::{Outcome, Registry, Request};
use anyhow::{Context, Result, ensure};
use netaudio_core::shure::{
    Change, Decoder, Device, Message, Protocol, Report, command, described, indexed, meter_value,
    sampled,
};
use serde_json::{Map, Value, json};
use std::net::Ipv4Addr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::{Instant, timeout, timeout_at};
use tokio_util::sync::CancellationToken;

const CONNECT_DEADLINE: Duration = Duration::from_secs(2);
const REQUEST_DEADLINE: Duration = Duration::from_secs(3);
const MAXIMUM_ATTEMPTS: u32 = 3;
const DEVICE_KEYS: &[&str] = &[
    "DEVICE_ID",
    "MODEL",
    "FW_VER",
    "RF_BAND",
    "TRANSMISSION_MODE",
    "QUADVERSITY_MODE",
    "ENCRYPTION_MODE",
];
const CHANNEL_KEYS: &[&str] = &[
    "CHAN_NAME",
    "AUDIO_GAIN",
    "AUDIO_MUTE",
    "FREQUENCY",
    "GROUP_CHANNEL",
    "FD_MODE",
    "ENCRYPTION_STATUS",
    "INTERFERENCE_STATUS",
    "AUDIO_LEVEL_PEAK",
    "AUDIO_LEVEL_RMS",
    "CHAN_QUALITY",
    "ANTENNA_STATUS",
    "TX_BATT_MINS",
    "TX_BATT_TYPE",
    "TX_BATT_CHARGE_PERCENT",
    "TX_BATT_BARS",
    "TX_BATT_CYCLE_COUNT",
    "TX_BATT_TEMP_F",
    "TX_MODEL",
    "TX_DEVICE_ID",
    "TX_POWER_LEVEL",
    "TX_MUTE_MODE_STATUS",
];
const PERSONAL_MONITOR_KEYS: &[&str] = &[
    "CHAN_NAME",
    "AUDIO_IN_LVL",
    "GROUP_CHAN",
    "FREQUENCY",
    "RF_TX_LVL",
    "RF_MUTE",
    "AUDIO_TX_MODE",
    "AUDIO_IN_LINE_LVL",
];

struct Connection {
    stream: TcpStream,
    decoder: Decoder,
    device: Device,
    registry: Arc<Registry>,
    generation: u64,
    channels: u8,
}

pub(super) async fn run(
    registry: Arc<Registry>,
    mac: String,
    address: Ipv4Addr,
    generation: u64,
    mut requests: mpsc::Receiver<Request>,
    cancellation: CancellationToken,
) -> Result<()> {
    let active = async {
        let mut attempt = 0;
        while attempt < MAXIMUM_ATTEMPTS {
            if attempt > 0 {
                tokio::select! {
                    _ = cancellation.cancelled() => break,
                    _ = tokio::time::sleep(Duration::from_secs(u64::from(attempt))) => {}
                }
            }
            if cancellation.is_cancelled() {
                break;
            }
            attempt += 1;
            let mut established = None;
            registry.status(&mac, generation, json!({"state": "connecting", "attempt": attempt, "maximum_attempts": MAXIMUM_ATTEMPTS}))?;
            let result = async {
                let mut connection = Connection::open(Arc::clone(&registry), &mac, address, generation, &cancellation).await?;
                registry.status(&mac, generation, json!({"state": "ready", "attempt": attempt, "maximum_attempts": MAXIMUM_ATTEMPTS}))?;
                established = Some(Instant::now());
                let result = connection.run(&mut requests, &cancellation).await;
                connection.close().await;
                result
            }.await;
            if cancellation.is_cancelled() {
                break;
            }
            let error = result.err().map_or_else(
                || "Wireless control session ended".to_owned(),
                |error: anyhow::Error| error.to_string(),
            );
            tracing::warn!(%mac, %address, attempt, %error, "Wireless control session disconnected");
            if established.is_some_and(|started| started.elapsed() >= Duration::from_secs(30)) {
                attempt = 0;
            }
            registry.status(&mac, generation, json!({"state": if attempt == MAXIMUM_ATTEMPTS { "exhausted" } else { "reconnecting" }, "error": error,
                "attempt": attempt, "maximum_attempts": MAXIMUM_ATTEMPTS}))?;
        }
        Ok::<(), anyhow::Error>(())
    };
    active.await?;
    requests.close();
    while let Some(request) = requests.recv().await {
        let _ = request.reply.send(Outcome { status: 503, value: json!({"error": "Wireless connection is unavailable", "change_sent": false, "verified": false}) });
    }
    Ok(())
}

impl Connection {
    async fn open(
        registry: Arc<Registry>,
        mac: &str,
        address: Ipv4Addr,
        generation: u64,
        cancellation: &CancellationToken,
    ) -> Result<Self> {
        let _connection_permit = tokio::select! {
            biased;
            _ = cancellation.cancelled() => anyhow::bail!("Wireless connection acquisition canceled"),
            permit = Arc::clone(&registry.connecting).acquire_owned() => permit?,
        };
        let mut stream = timeout(CONNECT_DEADLINE, TcpStream::connect((address, 2202))).await??;
        stream.set_nodelay(true)?;
        let keepalive = socket2::TcpKeepalive::new()
            .with_time(Duration::from_secs(30))
            .with_interval(Duration::from_secs(10));
        socket2::SockRef::from(&stream).set_tcp_keepalive(&keepalive)?;
        for query in ["GET MODEL", "GET DEVICE_ID", "GET DEVICE_NAME"] {
            timeout(REQUEST_DEADLINE, stream.write_all(&command(query)?)).await??;
        }
        let mut decoder = Decoder::default();
        let mut received = Vec::new();
        let deadline = Instant::now() + CONNECT_DEADLINE;
        let protocol = loop {
            let mut bytes = [0; 8192];
            let count = timeout_at(deadline, stream.read(&mut bytes)).await??;
            ensure!(
                count != 0,
                "Wireless peer closed during protocol identification"
            );
            let messages = decoder.push(&bytes[..count])?;
            let mut found = None;
            for message in &messages {
                if let Message::Report(protocol, report) = message {
                    if *protocol == Protocol::Ad4d
                        && report.channel.is_none()
                        && report.key == "MODEL"
                    {
                        ensure!(
                            Protocol::for_model(&report.value) == Some(Protocol::Ad4d),
                            "Wireless receiver model has no supported protocol"
                        );
                        found = Some(*protocol);
                    } else if *protocol == Protocol::P10t
                        && report.channel.is_none()
                        && report.key == "DEVICE_NAME"
                    {
                        found = Some(*protocol);
                    }
                }
            }
            ensure!(
                received.len() + messages.len() <= 256,
                "Wireless identification response exceeded its limit"
            );
            received.extend(messages);
            if let Some(protocol) = found {
                break protocol;
            }
        };
        let mut connection = Self {
            stream,
            decoder,
            device: Device::new(address.to_string(), mac.to_owned(), protocol),
            registry,
            generation,
            channels: 2,
        };
        connection.observe(&received)?;
        if connection
            .device
            .properties
            .get("MODEL")
            .and_then(Value::as_str)
            .is_some_and(|model| model.to_ascii_uppercase().starts_with("AD4Q"))
        {
            connection.channels = 4;
        }
        let mut queries = Vec::new();
        match protocol {
            Protocol::Ad4d => {
                queries.extend(DEVICE_KEYS.iter().map(|key| format!("GET {key}")));
                for channel in 1..=connection.channels {
                    queries.extend(
                        CHANNEL_KEYS
                            .iter()
                            .map(|key| format!("GET {channel} {key}")),
                    );
                }
                queries.push("GET 0 ALL".to_owned());
                for interface in ["SC", "D1", "D2"] {
                    queries.push(format!("GET NET_SETTINGS {interface}"));
                }
                for channel in 1..=connection.channels {
                    queries.push(format!("SET {channel} METER_RATE 00100"));
                }
            }
            Protocol::P10t => {
                queries.push("GET DEVICE_NAME".to_owned());
                for channel in 1..=connection.channels {
                    queries.extend(
                        PERSONAL_MONITOR_KEYS
                            .iter()
                            .map(|key| format!("GET {channel} {key}")),
                    );
                    queries.push(format!("SET {channel} METER_RATE 100"));
                }
            }
        }
        let mut bytes = Vec::new();
        for query in queries {
            bytes.extend(command(&query)?);
        }
        timeout(REQUEST_DEADLINE, connection.stream.write_all(&bytes)).await??;
        let initialized = async {
            let deadline = Instant::now() + REQUEST_DEADLINE;
            while !(1..=connection.channels).all(|channel| {
                connection
                    .device
                    .value(Some(channel), "CHAN_NAME", None)
                    .is_some()
            }) {
                let mut bytes = [0; 8192];
                let count = timeout_at(deadline, connection.stream.read(&mut bytes)).await??;
                ensure!(
                    count != 0,
                    "Wireless peer closed during inventory initialization"
                );
                let messages = connection.decoder.push(&bytes[..count])?;
                connection.observe(&messages)?;
            }
            Ok::<(), anyhow::Error>(())
        }
        .await;
        if let Err(error) = initialized {
            connection.close().await;
            return Err(error);
        }
        Ok(connection)
    }

    async fn run(
        &mut self,
        requests: &mut mpsc::Receiver<Request>,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        loop {
            let mut bytes = [0; 8192];
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => return Ok(()),
                result = self.stream.read(&mut bytes) => {
                    let count = result?;
                    ensure!(count != 0, "Wireless peer closed its control connection");
                    let messages = self.decoder.push(&bytes[..count])?;
                    self.observe(&messages)?;
                }
                request = requests.recv() => {
                    let request = request.context("Wireless request queue closed")?;
                    let (outcome, usable) = self.control(&request).await;
                    let _ = request.reply.send(outcome);
                    ensure!(usable, "Wireless command left the connection state uncertain");
                }
            }
        }
    }

    fn observe(&mut self, messages: &[Message]) -> Result<()> {
        let observed = chrono::Utc::now();
        let timestamp = observed.timestamp_micros() as f64 / 1_000_000.0;
        let mut changed = false;
        for message in messages {
            let reports: &[Report] = match message {
                Message::Report(protocol, report) => {
                    ensure!(
                        *protocol == self.device.protocol,
                        "Wireless peer changed response protocol"
                    );
                    std::slice::from_ref(report)
                }
                Message::Sample(reports) => {
                    ensure!(
                        self.device.protocol == Protocol::Ad4d,
                        "Wireless sample does not match its protocol"
                    );
                    reports
                }
                Message::Rejected(_) => continue,
            };
            for report in reports {
                let previous_antenna = self.device.value(report.channel, "ANTENNA_STATUS", None);
                let property_changed = self.device.observe(report, timestamp)?;
                changed |= property_changed
                    && (!sampled(&report.key)
                        || (report.key == "ANTENNA_STATUS"
                            && previous_antenna.as_deref() != Some(&report.value)));
                if let Some(channel) = report.channel {
                    if let Some((peak, signal)) = meter_value(&report.key, &report.value) {
                        self.registry.history.record_value(
                            &format!("shure:{}", self.device.mac),
                            &channel.to_string(),
                            observed.timestamp(),
                            peak,
                            signal,
                        )?;
                    }
                    if matches!(message, Message::Report(_, _))
                        && matches!(
                            report.key.as_str(),
                            "AUDIO_LEVEL_PEAK"
                                | "AUDIO_LEVEL_RMS"
                                | "AUDIO_IN_LVL_L"
                                | "AUDIO_IN_LVL_R"
                        )
                    {
                        self.registry.model.publish(json!({"event": "shure_meter_values", "mac": self.device.mac, "channel": channel, "key": report.key, "value": report.value, "observed_at": observed.to_rfc3339()}))?;
                    }
                }
            }
            if let Message::Sample(reports) = message {
                let mut values = Map::new();
                for report in reports {
                    if indexed(&report.key) {
                        if let Some((index, value)) = report.value.split_once(' ') {
                            values
                                .entry(report.key.clone())
                                .or_insert_with(|| json!({}))[index] = json!(value);
                        }
                    } else {
                        values.insert(report.key.clone(), json!(report.value));
                    }
                }
                self.registry.model.publish(json!({"event": "shure_meter_values", "mac": self.device.mac, "channel": reports.first().and_then(|report| report.channel), "values": values, "observed_at": observed.to_rfc3339()}))?;
            }
        }
        self.registry
            .observe(&self.device.mac, self.generation, &self.device, changed)
    }

    async fn exchange(
        &mut self,
        bytes: &[u8],
        change: &Change,
        sent: &mut Option<bool>,
    ) -> Result<Option<Report>> {
        let deadline = Instant::now() + REQUEST_DEADLINE;
        *sent = None;
        timeout_at(deadline, self.stream.write_all(bytes)).await??;
        *sent = Some(true);
        loop {
            let mut bytes = [0; 8192];
            let count = timeout_at(deadline, self.stream.read(&mut bytes)).await??;
            ensure!(
                count != 0,
                "Wireless connection closed while awaiting a property report"
            );
            let messages = self.decoder.push(&bytes[..count])?;
            self.observe(&messages)?;
            for message in messages {
                match message {
                    Message::Report(_, report) if change.matches(&report) => {
                        return Ok(Some(report));
                    }
                    Message::Rejected(_) => return Ok(None),
                    _ => {}
                }
            }
        }
    }

    async fn control(&mut self, request: &Request) -> (Outcome, bool) {
        if let Err(error) = self
            .registry
            .prepare_owned(&self.device.mac, self.generation, request)
        {
            return (
                Outcome {
                    status: 409,
                    value: json!({"error": error.to_string(), "change_sent": false, "verified": false}),
                },
                true,
            );
        }
        let mut preflight_complete = false;
        let prepared = async {
            let change = self
                .registry
                .prepare_owned(&self.device.mac, self.generation, request)?;
            if change.key != "FLASH" {
                let channel = change
                    .channel
                    .map_or(String::new(), |channel| format!("{channel} "));
                let index = if indexed(&change.key) {
                    format!(
                        " {}",
                        change
                            .value
                            .split_whitespace()
                            .next()
                            .context("Indexed setting has no index")?
                    )
                } else {
                    String::new()
                };
                let query = command(&format!("GET {channel}{}{index}", change.key))?;
                ensure!(
                    self.exchange(&query, &change, &mut Some(false))
                        .await?
                        .is_some(),
                    "Wireless device rejected the preflight query"
                );
            }
            preflight_complete = true;
            self.registry
                .prepare_owned(&self.device.mac, self.generation, request)
        }
        .await;
        let change = match prepared {
            Ok(change) => change,
            Err(error) => {
                return (
                    Outcome {
                        status: 409,
                        value: json!({"error": error.to_string(), "change_sent": false, "verified": false}),
                    },
                    preflight_complete,
                );
            }
        };
        let mut value = match serde_json::to_value(&change) {
            Ok(value) => value,
            Err(error) => {
                return (
                    Outcome {
                        status: 500,
                        value: json!({"error": error.to_string(), "change_sent": false, "verified": false}),
                    },
                    true,
                );
            }
        };
        let bytes = match change.command() {
            Ok(bytes) => bytes,
            Err(error) => {
                return (
                    Outcome {
                        status: 400,
                        value: json!({"error": error.to_string(), "change_sent": false, "verified": false}),
                    },
                    true,
                );
            }
        };
        let audit = match super::audit::Audit::begin(&self.registry.journal, &self.device, &change)
            .await
        {
            Ok(audit) => audit,
            Err(error) => {
                return (
                    Outcome {
                        status: 500,
                        value: json!({"error": error.to_string(), "change_sent": false, "verified": false}),
                    },
                    true,
                );
            }
        };
        value["operation_id"] = json!(audit.identifier());
        let mut sent = Some(false);
        let (mut outcome, usable) = match self.exchange(&bytes, &change, &mut sent).await {
            Ok(Some(report)) => {
                let verified = if change.key == "FLASH" {
                    report.value == "ON"
                } else {
                    change.verified(&report)
                };
                value["reported"] = json!(report.value);
                value["reported_described"] =
                    described(change.device_type, &change.key, &report.value);
                value["verified"] = json!(verified);
                value["change_sent"] = json!(true);
                value["verification_scope"] = json!(if change.key == "FLASH" {
                    "command_report"
                } else {
                    "reported_configuration"
                });
                value["physical_state_verified"] = Value::Null;
                (
                    Outcome {
                        status: if verified { 200 } else { 502 },
                        value,
                    },
                    true,
                )
            }
            Ok(None) => {
                value["error"] = json!("Wireless device rejected the command");
                value["rejected"] = json!(true);
                value["change_sent"] = json!(true);
                value["verified"] = json!(false);
                (Outcome { status: 409, value }, true)
            }
            Err(error) => {
                value["error"] = json!(error.to_string());
                value["change_sent"] = json!(sent);
                value["verified"] = json!(false);
                value["reported"] = Value::Null;
                (Outcome { status: 502, value }, false)
            }
        };
        if let Err(error) = audit.finish(&self.registry.journal, &outcome.value).await {
            outcome.status = 500;
            outcome.value["journal_error"] = json!(error.to_string());
        }
        (outcome, usable)
    }

    async fn close(&mut self) {
        let closing = async {
            for channel in 1..=self.channels {
                self.stream
                    .write_all(&command(&format!("SET {channel} METER_RATE 0"))?)
                    .await?;
            }
            self.stream.shutdown().await?;
            Ok::<_, anyhow::Error>(())
        };
        match timeout(Duration::from_secs(1), closing).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                tracing::debug!(mac = %self.device.mac, %error, "Wireless stream cleanup failed")
            }
            Err(error) => {
                tracing::debug!(mac = %self.device.mac, %error, "Wireless stream cleanup timed out")
            }
        }
    }
}
