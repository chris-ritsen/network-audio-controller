use super::Packet;
use anyhow::{Context, Result};
use flate2::{Compression, write::ZlibEncoder};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::Value;
use std::io::Write;
use std::path::Path;
use std::time::Duration;

pub(super) struct Recorder {
    database: Option<Connection>,
    session: Option<i64>,
    dissect: bool,
    hostname: String,
    insert: String,
}

impl Recorder {
    pub fn open(path: Option<&Path>, dissect: bool) -> Result<Self> {
        let mut session = None;
        let mut hexadecimal_column = false;
        let database = if let Some(path) = path {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let connection = Connection::open(path).context("Open packet capture database")?;
            connection.busy_timeout(Duration::from_secs(2))?;
            connection.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;")?;
            connection.execute_batch(include_str!("schema.sql"))?;
            for (table, column, definition) in [
                (
                    "packets",
                    "session_id",
                    "INTEGER REFERENCES capture_sessions(id)",
                ),
                ("packets", "source_host", "TEXT"),
                ("packets", "interface", "TEXT"),
                ("packets", "correlation_kind", "TEXT"),
                ("capture_sessions", "category", "TEXT DEFAULT 'experiment'"),
                ("capture_markers", "summary", "TEXT"),
            ] {
                let columns: Vec<String> = connection
                    .prepare(&format!("PRAGMA table_info({table})"))?
                    .query_map([], |row| row.get(1))?
                    .collect::<rusqlite::Result<_>>()?;
                if !columns.iter().any(|existing| existing == column) {
                    connection.execute(
                        &format!("ALTER TABLE {table} ADD COLUMN {column} {definition}"),
                        [],
                    )?;
                }
            }
            connection.execute("CREATE INDEX IF NOT EXISTS idx_packets_session ON packets(session_id,timestamp_ns)", [])?;
            let columns: Vec<String> = connection
                .prepare("PRAGMA table_info(packets)")?
                .query_map([], |row| row.get(1))?
                .collect::<rusqlite::Result<_>>()?;
            hexadecimal_column = columns.iter().any(|column| column == "payload_hex");
            session = connection.query_row("SELECT id FROM capture_sessions WHERE ended_ns IS NULL ORDER BY started_ns DESC, id DESC LIMIT 1", [], |row| row.get(0)).optional()?;
            Some(connection)
        } else {
            None
        };
        Ok(Self {
            database,
            session,
            dissect,
            hostname: hostname::get()?
                .to_str()
                .context("Hostname is not UTF-8")?
                .to_owned(),
            insert: format!(
                "INSERT INTO packets (timestamp_ns,timestamp_iso,src_ip,src_port,dst_ip,dst_port,source_type,direction,device_ip,protocol_id,protocol_name,transaction_id,opcode,opcode_name,result_code,result_name,payload,multicast_group,multicast_port,session_id,source_host{}) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?{})",
                if hexadecimal_column {
                    ",payload_hex"
                } else {
                    ""
                },
                if hexadecimal_column { ",''" } else { "" }
            ),
        })
    }

    pub fn session_id(&self) -> Option<i64> {
        self.session
    }

    pub fn write(&mut self, packets: Vec<Packet>) -> Result<()> {
        let transaction = self
            .database
            .as_mut()
            .map(Connection::transaction)
            .transpose()?;
        for packet in packets {
            let header = netaudio_core::protocol::diagnostic_packet_header(&packet.payload)
                .map(serde_json::to_value)
                .transpose()?
                .unwrap_or(Value::Null);
            if self.dissect {
                tracing::info!(source = %packet.source, destination = %packet.destination, direction = packet.direction, observed_at = %packet.observed_at, header = %header, payload_hexadecimal = %hex::encode(&packet.payload), "Device packet dissection");
            }
            let Some(transaction) = &transaction else {
                continue;
            };
            let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
            encoder.write_all(&packet.payload)?;
            let payload = encoder.finish()?;
            let multicast = packet.destination.ip().is_multicast();
            let source_type = if multicast {
                "multicast"
            } else if packet.direction == "request" {
                "netaudio_request"
            } else {
                "netaudio_response"
            };
            let device = if packet.direction == "request" {
                packet.destination
            } else {
                packet.source
            };
            let timestamp = packet
                .observed_at
                .timestamp_nanos_opt()
                .context("Packet timestamp exceeds the capture database range")?;
            transaction.execute(
                &self.insert,
                params![
                    timestamp,
                    packet.observed_at.to_rfc3339(),
                    packet.source.ip().to_string(),
                    packet.source.port(),
                    packet.destination.ip().to_string(),
                    packet.destination.port(),
                    source_type,
                    packet.direction,
                    device.ip().to_string(),
                    header["protocol_id"].as_i64(),
                    header["protocol_name"].as_str(),
                    header["transaction_id"].as_i64(),
                    header["opcode"].as_i64(),
                    header["opcode_name"].as_str(),
                    header["result_code"].as_i64(),
                    header["result_name"].as_str(),
                    payload,
                    multicast.then(|| packet.destination.ip().to_string()),
                    multicast.then_some(packet.destination.port()),
                    self.session,
                    self.hostname,
                ],
            )?;
            let identifier = transaction.last_insert_rowid();
            if let Some(session) = self.session {
                transaction.execute(
                    "INSERT INTO packet_sessions (packet_id,session_id) VALUES (?,?)",
                    params![identifier, session],
                )?;
            }
            if let Some(transaction_identifier) = header["transaction_id"].as_i64() {
                let peer_direction = if packet.direction == "request" {
                    "response"
                } else {
                    "request"
                };
                let peer: Option<i64> = transaction.query_row("SELECT id FROM packets WHERE transaction_id=? AND device_ip=? AND direction=? AND opcode=? AND protocol_name IS ? AND session_id IS ? AND correlated_packet_id IS NULL AND id!=? AND (CASE WHEN direction='request' THEN dst_port ELSE src_port END)=? ORDER BY timestamp_ns DESC,id DESC LIMIT 1", params![transaction_identifier,device.ip().to_string(),peer_direction,header["opcode"].as_i64(),header["protocol_name"].as_str(),self.session,identifier,device.port()], |row| row.get(0)).optional()?;
                if let Some(peer) = peer {
                    transaction.execute("UPDATE packets SET correlated_packet_id=?,correlation_kind='transaction' WHERE id=?", params![peer,identifier])?;
                    transaction.execute("UPDATE packets SET correlated_packet_id=?,correlation_kind='transaction' WHERE id=?", params![identifier,peer])?;
                }
            }
            if multicast {
                let request_opcode = match header["opcode"].as_u64() {
                    Some(0x0080) => Some(0x0081),
                    Some(0x0082) => Some(0x0083),
                    Some(0x0084) => Some(0x0085),
                    Some(0x100b) => Some(0x100a),
                    Some(0x100e) => Some(0x100d),
                    _ => None,
                };
                if let Some(opcode) = request_opcode {
                    transaction.execute("INSERT OR IGNORE INTO packet_status_request_candidates (status_packet_id,request_packet_id,rule,delay_ns) SELECT ?,id,'temporal_same_status_family',?-timestamp_ns FROM packets WHERE device_ip=? AND direction='request' AND session_id IS ? AND opcode=? AND timestamp_ns BETWEEN ? AND ? AND id!=?", params![identifier,timestamp,device.ip().to_string(),self.session,opcode,timestamp-100_000_000,timestamp,identifier])?;
                }
            }
        }
        if let Some(transaction) = transaction {
            transaction.commit()?;
        }
        Ok(())
    }
}
