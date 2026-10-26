CREATE TABLE IF NOT EXISTS capture_sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    source_host TEXT,
    description TEXT,
    category TEXT DEFAULT 'experiment',
    started_ns INTEGER NOT NULL,
    started_iso TEXT NOT NULL,
    ended_ns INTEGER,
    ended_iso TEXT,
    metadata_json TEXT
);

CREATE TABLE IF NOT EXISTS capture_markers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id INTEGER NOT NULL REFERENCES capture_sessions(id),
    marker_type TEXT NOT NULL,
    label TEXT NOT NULL,
    summary TEXT,
    note TEXT,
    source_host TEXT,
    data_json TEXT,
    timestamp_ns INTEGER NOT NULL,
    timestamp_iso TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS packets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp_ns INTEGER NOT NULL,
    timestamp_iso TEXT NOT NULL,
    src_ip TEXT,
    src_port INTEGER,
    dst_ip TEXT,
    dst_port INTEGER,
    source_type TEXT NOT NULL,
    direction TEXT,
    device_name TEXT,
    device_ip TEXT,
    protocol_id INTEGER,
    protocol_name TEXT,
    transaction_id INTEGER,
    opcode INTEGER,
    opcode_name TEXT,
    result_code INTEGER,
    result_name TEXT,
    payload BLOB NOT NULL,
    correlated_packet_id INTEGER REFERENCES packets(id),
    multicast_group TEXT,
    multicast_port INTEGER,
    session_id INTEGER REFERENCES capture_sessions(id),
    source_host TEXT,
    interface TEXT,
    correlation_kind TEXT
);

CREATE INDEX IF NOT EXISTS idx_packets_transaction
    ON packets(transaction_id, device_ip, direction);
CREATE INDEX IF NOT EXISTS idx_packets_opcode
    ON packets(opcode);
CREATE INDEX IF NOT EXISTS idx_packets_source_type
    ON packets(source_type);
CREATE INDEX IF NOT EXISTS idx_packets_device_ip_time
    ON packets(device_ip, timestamp_ns);
CREATE INDEX IF NOT EXISTS idx_packets_correlated
    ON packets(correlated_packet_id);
CREATE INDEX IF NOT EXISTS idx_capture_markers_session
    ON capture_markers(session_id, timestamp_ns);
CREATE INDEX IF NOT EXISTS idx_capture_sessions_started
    ON capture_sessions(started_ns);

CREATE TABLE IF NOT EXISTS capture_artifacts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id INTEGER NOT NULL REFERENCES capture_sessions(id),
    label TEXT NOT NULL,
    role TEXT NOT NULL,
    filename TEXT NOT NULL,
    media_type TEXT NOT NULL,
    note TEXT,
    source_path TEXT,
    source_host TEXT,
    source_modified_ns INTEGER,
    sha256 TEXT NOT NULL,
    size INTEGER NOT NULL,
    content BLOB NOT NULL,
    timestamp_ns INTEGER NOT NULL,
    timestamp_iso TEXT NOT NULL,
    UNIQUE(session_id, label, sha256)
);

CREATE INDEX IF NOT EXISTS idx_capture_artifacts_session
    ON capture_artifacts(session_id, timestamp_ns, id);

CREATE TABLE IF NOT EXISTS packet_status_request_candidates (
    status_packet_id INTEGER NOT NULL REFERENCES packets(id),
    request_packet_id INTEGER NOT NULL REFERENCES packets(id),
    rule TEXT NOT NULL,
    delay_ns INTEGER NOT NULL,
    PRIMARY KEY (status_packet_id, request_packet_id)
);
CREATE INDEX IF NOT EXISTS idx_packet_status_request_candidates_request
    ON packet_status_request_candidates(request_packet_id);

CREATE TABLE IF NOT EXISTS packet_sessions (
    packet_id INTEGER NOT NULL REFERENCES packets(id),
    session_id INTEGER NOT NULL REFERENCES capture_sessions(id),
    PRIMARY KEY (packet_id, session_id)
);
CREATE INDEX IF NOT EXISTS idx_packet_sessions_session
    ON packet_sessions(session_id, packet_id);
