use std::collections::VecDeque;
use std::io;
use std::net::{IpAddr, SocketAddr, UdpSocket};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::Serialize;

use crate::commands;
use crate::parser::{
    parse_channel_audio_metadata, parse_channel_count, parse_rx_page, parse_tx_friendly_page,
    parse_tx_info_page, ChannelAudioMetadata, ChannelCount, RxChannel, TxChannel,
    RX_CHANNELS_PER_PAGE, TX_CHANNELS_PER_PAGE,
};
use crate::protocol::{
    build_set_device_name, NetaudioError, RESPONSE_HEADER_SIZE, RESULT_CODE_MORE_PAGES,
};
use crate::responses::{
    parse_aes67_configured, parse_device_info, parse_device_name, parse_device_settings,
    parse_property_directory, parse_result_code, DeviceInfo, DeviceSettings, PropertyDirectory,
    RESULT_CODE_SUCCESS,
};
use crate::spec::{build_routed_command, IoMode, SpecError, Target};

pub const DEVICE_SETTINGS_PORT: u16 = crate::protocol::NetaudioPort::Settings as u16;
pub const DEVICE_CONTROL_PORT: u16 = crate::protocol::NetaudioPort::Control as u16;

const MAX_WIRE_CAPTURES: usize = 256;
const MAX_WIRE_CAPTURE_BYTES: usize = 4 * 1024 * 1024;
const MAX_UDP_DATAGRAM_BYTES: usize = 65_535;
const MINIMUM_PACKET_BYTES: usize = 6;
const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";

#[derive(Clone, Debug, Serialize)]
pub struct WireCapture {
    pub timestamp_unix_milliseconds: u64,
    pub direction: &'static str,
    pub port: u16,
    pub matched: Option<bool>,
    pub payload_hex: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct RxInventory {
    pub channels: Vec<RxChannel>,
    pub channel_audio_metadata: Option<ChannelAudioMetadata>,
}

#[derive(Default)]
struct WireCaptureBuffer {
    entries: VecDeque<WireCapture>,
    payload_bytes: usize,
}

#[derive(Debug)]
pub enum ClientError {
    InvalidAddress,
    InvalidLength,
    Io(io::Error),
    MalformedResponse,
    MalformedResponseAt(&'static str),
    Protocol(NetaudioError),
    Spec(SpecError),
    Timeout,
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientError::InvalidAddress => {
                formatter.write_str("invalid device or local IPv4 address")
            }
            ClientError::InvalidLength => write!(
                formatter,
                "packet must be at least {MINIMUM_PACKET_BYTES} bytes so it carries a message id"
            ),
            ClientError::Io(error) => write!(formatter, "socket error: {error}"),
            ClientError::MalformedResponse => {
                formatter.write_str("binary device reply did not parse as the expected response")
            }
            ClientError::MalformedResponseAt(stage) => {
                write!(formatter, "binary device reply did not parse as {stage}")
            }
            ClientError::Protocol(error) => write!(formatter, "{error}"),
            ClientError::Spec(error) => write!(formatter, "{error}"),
            ClientError::Timeout => formatter.write_str("device did not reply before the timeout"),
        }
    }
}

impl From<SpecError> for ClientError {
    fn from(error: SpecError) -> Self {
        ClientError::Spec(error)
    }
}

impl From<NetaudioError> for ClientError {
    fn from(error: NetaudioError) -> Self {
        ClientError::Protocol(error)
    }
}

impl From<io::Error> for ClientError {
    fn from(error: io::Error) -> Self {
        ClientError::Io(error)
    }
}

#[derive(Clone, Debug)]
pub struct PreparedCommand {
    pub address: SocketAddr,
    pub io: IoMode,
    pub message_id: u16,
    pub packet: Vec<u8>,
}

fn more_pages(response: &[u8]) -> bool {
    crate::bytes::read_u16(response, 8) == Some(RESULT_CODE_MORE_PAGES)
}

fn next_message_id(counter: &mut u16) -> u16 {
    *counter = crate::protocol::next_message_id(*counter);
    *counter
}

pub fn fire_repeated(
    repeat: u32,
    interval_ms: u64,
    mut send: impl FnMut() -> Result<(), ClientError>,
) -> Result<Vec<u8>, ClientError> {
    let repeat = repeat.max(1);
    for attempt in 0..repeat {
        send()?;
        if attempt + 1 < repeat && interval_ms > 0 {
            thread::sleep(Duration::from_millis(interval_ms));
        }
    }
    Ok(Vec::new())
}

pub struct Client {
    socket: UdpSocket,
    device_address: SocketAddr,
    control_port: u16,
    settings_port: u16,
    message_counter: u16,
    timeout: Duration,
    attempts: u32,
    host_mac: Option<[u8; 6]>,
    wire_captures: WireCaptureBuffer,
}

impl Client {
    pub fn new(
        device_ip: IpAddr,
        local_ip: Option<IpAddr>,
        arc_port: u16,
        timeout: Duration,
        attempts: u32,
    ) -> Result<Client, ClientError> {
        let device_ip = match device_ip {
            IpAddr::V4(address) if !address.is_unspecified() && !address.is_multicast() => address,
            _ => return Err(ClientError::InvalidAddress),
        };
        let local_ip = match local_ip {
            None => Some(crate::netif::source_ipv4_for(device_ip).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NetworkUnreachable,
                    format!("no IPv4 network path is available for {device_ip}"),
                )
            })?),
            Some(IpAddr::V4(address))
                if !address.is_unspecified()
                    && !address.is_multicast()
                    && !address.is_broadcast() =>
            {
                Some(address)
            }
            _ => return Err(ClientError::InvalidAddress),
        };
        let socket = UdpSocket::bind((local_ip.expect("local IPv4 source was resolved"), 0))?;
        Ok(Client {
            socket,
            device_address: SocketAddr::new(IpAddr::V4(device_ip), arc_port),
            control_port: DEVICE_CONTROL_PORT,
            settings_port: DEVICE_SETTINGS_PORT,
            message_counter: 0,
            timeout,
            attempts: attempts.max(1),
            host_mac: Some(crate::netif::command_host_mac(
                local_ip.expect("local IPv4 source was resolved"),
            )),
            wire_captures: WireCaptureBuffer::default(),
        })
    }

    pub fn clear_wire_captures(&mut self) {
        self.wire_captures.entries.clear();
        self.wire_captures.payload_bytes = 0;
    }

    pub fn wire_captures(&self) -> Vec<WireCapture> {
        self.wire_captures.entries.iter().cloned().collect()
    }

    fn record_wire_capture(
        &mut self,
        direction: &'static str,
        port: u16,
        matched: Option<bool>,
        payload: &[u8],
    ) -> Result<(), ClientError> {
        if payload.len() > MAX_WIRE_CAPTURE_BYTES {
            return Err(NetaudioError::PacketTooLarge.into());
        }

        let mut payload_hex = String::with_capacity(payload.len() * 2);
        for byte in payload {
            payload_hex.push(char::from(HEX_DIGITS[usize::from(*byte >> 4)]));
            payload_hex.push(char::from(HEX_DIGITS[usize::from(*byte & 0x0F)]));
        }

        let timestamp_unix_milliseconds = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .min(u64::MAX as u128) as u64;

        let captures = &mut self.wire_captures;
        while !captures.entries.is_empty()
            && (captures.entries.len() >= MAX_WIRE_CAPTURES
                || captures.payload_bytes + payload.len() > MAX_WIRE_CAPTURE_BYTES)
        {
            if let Some(removed) = captures.entries.pop_front() {
                captures.payload_bytes = captures
                    .payload_bytes
                    .saturating_sub(removed.payload_hex.len() / 2);
            }
        }
        captures.payload_bytes += payload.len();
        captures.entries.push_back(WireCapture {
            timestamp_unix_milliseconds,
            direction,
            port,
            matched,
            payload_hex,
        });
        Ok(())
    }

    /// Route CMC and settings commands to the ports this device advertises or reports, for devices that do not
    /// use the well-known ones (for example several devices sharing one host).
    pub fn set_service_ports(&mut self, control_port: u16, settings_port: u16) {
        self.control_port = control_port;
        self.settings_port = settings_port;
    }

    pub fn set_host_mac(&mut self, host_mac: [u8; 6]) {
        self.host_mac = Some(host_mac);
    }

    fn target_address(&self, target: Target) -> SocketAddr {
        let port = match target {
            Target::Arc => self.device_address.port(),
            Target::Control => self.control_port,
            Target::Settings => self.settings_port,
        };
        SocketAddr::new(self.device_address.ip(), port)
    }

    pub fn prepare_command(&mut self, json: &str) -> Result<PreparedCommand, ClientError> {
        let host_mac = self.host_mac;
        let counter = &mut self.message_counter;
        let routed = build_routed_command(json, host_mac, || next_message_id(counter))?;
        Ok(PreparedCommand {
            address: self.target_address(routed.target),
            io: routed.io,
            message_id: routed.message_id,
            packet: routed.packet,
        })
    }

    pub fn execute(&mut self, json: &str) -> Result<Vec<u8>, ClientError> {
        let prepared = self.prepare_command(json)?;
        match prepared.io {
            IoMode::Fire {
                repeat,
                interval_ms,
            } => fire_repeated(repeat, interval_ms, || self.send_prepared(&prepared)),
            IoMode::Request => self.request_prepared(&prepared),
        }
    }

    pub fn send_prepared(&mut self, prepared: &PreparedCommand) -> Result<(), ClientError> {
        self.send_datagram(&prepared.packet, prepared.address)
    }

    pub fn request_prepared(&mut self, prepared: &PreparedCommand) -> Result<Vec<u8>, ClientError> {
        self.request_to(&prepared.packet, prepared.message_id, prepared.address)
    }

    fn next_message_id(&mut self) -> u16 {
        next_message_id(&mut self.message_counter)
    }

    pub fn set_device_name(&mut self, name: &str) -> Result<Vec<u8>, ClientError> {
        let message_id = self.next_message_id();
        let packet = build_set_device_name(name, message_id)?;
        let response = self.request(&packet, message_id)?;
        if parse_result_code(&response) != Some(RESULT_CODE_SUCCESS) {
            return Err(ClientError::MalformedResponse);
        }
        Ok(response)
    }

    pub fn lock_device(
        &self,
        pin: &str,
        key: &[u8],
    ) -> Result<crate::lock::LockResult, crate::lock::LockError> {
        crate::lock::lock_operation(
            self.device_address.ip(),
            self.socket.local_addr()?.ip(),
            pin,
            key,
            crate::lock::LockOperation::Lock,
        )
    }

    pub fn unlock_device(
        &self,
        pin: &str,
        key: &[u8],
    ) -> Result<crate::lock::LockResult, crate::lock::LockError> {
        crate::lock::lock_operation(
            self.device_address.ip(),
            self.socket.local_addr()?.ip(),
            pin,
            key,
            crate::lock::LockOperation::Unlock,
        )
    }

    pub fn get_device_name(&mut self) -> Result<Option<String>, ClientError> {
        let message_id = self.next_message_id();
        let packet = commands::build_device_name(message_id)?;
        let response = self.request(&packet, message_id)?;
        parse_device_name(&response)
            .map(Some)
            .ok_or(ClientError::MalformedResponse)
    }

    fn raw_target(
        &self,
        packet: &[u8],
        target_port: u16,
    ) -> Result<(SocketAddr, u16), ClientError> {
        if packet.len() < MINIMUM_PACKET_BYTES {
            return Err(ClientError::InvalidLength);
        }
        let address = SocketAddr::new(self.device_address.ip(), target_port);
        let message_id = u16::from_be_bytes([packet[4], packet[5]]);
        Ok((address, message_id))
    }

    pub fn send_raw(&mut self, packet: &[u8], target_port: u16) -> Result<(), ClientError> {
        let (address, _) = self.raw_target(packet, target_port)?;
        self.send_datagram(packet, address)
    }

    pub fn request_raw(&mut self, packet: &[u8], target_port: u16) -> Result<Vec<u8>, ClientError> {
        let (address, message_id) = self.raw_target(packet, target_port)?;
        self.request_to(packet, message_id, address)
    }

    pub fn get_device_info(&mut self) -> Result<DeviceInfo, ClientError> {
        let message_id = self.next_message_id();
        let packet = commands::build_device_info(message_id)?;
        let response = self.request(&packet, message_id)?;
        parse_device_info(&response).ok_or(ClientError::MalformedResponse)
    }

    pub fn get_device_settings(&mut self) -> Result<DeviceSettings, ClientError> {
        let message_id = self.next_message_id();
        let packet = commands::build_device_settings(message_id)?;
        let response = self.request(&packet, message_id)?;
        parse_device_settings(&response).ok_or(ClientError::MalformedResponse)
    }

    pub fn get_property_directory(&mut self) -> Result<PropertyDirectory, ClientError> {
        let message_id = self.next_message_id();
        let packet = commands::build_property_directory(message_id)?;
        let response = self.request(&packet, message_id)?;
        parse_property_directory(&response).ok_or(ClientError::MalformedResponse)
    }

    pub fn get_aes67_configured(&mut self) -> Result<Option<bool>, ClientError> {
        let message_id = self.next_message_id();
        let packet = commands::build_query_latency_config(message_id)?;
        let response = self.request(&packet, message_id)?;
        parse_aes67_configured(&response).ok_or(ClientError::MalformedResponse)
    }

    pub fn get_channel_count(&mut self) -> Result<ChannelCount, ClientError> {
        let message_id = self.next_message_id();
        let packet = commands::build_channel_count(message_id)?;
        let response = self.request(&packet, message_id)?;
        parse_channel_count(&response).ok_or(ClientError::MalformedResponse)
    }

    pub fn get_channel_audio_metadata(
        &mut self,
        tx_count: u16,
        rx_count: u16,
    ) -> Result<Option<ChannelAudioMetadata>, ClientError> {
        for (count, transmitter) in [(tx_count, true), (rx_count, false)] {
            if count == 0 {
                continue;
            }

            let message_id = self.next_message_id();
            let packet = if transmitter {
                commands::build_transmitters(0, false, message_id)?
            } else {
                commands::build_receivers(0, message_id)?
            };

            if let Ok(response) = self.request(&packet, message_id) {
                if let Some(metadata) = parse_channel_audio_metadata(&response) {
                    return Ok(Some(metadata));
                }
            }
        }

        Ok(None)
    }

    pub fn get_rx_channels(&mut self) -> Result<Vec<RxChannel>, ClientError> {
        let rx_count = self.get_channel_count()?.rx_count;
        Ok(self.get_rx_inventory(rx_count)?.channels)
    }

    pub fn get_rx_inventory(&mut self, rx_count: u16) -> Result<RxInventory, ClientError> {
        let mut channels = Vec::new();
        let mut channel_audio_metadata = None;
        let mut starting_channel = 1;

        loop {
            let message_id = self.next_message_id();
            let packet = commands::build_receivers_from(starting_channel, message_id)?;
            let response = self.request(&packet, message_id)?;
            if starting_channel == 1 {
                channel_audio_metadata = parse_channel_audio_metadata(&response);
            }
            let parsed =
                parse_rx_page(&response, starting_channel).ok_or(ClientError::MalformedResponse)?;
            let continues = parsed.len() == RX_CHANNELS_PER_PAGE as usize || more_pages(&response);
            let Some(last_channel) = parsed.last().map(|channel| channel.number) else {
                break;
            };
            channels.extend(parsed);
            if !continues || channels.len() >= usize::from(rx_count) {
                break;
            }
            starting_channel = last_channel
                .checked_add(1)
                .ok_or(ClientError::MalformedResponse)?;
        }

        if channels.len() != usize::from(rx_count) {
            return Err(ClientError::MalformedResponse);
        }

        Ok(RxInventory {
            channels,
            channel_audio_metadata,
        })
    }

    pub fn get_tx_channels(&mut self) -> Result<Vec<TxChannel>, ClientError> {
        let tx_count = self.get_channel_count()?.tx_count;

        let friendly_names = if tx_count == 0 {
            Vec::new()
        } else {
            let message_id = self.next_message_id();
            let packet = commands::build_transmitter_names(tx_count, message_id)?;
            let response = self.request(&packet, message_id)?;
            parse_tx_friendly_page(&response, 1).ok_or(ClientError::MalformedResponseAt(
                "transmitter friendly names",
            ))?
        };

        let mut channels = Vec::new();
        let mut starting_channel = 1;
        loop {
            let message_id = self.next_message_id();
            let packet = commands::build_transmitters_from(starting_channel, message_id)?;
            let response = self.request(&packet, message_id)?;
            let parsed = parse_tx_info_page(&response, starting_channel)
                .ok_or(ClientError::MalformedResponse)?;
            let declared_rows =
                usize::from(response.get(RESPONSE_HEADER_SIZE + 1).copied().unwrap_or(0));
            let continues = parsed.len() == TX_CHANNELS_PER_PAGE as usize
                || (more_pages(&response) && parsed.len() == declared_rows);
            let Some(last_channel) = parsed.last().map(|channel| channel.number) else {
                break;
            };
            channels.extend(parsed);
            if !continues || channels.len() >= usize::from(tx_count) {
                break;
            }
            starting_channel = last_channel
                .checked_add(1)
                .ok_or(ClientError::MalformedResponse)?;
        }

        if channels.len() > usize::from(tx_count) {
            return Err(ClientError::MalformedResponse);
        }
        if tx_count > 0 && channels.is_empty() {
            return Err(ClientError::MalformedResponseAt(
                "transmitter channels: the device advertises transmitter channels but listed none",
            ));
        }

        for channel in &mut channels {
            if let Some((_, name)) = friendly_names
                .iter()
                .find(|(number, _)| *number == channel.number)
            {
                channel.friendly_name = Some(name.clone());
            }
        }

        Ok(channels)
    }

    fn send_datagram(&mut self, packet: &[u8], address: SocketAddr) -> Result<(), ClientError> {
        self.record_wire_capture("request", address.port(), None, packet)?;
        self.socket.send_to(packet, address)?;
        Ok(())
    }

    fn request(&mut self, packet: &[u8], message_id: u16) -> Result<Vec<u8>, ClientError> {
        self.request_to(packet, message_id, self.device_address)
    }

    fn request_to(
        &mut self,
        packet: &[u8],
        message_id: u16,
        address: SocketAddr,
    ) -> Result<Vec<u8>, ClientError> {
        for _ in 0..self.attempts {
            self.send_datagram(packet, address)?;
            if let Some(response) = self.wait_for_response(message_id)? {
                return Ok(response);
            }
        }
        Err(ClientError::Timeout)
    }

    fn wait_for_response(&mut self, message_id: u16) -> Result<Option<Vec<u8>>, ClientError> {
        let deadline = Instant::now() + self.timeout;
        let mut buffer = [0u8; MAX_UDP_DATAGRAM_BYTES];

        loop {
            let now = Instant::now();
            if now >= deadline {
                return Ok(None);
            }
            self.socket.set_read_timeout(Some(deadline - now))?;

            let (length, source) = match self.socket.recv_from(&mut buffer) {
                Ok(received) => received,
                Err(error)
                    if error.kind() == io::ErrorKind::WouldBlock
                        || error.kind() == io::ErrorKind::TimedOut =>
                {
                    return Ok(None);
                }
                Err(error) => return Err(error.into()),
            };

            if source.ip() != self.device_address.ip() {
                continue;
            }
            let matched = length >= MINIMUM_PACKET_BYTES
                && u16::from_be_bytes([buffer[4], buffer[5]]) == message_id;
            self.record_wire_capture("response", source.port(), Some(matched), &buffer[..length])?;
            if !matched {
                continue;
            }

            return Ok(Some(buffer[..length].to_vec()));
        }
    }
}
