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
use crate::protocol::{build_set_device_name, NetaudioError};
use crate::responses::{
    parse_aes67_configured, parse_device_info, parse_device_name, parse_device_settings,
    parse_property_directory, parse_result_code, DeviceInfo, DeviceSettings, PropertyDirectory,
    RESULT_CODE_SUCCESS,
};
use crate::spec::{build_routed_command, IoMode, SpecError, Target};

pub const DEVICE_SETTINGS_PORT: u16 = 8700;
pub const DEVICE_CONTROL_PORT: u16 = 8800;

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
    Protocol(NetaudioError),
    Spec(SpecError),
    Timeout,
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientError::InvalidAddress => formatter.write_str("device address must be IPv4"),
            ClientError::InvalidLength => write!(
                formatter,
                "packet must be at least {MINIMUM_PACKET_BYTES} bytes so it carries a message id"
            ),
            ClientError::Io(error) => write!(formatter, "socket error: {error}"),
            ClientError::MalformedResponse => {
                formatter.write_str("device reply did not parse as the expected response")
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

fn page_count(channel_count: u16, per_page: u16) -> u16 {
    if channel_count == 0 {
        1
    } else {
        channel_count.div_ceil(per_page)
    }
}

fn next_message_id(counter: &mut u16) -> u16 {
    *counter = counter.wrapping_add(1);
    if *counter == 0 {
        *counter = 1;
    }
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
    message_counter: u16,
    timeout: Duration,
    attempts: u32,
    host_mac: Option<[u8; 6]>,
    wire_captures: WireCaptureBuffer,
}

impl Client {
    pub fn new(
        device_ip: IpAddr,
        arc_port: u16,
        timeout: Duration,
        attempts: u32,
    ) -> Result<Client, ClientError> {
        if !device_ip.is_ipv4() {
            return Err(ClientError::InvalidAddress);
        }
        let socket = UdpSocket::bind(("0.0.0.0", 0))?;
        Ok(Client {
            socket,
            device_address: SocketAddr::new(device_ip, arc_port),
            message_counter: 0,
            timeout,
            attempts: attempts.max(1),
            host_mac: crate::netif::discover_host_mac(),
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

    pub fn set_host_mac(&mut self, host_mac: [u8; 6]) {
        self.host_mac = Some(host_mac);
    }

    fn target_address(&self, target: Target) -> SocketAddr {
        let port = match target {
            Target::Arc => self.device_address.port(),
            Target::Control => DEVICE_CONTROL_PORT,
            Target::Settings => DEVICE_SETTINGS_PORT,
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

    pub fn get_rx_channels(&mut self) -> Result<Vec<RxChannel>, ClientError> {
        let rx_count = self.get_channel_count()?.rx_count;
        Ok(self.get_rx_inventory(rx_count)?.channels)
    }

    pub fn get_rx_inventory(&mut self, rx_count: u16) -> Result<RxInventory, ClientError> {
        let pages = page_count(rx_count, RX_CHANNELS_PER_PAGE);
        let mut channels = Vec::new();
        let mut channel_audio_metadata = None;

        for page in 0..pages {
            let message_id = self.next_message_id();
            let packet = commands::build_receivers(page, message_id)?;
            let response = self.request(&packet, message_id)?;
            if page == 0 {
                channel_audio_metadata = parse_channel_audio_metadata(&response);
            }
            let starting_channel = page * RX_CHANNELS_PER_PAGE + 1;
            let parsed =
                parse_rx_page(&response, starting_channel).ok_or(ClientError::MalformedResponse)?;
            let complete = parsed.len() == RX_CHANNELS_PER_PAGE as usize;
            channels.extend(parsed);
            if !complete {
                break;
            }
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
        let pages = page_count(tx_count, TX_CHANNELS_PER_PAGE);

        let friendly_names = if tx_count == 0 {
            Vec::new()
        } else {
            let message_id = self.next_message_id();
            let packet = commands::build_transmitter_names(tx_count, message_id)?;
            let response = self.request(&packet, message_id)?;
            parse_tx_friendly_page(&response, 1).ok_or(ClientError::MalformedResponse)?
        };

        let mut channels = Vec::new();
        for page in 0..pages {
            let message_id = self.next_message_id();
            let packet = commands::build_transmitters(page, false, message_id)?;
            let response = self.request(&packet, message_id)?;
            let starting_channel = page * TX_CHANNELS_PER_PAGE + 1;
            let parsed = parse_tx_info_page(&response, starting_channel)
                .ok_or(ClientError::MalformedResponse)?;
            let complete = parsed.len() == TX_CHANNELS_PER_PAGE as usize;
            channels.extend(parsed);
            if !complete {
                break;
            }
        }

        if channels.len() > usize::from(tx_count) {
            return Err(ClientError::MalformedResponse);
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;
    use std::sync::mpsc;
    use std::thread;

    struct FakeDevice {
        socket: UdpSocket,
    }

    impl FakeDevice {
        fn new() -> FakeDevice {
            let socket = UdpSocket::bind(("127.0.0.1", 0)).unwrap();
            FakeDevice { socket }
        }

        fn port(&self) -> u16 {
            self.socket.local_addr().unwrap().port()
        }

        fn receive(&self) -> (Vec<u8>, SocketAddr) {
            let mut buffer = [0u8; 2048];
            let (length, source) = self.socket.recv_from(&mut buffer).unwrap();
            (buffer[..length].to_vec(), source)
        }

        fn reply(&self, request: &[u8], destination: SocketAddr) {
            self.reply_with_arc_body(
                request,
                destination,
                crate::protocol::RESULT_CODE_SUCCESS,
                &[],
            );
        }

        fn reply_with_arc_body(
            &self,
            request: &[u8],
            destination: SocketAddr,
            result_code: u16,
            body: &[u8],
        ) {
            let response_length = u16::try_from(10 + body.len()).unwrap();
            let mut response = Vec::with_capacity(usize::from(response_length));
            response.extend_from_slice(&request[0..2]);
            response.extend_from_slice(&response_length.to_be_bytes());
            response.extend_from_slice(&request[4..6]);
            response.extend_from_slice(&request[6..8]);
            response.extend_from_slice(&result_code.to_be_bytes());
            response.extend_from_slice(body);
            self.socket.send_to(&response, destination).unwrap();
        }
    }

    fn test_client(port: u16) -> Client {
        Client::new(
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            port,
            Duration::from_millis(200),
            1,
        )
        .unwrap()
    }

    #[test]
    fn set_device_name_round_trip() {
        let device = FakeDevice::new();
        let mut client = test_client(device.port());

        let (sender, receiver) = mpsc::channel();
        let device_thread = thread::spawn(move || {
            let (request, source) = device.receive();
            device.reply(&request, source);
            sender.send(request).unwrap();
        });

        let response = client.set_device_name("Studio-AVIO").unwrap();
        let request = receiver.recv().unwrap();
        device_thread.join().unwrap();

        let expected = build_set_device_name("Studio-AVIO", 1).unwrap();
        assert_eq!(request, expected);
        assert_eq!(&response[4..6], &[0x00, 0x01]);
    }

    #[test]
    fn set_device_name_rejects_failure_acknowledgement() {
        let device = FakeDevice::new();
        let mut client = test_client(device.port());

        let device_thread = thread::spawn(move || {
            let (request, source) = device.receive();
            device.reply_with_arc_body(&request, source, 0x0600, &[]);
        });

        assert!(matches!(
            client.set_device_name("Studio-AVIO"),
            Err(ClientError::MalformedResponse)
        ));
        device_thread.join().unwrap();
    }

    #[test]
    fn construction_rejects_ipv6_addresses() {
        assert!(matches!(
            Client::new("::1".parse().unwrap(), 4440, Duration::from_millis(1), 1,),
            Err(ClientError::InvalidAddress)
        ));
    }

    #[test]
    fn execute_arc_command_stamps_transaction_id_and_matches_response() {
        let device = FakeDevice::new();
        let mut client = test_client(device.port());

        let (sender, receiver) = mpsc::channel();
        let device_thread = thread::spawn(move || {
            let (request, source) = device.receive();
            device.reply(&request, source);
            sender.send(request).unwrap();
        });

        let spec = "{\"command\":\"set_channel_name\",\"channel_type\":\"rx\",\"channel_number\":1,\"name\":\"label\"}";
        let response = client.execute(spec).unwrap();
        let request = receiver.recv().unwrap();
        device_thread.join().unwrap();

        let expected = crate::commands::build_set_channel_name(
            crate::commands::ChannelType::Rx,
            1,
            "label",
            1,
        )
        .unwrap();
        assert_eq!(request, expected);
        assert_eq!(&response[4..6], &[0x00, 0x01]);
    }

    #[test]
    fn execute_fire_command_does_not_wait_for_response() {
        let mut client = Client::new(
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            9,
            Duration::from_millis(5000),
            1,
        )
        .unwrap();
        let started = Instant::now();
        let response = client
            .execute("{\"command\":\"identify\",\"sequence\":1}")
            .unwrap();
        assert!(response.is_empty());
        assert!(started.elapsed() < Duration::from_millis(500));
    }

    #[test]
    fn transaction_id_increments_per_request() {
        let device = FakeDevice::new();
        let mut client = test_client(device.port());

        let device_thread = thread::spawn(move || {
            for _ in 0..2 {
                let (request, source) = device.receive();
                device.reply(&request, source);
            }
        });

        client.set_device_name("First").unwrap();
        client.set_device_name("Second").unwrap();
        device_thread.join().unwrap();

        assert_eq!(client.message_counter, 2);
    }

    #[test]
    fn timeout_when_device_does_not_reply() {
        let device = FakeDevice::new();
        let mut client = test_client(device.port());

        let started = Instant::now();
        let result = client.set_device_name("Studio-AVIO");

        assert!(matches!(result, Err(ClientError::Timeout)));
        assert!(started.elapsed() >= Duration::from_millis(200));
    }

    #[test]
    fn raw_request_that_expects_a_reply_reports_timeout_instead_of_an_empty_buffer() {
        let device = FakeDevice::new();
        let mut client = test_client(device.port());
        let packet = crate::commands::build_device_log_export([0x11; 6], 0x0083).unwrap();

        let result = client.request_raw(&packet, device.port());

        assert!(matches!(result, Err(ClientError::Timeout)));
    }

    #[test]
    fn raw_fire_request_keeps_empty_response_semantics() {
        let device = FakeDevice::new();
        let mut client = test_client(device.port());
        let packet = crate::commands::build_device_log_export([0x11; 6], 0x0083).unwrap();

        let response = fire_repeated(1, 0, || client.send_raw(&packet, device.port())).unwrap();

        assert!(response.is_empty());
    }

    #[test]
    fn raw_packets_shorter_than_a_message_id_are_rejected_before_sending() {
        let device = FakeDevice::new();
        let mut client = test_client(device.port());

        assert!(matches!(
            client.request_raw(&[0xFF; 5], device.port()),
            Err(ClientError::InvalidLength)
        ));
        assert!(matches!(
            client.send_raw(&[], device.port()),
            Err(ClientError::InvalidLength)
        ));
    }

    #[test]
    fn execute_assigns_message_ids_to_conmon_commands_that_omit_them() {
        let device = FakeDevice::new();
        let mut client = test_client(device.port());
        client.set_host_mac([0x11; 6]);

        let arc = client
            .prepare_command("{\"command\":\"device_info\"}")
            .unwrap();
        assert_eq!(arc.message_id, 1);
        assert_eq!(&arc.packet[4..6], &1u16.to_be_bytes());

        let prepared = client.prepare_command("{\"command\":\"reboot\"}").unwrap();
        assert_eq!(prepared.message_id, 2);
        assert_eq!(&prepared.packet[4..6], &2u16.to_be_bytes());
        assert_eq!(
            prepared.packet,
            crate::commands::build_reboot([0x11; 6], 2).unwrap()
        );

        let explicit = client
            .prepare_command("{\"command\":\"reboot\",\"message_id\":513}")
            .unwrap();
        assert_eq!(explicit.message_id, 513);
        assert_eq!(&explicit.packet[4..6], &513u16.to_be_bytes());
        assert_eq!(client.message_counter, 2);
    }

    #[test]
    fn retries_resend_the_packet() {
        let device = FakeDevice::new();
        let mut client = Client::new(
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            device.port(),
            Duration::from_millis(100),
            3,
        )
        .unwrap();

        let device_thread = thread::spawn(move || {
            let (first, _) = device.receive();
            let (second, source) = device.receive();
            assert_eq!(first, second);
            device.reply(&second, source);
        });

        client.set_device_name("Studio-AVIO").unwrap();
        device_thread.join().unwrap();
    }

    #[test]
    fn ignores_mismatched_transaction_id() {
        let device = FakeDevice::new();
        let mut client = test_client(device.port());

        let device_thread = thread::spawn(move || {
            let (request, source) = device.receive();
            let mut bogus = request.clone();
            bogus[4] = 0xDE;
            bogus[5] = 0xAD;
            device.reply(&bogus, source);
            device.reply(&request, source);
        });

        let response = client.set_device_name("Studio-AVIO").unwrap();
        device_thread.join().unwrap();

        assert_eq!(&response[4..6], &[0x00, 0x01]);
    }

    #[test]
    fn wire_captures_preserve_requests_and_all_device_responses() {
        let device = FakeDevice::new();
        let mut client = test_client(device.port());

        let device_thread = thread::spawn(move || {
            let (request, source) = device.receive();
            let mut bogus = request.clone();
            bogus[4] = 0xDE;
            bogus[5] = 0xAD;
            device.reply(&bogus, source);
            device.reply(&request, source);
        });

        client.set_device_name("Studio-AVIO").unwrap();
        device_thread.join().unwrap();

        let captures = client.wire_captures();
        assert_eq!(captures.len(), 3);
        assert_eq!(captures[0].direction, "request");
        assert_eq!(captures[0].matched, None);
        assert_eq!(captures[1].direction, "response");
        assert_eq!(captures[1].matched, Some(false));
        assert_eq!(captures[2].direction, "response");
        assert_eq!(captures[2].matched, Some(true));
        assert_eq!(&captures[2].payload_hex[8..12], "0001");

        client.clear_wire_captures();
        assert!(client.wire_captures().is_empty());
    }

    #[test]
    fn wire_capture_does_not_truncate_datagrams_larger_than_two_kibibytes() {
        let device = FakeDevice::new();
        let mut client = test_client(device.port());

        let device_thread = thread::spawn(move || {
            let (request, source) = device.receive();
            let mut response = vec![0xA5; 4_096];
            response[0..4].copy_from_slice(&[0x27, 0xFF, 0x10, 0x00]);
            response[4..6].copy_from_slice(&request[4..6]);
            response[6..8].copy_from_slice(&request[6..8]);
            response[8..10].copy_from_slice(&crate::protocol::RESULT_CODE_SUCCESS.to_be_bytes());
            device.socket.send_to(&response, source).unwrap();
        });

        let response = client.set_device_name("Studio-AVIO").unwrap();
        device_thread.join().unwrap();

        assert_eq!(response.len(), 4_096);
        let capture = client.wire_captures().pop().unwrap();
        assert_eq!(capture.direction, "response");
        assert_eq!(capture.payload_hex.len(), 8_192);
        assert!(capture.payload_hex.ends_with("a5a5"));
    }

    #[test]
    fn rx_inventory_rejects_page_count_mismatch() {
        let device = FakeDevice::new();
        let mut client = test_client(device.port());

        let device_thread = thread::spawn(move || {
            let (count_request, count_source) = device.receive();
            device.reply_with_arc_body(
                &count_request,
                count_source,
                crate::protocol::RESULT_CODE_SUCCESS,
                &[0, 0, 0, 0, 0, 2],
            );

            let (page_request, page_source) = device.receive();
            let mut body = vec![1, 1];
            let mut record = [0u8; crate::parser::RX_RECORD_SIZE];
            record[0..2].copy_from_slice(&1u16.to_be_bytes());
            record[10..12].copy_from_slice(&32u16.to_be_bytes());
            body.extend_from_slice(&record);
            body.extend_from_slice(b"rx\0");
            device.reply_with_arc_body(
                &page_request,
                page_source,
                crate::protocol::RESULT_CODE_SUCCESS,
                &body,
            );
        });

        assert!(matches!(
            client.get_rx_channels(),
            Err(ClientError::MalformedResponse)
        ));
        device_thread.join().unwrap();
    }

    #[test]
    fn tx_inventory_uses_primary_metadata_group_with_larger_capacity() {
        let device = FakeDevice::new();
        let mut client = test_client(device.port());

        let device_thread = thread::spawn(move || {
            let (count_request, count_source) = device.receive();
            device.reply_with_arc_body(
                &count_request,
                count_source,
                crate::protocol::RESULT_CODE_SUCCESS,
                &[0, 0, 0, 64, 0, 1],
            );

            let (friendly_request, friendly_source) = device.receive();
            assert_eq!(
                &friendly_request[10..],
                &[0x00, 0x01, 0x00, 0x01, 0x00, 0x40]
            );
            device.reply_with_arc_body(
                &friendly_request,
                friendly_source,
                crate::protocol::RESULT_CODE_SUCCESS,
                &[0, 0],
            );

            let (page_request, page_source) = device.receive();
            let mut body = vec![3, 3];
            for (channel_number, metadata_pointer, name_pointer) in
                [(1u16, 36u16, 52u16), (2, 36, 54), (3, 56, 72)]
            {
                let mut record = [0u8; crate::parser::TX_RECORD_SIZE];
                record[0..2].copy_from_slice(&channel_number.to_be_bytes());
                record[4..6].copy_from_slice(&metadata_pointer.to_be_bytes());
                record[6..8].copy_from_slice(&name_pointer.to_be_bytes());
                body.extend_from_slice(&record);
            }
            body.extend_from_slice(&[0, 0, 0xBB, 0x80, 2, 2, 0, 24, 0, 0, 0, 0, 0, 0, 0, 4]);
            body.extend_from_slice(b"a\0b\0");
            body.extend_from_slice(&[0, 0, 0xBB, 0x80, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
            body.extend_from_slice(b"c\0");
            device.reply_with_arc_body(
                &page_request,
                page_source,
                crate::protocol::RESULT_CODE_MORE_PAGES,
                &body,
            );
        });

        assert_eq!(
            client.get_tx_channels().unwrap(),
            vec![
                TxChannel {
                    number: 1,
                    name: Some("a".to_string()),
                    friendly_name: None,
                },
                TxChannel {
                    number: 2,
                    name: Some("b".to_string()),
                    friendly_name: None,
                },
            ]
        );
        device_thread.join().unwrap();
    }

    #[test]
    fn invalid_name_fails_without_sending() {
        let device = FakeDevice::new();
        let mut client = test_client(device.port());

        let result = client.set_device_name("-bad");
        assert!(matches!(
            result,
            Err(ClientError::Protocol(NetaudioError::NameInvalidHyphen))
        ));

        device
            .socket
            .set_read_timeout(Some(Duration::from_millis(50)))
            .unwrap();
        let mut buffer = [0u8; 64];
        assert!(device.socket.recv_from(&mut buffer).is_err());
    }
}
