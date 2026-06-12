use std::io;
use std::net::{IpAddr, SocketAddr, UdpSocket};
use std::thread;
use std::time::{Duration, Instant};

use crate::parser::{
    parse_channel_count, parse_rx_page, parse_tx_friendly_page, parse_tx_info_page, ChannelCount,
    RxChannel, TxChannel, RX_CHANNELS_PER_PAGE, TX_CHANNELS_PER_PAGE,
};
use crate::commands;
use crate::protocol::{build_set_device_name, NetaudioError};
use crate::responses::{
    parse_aes67_configured, parse_device_info, parse_device_name, parse_device_settings, DeviceInfo,
    DeviceSettings,
};
use crate::spec::{build_routed_command, IoMode, SpecError, Target};

pub const DEVICE_SETTINGS_PORT: u16 = 8700;
pub const DEVICE_CONTROL_PORT: u16 = 8800;

#[derive(Debug)]
pub enum ClientError {
    Protocol(NetaudioError),
    Io(io::Error),
    Timeout,
    MalformedResponse,
    Spec(SpecError),
}

impl From<SpecError> for ClientError {
    fn from(error: SpecError) -> Self {
        ClientError::Spec(error)
    }
}

fn page_count(channel_count: u16, per_page: u16) -> u16 {
    if channel_count == 0 {
        1
    } else {
        channel_count.div_ceil(per_page)
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

pub struct Client {
    socket: UdpSocket,
    device_address: SocketAddr,
    transaction_counter: u16,
    timeout: Duration,
    attempts: u32,
    host_mac: [u8; 6],
}

impl Client {
    pub fn new(
        device_ip: IpAddr,
        arc_port: u16,
        timeout: Duration,
        attempts: u32,
    ) -> Result<Client, ClientError> {
        let socket = UdpSocket::bind(("0.0.0.0", 0))?;
        Ok(Client {
            socket,
            device_address: SocketAddr::new(device_ip, arc_port),
            transaction_counter: 0,
            timeout,
            attempts: attempts.max(1),
            host_mac: crate::netif::discover_host_mac().unwrap_or([0u8; 6]),
        })
    }

    pub fn set_host_mac(&mut self, host_mac: [u8; 6]) {
        self.host_mac = host_mac;
    }

    fn target_address(&self, target: Target) -> SocketAddr {
        let port = match target {
            Target::Arc => self.device_address.port(),
            Target::Settings => DEVICE_SETTINGS_PORT,
            Target::Control => DEVICE_CONTROL_PORT,
        };
        SocketAddr::new(self.device_address.ip(), port)
    }

    pub fn execute(&mut self, json: &str) -> Result<Vec<u8>, ClientError> {
        let routed = build_routed_command(json, self.host_mac)?;
        let address = self.target_address(routed.target);

        match routed.io {
            IoMode::StampedRequest => {
                let transaction_id = self.next_transaction_id();
                let mut packet = routed.packet;
                packet[4..6].copy_from_slice(&transaction_id.to_be_bytes());
                self.request_to(&packet, transaction_id, address)
            }
            IoMode::Request => {
                let match_id = u16::from_be_bytes([routed.packet[4], routed.packet[5]]);
                match self.request_to(&routed.packet, match_id, address) {
                    Ok(response) => Ok(response),
                    Err(ClientError::Timeout) => Ok(Vec::new()),
                    Err(error) => Err(error),
                }
            }
            IoMode::Fire { repeat, interval_ms } => {
                for attempt in 0..repeat {
                    self.socket.send_to(&routed.packet, address)?;
                    if attempt + 1 < repeat && interval_ms > 0 {
                        thread::sleep(Duration::from_millis(interval_ms));
                    }
                }
                Ok(Vec::new())
            }
        }
    }

    fn next_transaction_id(&mut self) -> u16 {
        self.transaction_counter = self.transaction_counter.wrapping_add(1);
        self.transaction_counter
    }

    pub fn set_device_name(&mut self, name: &str) -> Result<Vec<u8>, ClientError> {
        let transaction_id = self.next_transaction_id();
        let packet = build_set_device_name(name, transaction_id)?;
        self.request(&packet, transaction_id)
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
        let transaction_id = self.next_transaction_id();
        let packet = commands::build_device_name(transaction_id);
        let response = self.request(&packet, transaction_id)?;
        Ok(parse_device_name(&response))
    }

    pub fn request_raw(
        &self,
        packet: &[u8],
        target_port: u16,
        expect_response: bool,
        repeat: u32,
        interval_ms: u64,
    ) -> Result<Vec<u8>, ClientError> {
        if packet.len() < 6 {
            return Err(ClientError::MalformedResponse);
        }
        let address = SocketAddr::new(self.device_address.ip(), target_port);
        let match_id = u16::from_be_bytes([packet[4], packet[5]]);

        if expect_response {
            return match self.request_to(packet, match_id, address) {
                Ok(response) => Ok(response),
                Err(ClientError::Timeout) => Ok(Vec::new()),
                Err(error) => Err(error),
            };
        }

        for attempt in 0..repeat.max(1) {
            self.socket.send_to(packet, address)?;
            if attempt + 1 < repeat.max(1) && interval_ms > 0 {
                thread::sleep(Duration::from_millis(interval_ms));
            }
        }
        Ok(Vec::new())
    }

    pub fn get_device_info(&mut self) -> Result<DeviceInfo, ClientError> {
        let transaction_id = self.next_transaction_id();
        let packet = commands::build_device_info(transaction_id);
        let response = self.request(&packet, transaction_id)?;
        parse_device_info(&response).ok_or(ClientError::MalformedResponse)
    }

    pub fn get_device_settings(&mut self) -> Result<DeviceSettings, ClientError> {
        let transaction_id = self.next_transaction_id();
        let packet = commands::build_device_settings(transaction_id);
        let response = self.request(&packet, transaction_id)?;
        parse_device_settings(&response).ok_or(ClientError::MalformedResponse)
    }

    pub fn get_aes67_configured(&mut self) -> Result<Option<bool>, ClientError> {
        let transaction_id = self.next_transaction_id();
        let packet = commands::build_query_latency_config(transaction_id);
        let response = self.request(&packet, transaction_id)?;
        Ok(parse_aes67_configured(&response))
    }

    pub fn get_channel_count(&mut self) -> Result<ChannelCount, ClientError> {
        let transaction_id = self.next_transaction_id();
        let packet = commands::build_channel_count(transaction_id);
        let response = self.request(&packet, transaction_id)?;
        parse_channel_count(&response).ok_or(ClientError::MalformedResponse)
    }

    pub fn get_rx_channels(&mut self) -> Result<Vec<RxChannel>, ClientError> {
        let rx_count = self.get_channel_count()?.rx_count;
        let pages = page_count(rx_count, RX_CHANNELS_PER_PAGE);
        let mut channels = Vec::new();

        for page in 0..pages {
            let transaction_id = self.next_transaction_id();
            let packet = commands::build_receivers(page, transaction_id);
            let response = self.request(&packet, transaction_id)?;
            let starting_channel = page * RX_CHANNELS_PER_PAGE + 1;
            let parsed = parse_rx_page(&response, starting_channel);
            let complete = parsed.len() == RX_CHANNELS_PER_PAGE as usize;
            channels.extend(parsed);
            if !complete {
                break;
            }
        }

        Ok(channels)
    }

    pub fn get_tx_channels(&mut self) -> Result<Vec<TxChannel>, ClientError> {
        let tx_count = self.get_channel_count()?.tx_count;
        let pages = page_count(tx_count, TX_CHANNELS_PER_PAGE);

        let mut friendly_names = Vec::new();
        for page in 0..pages {
            let transaction_id = self.next_transaction_id();
            let packet = commands::build_transmitters(page, true, transaction_id);
            let response = self.request(&packet, transaction_id)?;
            let parsed = parse_tx_friendly_page(&response);
            let complete = parsed.len() == TX_CHANNELS_PER_PAGE as usize;
            friendly_names.extend(parsed);
            if !complete {
                break;
            }
        }

        let mut channels = Vec::new();
        for page in 0..pages {
            let transaction_id = self.next_transaction_id();
            let packet = commands::build_transmitters(page, false, transaction_id);
            let response = self.request(&packet, transaction_id)?;
            let starting_channel = page * TX_CHANNELS_PER_PAGE + 1;
            let parsed = parse_tx_info_page(&response, starting_channel);
            let complete = parsed.len() == TX_CHANNELS_PER_PAGE as usize;
            channels.extend(parsed);
            if !complete {
                break;
            }
        }

        for channel in &mut channels {
            if let Some((_, name)) = friendly_names.iter().find(|(number, _)| *number == channel.number) {
                channel.friendly_name = Some(name.clone());
            }
        }

        Ok(channels)
    }

    fn request(&self, packet: &[u8], transaction_id: u16) -> Result<Vec<u8>, ClientError> {
        self.request_to(packet, transaction_id, self.device_address)
    }

    fn request_to(
        &self,
        packet: &[u8],
        match_id: u16,
        address: SocketAddr,
    ) -> Result<Vec<u8>, ClientError> {
        for _ in 0..self.attempts {
            self.socket.send_to(packet, address)?;
            if let Some(response) = self.wait_for_response(match_id)? {
                return Ok(response);
            }
        }
        Err(ClientError::Timeout)
    }

    fn wait_for_response(&self, transaction_id: u16) -> Result<Option<Vec<u8>>, ClientError> {
        let deadline = Instant::now() + self.timeout;
        let mut buffer = [0u8; 2048];

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
            if length < 6 {
                continue;
            }
            if u16::from_be_bytes([buffer[4], buffer[5]]) != transaction_id {
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
            let mut response = vec![0x27, 0xFF, 0x00, 0x0A];
            response.extend_from_slice(&request[4..6]);
            response.extend_from_slice(&request[6..8]);
            response.extend_from_slice(&[0x00, 0x00]);
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
        let response = client.execute("{\"command\":\"identify\"}").unwrap();
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

        assert_eq!(client.transaction_counter, 2);
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
    fn invalid_name_fails_without_sending() {
        let device = FakeDevice::new();
        let mut client = test_client(device.port());

        let result = client.set_device_name("-bad");
        assert!(matches!(
            result,
            Err(ClientError::Protocol(NetaudioError::NameInvalidHyphen))
        ));

        device.socket.set_read_timeout(Some(Duration::from_millis(50))).unwrap();
        let mut buffer = [0u8; 64];
        assert!(device.socket.recv_from(&mut buffer).is_err());
    }
}
