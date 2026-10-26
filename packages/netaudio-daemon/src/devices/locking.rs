use super::capabilities;
use super::operations::ControlError;
use crate::configuration::Configuration;
use anyhow::{Context, Result, ensure};
use netaudio_core::lock::{self, KEY_LENGTH, LockOperation, LockResult};
use rand::RngCore;
use serde_json::Value;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::time::{Instant, timeout_at};

const EXCHANGE_WINDOW: Duration = Duration::from_secs(5);

pub struct Settings {
    key: Option<[u8; KEY_LENGTH]>,
    pub observation_window: Duration,
}

impl Settings {
    pub fn load(configuration: &Configuration) -> Result<Self> {
        let environment = std::env::var("NETAUDIO_DEVICE_LOCK_KEY").ok();
        let key = configuration
            .profile
            .get("device_lock_key")
            .map(|value| value.as_str().context("device_lock_key must be a string"))
            .transpose()?
            .or(environment.as_deref())
            .filter(|value| !value.is_empty());
        let key = key
            .map(|value| {
                ensure!(
                    value.len() == KEY_LENGTH && value.is_ascii(),
                    "device_lock_key must contain exactly {KEY_LENGTH} ASCII characters"
                );
                value
                    .as_bytes()
                    .try_into()
                    .context("invalid device lock key length")
            })
            .transpose()?;
        let observation_seconds = std::env::var("NETAUDIO_LOCK_STATE_TIMEOUT")
            .ok()
            .map(|value| value.parse::<f64>())
            .transpose()
            .context("invalid lock state timeout")?
            .unwrap_or(4.0);
        ensure!(
            observation_seconds.is_finite()
                && observation_seconds > 0.0
                && observation_seconds <= 60.0,
            "lock state timeout must be greater than zero and at most 60 seconds"
        );
        Ok(Self {
            key,
            observation_window: Duration::from_secs_f64(observation_seconds),
        })
    }

    pub fn key(&self) -> Result<&[u8; KEY_LENGTH], ControlError> {
        self.key
            .as_ref()
            .ok_or_else(|| ControlError::Unavailable("device_lock_key not configured".to_owned()))
    }

    pub fn export(&self) -> Result<String, ControlError> {
        let key = self.key()?;

        if !key.iter().all(u8::is_ascii_hexdigit) {
            return Err(ControlError::Invalid(
                "key must be a 32-character hex string".to_owned(),
            ));
        }

        Ok(String::from_utf8_lossy(key).to_ascii_lowercase())
    }
}

#[derive(Clone)]
pub struct Operation {
    pub locking: bool,
    pin: String,
}

impl std::fmt::Debug for Operation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LockOperation")
            .field("locking", &self.locking)
            .finish_non_exhaustive()
    }
}

impl Operation {
    pub fn new(parameters: &Value, locking: bool) -> Result<Self, ControlError> {
        let pin = parameters["pin"]
            .as_str()
            .ok_or_else(|| ControlError::Invalid("pin must be exactly 4 digits".to_owned()))?;
        lock::validate_pin(pin).map_err(|error| ControlError::Invalid(error.to_string()))?;
        Ok(Self {
            locking,
            pin: pin.to_owned(),
        })
    }

    pub fn name(&self) -> &'static str {
        if self.locking {
            "lock_device"
        } else {
            "unlock_device"
        }
    }

    pub fn validate(&self, record: &Value) -> Result<(), ControlError> {
        let availability = capabilities::operation(record, "locking")?;

        if record["online"] != true || !availability.write_permitted {
            return Err(ControlError::Conflict(format!(
                "device locking unavailable: {}",
                availability.reasons.join(", ")
            )));
        }

        if record["requires_managed_control"] == true {
            return Err(ControlError::Conflict(
                "device locking has no verified managed-control operation".to_owned(),
            ));
        }

        Ok(())
    }
}

pub struct Exchange {
    socket: UdpSocket,
    sequence: u16,
    authentication: Vec<u8>,
}

impl Exchange {
    pub async fn prepare(
        local: IpAddr,
        target: Ipv4Addr,
        operation: &Operation,
        key: &[u8],
    ) -> Result<Self> {
        let socket = UdpSocket::bind(SocketAddr::new(local, 0)).await?;
        socket.connect((target, lock::DEVICE_LOCK_PORT)).await?;
        let sequence = (rand::rngs::OsRng.next_u32() as u16).max(1);
        socket
            .send(&lock::build_challenge_request(sequence))
            .await?;
        let deadline = Instant::now() + EXCHANGE_WINDOW;
        let mut buffer = [0_u8; 65_535];
        let nonce = loop {
            let length = timeout_at(deadline, socket.recv(&mut buffer))
                .await
                .context("device lock challenge timed out")??;

            if let Some(nonce) = lock::parse_challenge_response(&buffer[..length], sequence) {
                break nonce;
            }
        };
        let token = lock::compute_token(&operation.pin, &nonce, key)
            .map_err(|error| anyhow::anyhow!("{error}"))?;
        let requested = if operation.locking {
            LockOperation::Lock
        } else {
            LockOperation::Unlock
        };
        Ok(Self {
            socket,
            sequence,
            authentication: lock::build_auth_message(sequence, requested, &token),
        })
    }

    pub async fn send(&self) -> Result<()> {
        self.socket.send(&self.authentication).await?;
        Ok(())
    }

    pub async fn result(&self) -> Result<LockResult> {
        let deadline = Instant::now() + EXCHANGE_WINDOW;
        let mut buffer = [0_u8; 65_535];

        loop {
            let length = timeout_at(deadline, self.socket.recv(&mut buffer))
                .await
                .context(
                    "device lock authentication outcome is unknown; the request was not repeated",
                )??;

            if let Some(result) =
                lock::parse_authentication_response(&buffer[..length], self.sequence)
            {
                return Ok(result);
            }
        }
    }
}

pub fn response_status(result: &Value) -> u16 {
    if result["success"] == true && result["state_verified"] == true {
        200
    } else if result["observation_unavailable"] == true || result["operation_result"].is_null() {
        504
    } else {
        409
    }
}
