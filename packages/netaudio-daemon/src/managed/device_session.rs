use super::{
    Credential, Error, Limits,
    control::FreshTarget,
    controller::{Controller, Exchange, ExchangeError, Session},
    registry::Source,
};
use crate::devices::operations::ControlError;
use netaudio_core::{
    device_identity::{DeviceIdentityRequest, managed_device_id, normalize},
    managed_api::{
        inventory::{Observation, primary_mac},
        models::Device,
    },
    managed_session::ManagedOperation,
};
use serde_json::{Value, json};
use std::{net::IpAddr, num::NonZeroU16};

pub(super) struct DeviceSession {
    session: Session,
    credential: Credential,
    pub device_id: String,
    domain: String,
    pub host_mac: [u8; 6],
    message_id: u16,
}

impl DeviceSession {
    pub async fn open(source: &Source, fresh: &FreshTarget) -> Result<Self, ControlError> {
        let (device_id, domain) = identities(fresh)?;
        Self::connect(source, device_id, domain)
            .await
            .map_err(|failure| ControlError::ObservationUnavailable(failure.to_string()))
    }

    pub async fn connect(
        source: &Source,
        device_id: String,
        domain: String,
    ) -> Result<Self, ExchangeError> {
        let (session, credential, host_mac) =
            tokio::time::timeout(Limits::default().timeout, connection(source))
                .await
                .unwrap_or(Err(Error::Deadline))
                .map_err(|error| ExchangeError {
                    error,
                    request_sent: Some(false),
                })?;
        Ok(Self {
            session,
            credential,
            device_id,
            domain,
            host_mac,
            message_id: 0,
        })
    }

    pub async fn exchange(
        &mut self,
        operation: ManagedOperation,
    ) -> Result<Exchange, ExchangeError> {
        self.session
            .exchange_with_receipt(&self.credential, operation, Some(self.domain.clone()))
            .await
    }

    pub async fn command(&mut self, specification: Value) -> Result<Exchange, ExchangeError> {
        self.message_id = self.message_id.checked_add(1).ok_or(ExchangeError {
            error: Error::Conflict("Managed command identifiers exhausted"),
            request_sent: Some(false),
        })?;
        let command = netaudio_core::spec::build_managed_command(
            netaudio_core::spec::ManagedCommandRequest {
                specification,
                host_mac: Some(hex::encode(self.host_mac)),
                message_id: self.message_id,
            },
        )
        .map_err(|error| ExchangeError {
            error: Error::Session(error.to_string()),
            request_sent: Some(false),
        })?;
        let operation = match command.transport {
            netaudio_core::spec::Target::Arc => ManagedOperation::Arc {
                device_id: self.device_id.clone(),
                packet: command.packet,
            },
            netaudio_core::spec::Target::Settings => ManagedOperation::Settings {
                device_id: self.device_id.clone(),
                packet: command.packet,
                response_opcode: command.response_opcode.ok_or(ExchangeError {
                    error: Error::Invalid("Managed settings command has no response opcode"),
                    request_sent: Some(false),
                })?,
            },
            _ => {
                return Err(ExchangeError {
                    error: Error::Invalid("Unsupported managed command transport"),
                    request_sent: Some(false),
                });
            }
        };
        self.exchange(operation).await
    }
}

pub(super) fn identities(fresh: &FreshTarget) -> Result<(String, String), ControlError> {
    target_identity(&fresh.target, &fresh.inventory.devices)
}

pub(super) fn target_identity(
    target: &Observation,
    observations: &[Observation],
) -> Result<(String, String), ControlError> {
    let domain = target
        .domain_id
        .as_deref()
        .and_then(|domain| normalize(DeviceIdentityRequest::ManagedDomain(json!(domain))))
        .ok_or_else(|| {
            ControlError::Conflict("Managed domain has no supported Controller identity".into())
        })?;
    let identifier = controller_identity(&target.device).ok_or_else(|| {
        ControlError::Conflict("Managed device has no supported primary Controller identity".into())
    })?;
    let ambiguous = observations.iter().any(|other| {
        other.device.id != target.device.id
            && other
                .domain_id
                .as_ref()
                .and_then(|domain| normalize(DeviceIdentityRequest::ManagedDomain(json!(domain))))
                .as_deref()
                == Some(&domain)
            && !other
                .device
                .connection
                .as_ref()
                .and_then(|connection| connection.state.as_deref())
                .is_some_and(|state| state.eq_ignore_ascii_case("DISCONNECTED"))
            && controller_identity(&other.device).as_deref() == Some(&identifier)
    });
    if ambiguous {
        return Err(ControlError::Conflict(
            "Managed Controller device identity is ambiguous".into(),
        ));
    }
    Ok((identifier, domain))
}

pub(super) fn controller_identity(device: &Device) -> Option<String> {
    managed_device_id(&device.id).or_else(|| {
        normalize(DeviceIdentityRequest::ManagedInventory(json!(device.id)))?;
        normalize(DeviceIdentityRequest::ManagedPrimary(json!(primary_mac(
            device
        )?)))
    })
}

async fn connection(source: &Source) -> Result<(Session, Credential, [u8; 6]), Error> {
    let configuration = &source.configuration;
    let endpoint = super::client::endpoint(
        configuration
            .url
            .as_deref()
            .ok_or(Error::Invalid("DDM URL is missing"))?,
    )?;
    let controller = Controller::new(
        endpoint
            .host_str()
            .ok_or(Error::Invalid("DDM hostname is missing"))?,
        NonZeroU16::new(netaudio_core::controller_api::AUTH_PORT).expect("nonzero Controller port"),
        source.transport.clone().ok_or(Error::Transport)?,
        Limits::default(),
    )?;
    let endpoints = controller.endpoints().await?;
    let session = controller.connect(endpoints.service_port).await?;
    let IpAddr::V4(address) = session.notification_address()?.ip() else {
        return Err(Error::Transport);
    };
    let host_mac = netaudio_core::netif::host_mac_for_ipv4(address).ok_or(Error::Conflict(
        "The Controller connection has no local hardware address",
    ))?;
    let credential = source
        .credential
        .clone()
        .ok_or(Error::Credential("DDM credential is unavailable"))?;
    Ok((session, credential, host_mac))
}
