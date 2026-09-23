//! Transport-independent Controller session and operation state.

use std::{collections::BTreeMap, net::Ipv4Addr, num::NonZeroU16};

use serde::{Deserialize, Serialize};

use crate::{dapi, device_identity, protocol};

#[derive(Clone, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ManagedOperation {
    MonitorSignals,
    Identify {
        device_id: String,
        host_mac: [u8; 6],
    },
    Arc {
        device_id: String,
        packet: Vec<u8>,
    },
    Settings {
        device_id: String,
        packet: Vec<u8>,
        response_opcode: u16,
    },
    Reboot {
        device_id: String,
        host_mac: [u8; 6],
    },
}

impl ManagedOperation {
    fn device_id(&self) -> &str {
        match self {
            Self::MonitorSignals => "",
            Self::Identify { device_id, .. }
            | Self::Arc { device_id, .. }
            | Self::Settings { device_id, .. }
            | Self::Reboot { device_id, .. } => device_id,
        }
    }

    fn validate(&self) -> Result<String, String> {
        if matches!(self, Self::MonitorSignals) {
            return Ok(String::new());
        }

        let target = device_identity::managed_device_id(self.device_id())
            .ok_or("invalid managed device identifier")?;

        match self {
            Self::Arc { packet, .. } if dapi::parse_arc_request_header(packet).is_none() => {
                return Err("invalid or unsupported ARC request packet".into());
            }
            Self::Settings { packet, .. }
                if dapi::build_settings_request(0, 1, packet).is_none() =>
            {
                return Err("invalid or unsupported settings request packet".into());
            }
            _ => {}
        }

        Ok(target)
    }
}

#[derive(Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "schema", schemars(extend("required" = ["domain_id", "expected_domain_id", "operation", "settings", "local_ipv4", "notification_port", "targets", "wrapper_id", "target", "sent", "frame"])))]
pub struct ManagedSessionState {
    #[serde(deserialize_with = "Option::deserialize")]
    domain_id: Option<String>,
    #[serde(deserialize_with = "Option::deserialize")]
    expected_domain_id: Option<String>,
    local_ipv4: Ipv4Addr,
    notification_port: NonZeroU16,
    targets: BTreeMap<String, u16>,
    wrapper_id: u16,
    #[serde(deserialize_with = "Option::deserialize")]
    operation: Option<ManagedOperation>,
    target: String,
    sent: bool,
    #[serde(deserialize_with = "Option::deserialize")]
    settings: Option<dapi::SettingsExchange>,
    frame: Vec<u8>,
}

#[derive(Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum ManagedSessionRequest {
    Begin {
        state: Option<ManagedSessionState>,
        credential: String,
        local_ipv4: Ipv4Addr,
        notification_port: NonZeroU16,
        expected_domain_id: Option<String>,
        operation: ManagedOperation,
    },
    Receive {
        state: ManagedSessionState,
        data: Vec<u8>,
    },
}

#[derive(Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ManagedSessionStep {
    pub state: ManagedSessionState,
    pub outgoing: Vec<Vec<u8>>,
    pub receive_bytes: usize,
    pub complete: bool,
    pub packet_hex: Option<String>,
    pub signal_presence: Option<dapi::SignalPresencePublication>,
}

impl ManagedSessionState {
    fn receive_bytes(&self) -> Result<usize, String> {
        if self.frame.len() < dapi::FRAME_HEADER_BYTES {
            return Ok(dapi::FRAME_HEADER_BYTES - self.frame.len());
        }

        let header = dapi::parse_frame_header(&self.frame[..dapi::FRAME_HEADER_BYTES])
            .filter(|header| header.server_to_client)
            .ok_or("invalid or unsupported Controller frame header")?;
        (dapi::FRAME_HEADER_BYTES + header.payload_length)
            .checked_sub(self.frame.len())
            .ok_or_else(|| "Controller frame exceeds its declared length".into())
    }

    fn send_operation(&mut self, outgoing: &mut Vec<Vec<u8>>) -> Result<(), String> {
        if self.sent || self.domain_id.is_none() {
            return Ok(());
        }

        if matches!(self.operation, Some(ManagedOperation::MonitorSignals)) {
            self.sent = true;
            return Ok(());
        }

        let Some(selector) = self.targets.get(&self.target).copied() else {
            return Ok(());
        };
        let Some(operation) = &self.operation else {
            return Ok(());
        };
        let wrapper = dapi::next_wrapper_id(self.wrapper_id);
        let packet = match operation {
            ManagedOperation::MonitorSignals => {
                unreachable!("monitoring does not issue device commands")
            }
            ManagedOperation::Identify { host_mac, .. } => dapi::build_identify(
                selector,
                wrapper,
                protocol::allocate_message_id(),
                *host_mac,
            ),
            ManagedOperation::Arc { packet, .. } => {
                dapi::build_arc_request(selector, wrapper, packet)
            }
            ManagedOperation::Settings {
                packet,
                response_opcode,
                ..
            } => {
                self.settings = Some(dapi::SettingsExchange::new(
                    &self.target,
                    wrapper,
                    Some(*response_opcode),
                )?);
                dapi::build_settings_request(selector, wrapper, packet)
            }
            ManagedOperation::Reboot { host_mac, .. } => {
                let packet =
                    crate::commands::build_reboot(*host_mac, protocol::allocate_message_id())
                        .map_err(|error| error.to_string())?;
                self.settings = Some(dapi::SettingsExchange::new(&self.target, wrapper, None)?);
                dapi::build_settings_request(selector, wrapper, &packet)
            }
        }
        .ok_or("could not encode managed operation")?;

        self.wrapper_id = wrapper;
        self.sent = true;
        outgoing.push(packet);
        Ok(())
    }

    fn accept_frame(
        &mut self,
        frame: &[u8],
        outgoing: &mut Vec<Vec<u8>>,
    ) -> Result<Option<String>, String> {
        if let Some(description) = dapi::parse_session_description(frame) {
            if self
                .domain_id
                .as_ref()
                .is_some_and(|domain| domain != &description.domain_id)
            {
                return Err("Controller changed the domain of an initialized session".into());
            }

            check_domain(&description.domain_id, self.expected_domain_id.as_deref())?;

            if self.domain_id.is_none() {
                let domain = u128::from_str_radix(&description.domain_id, 16)
                    .map_err(|_| "invalid Controller domain identifier")?
                    .to_be_bytes();
                outgoing.push(
                    dapi::build_domain_initialization(
                        &domain,
                        protocol::allocate_message_id(),
                        self.notification_port.get(),
                        self.local_ipv4.octets(),
                    )
                    .ok_or("invalid Controller initialization parameters")?,
                );
                self.domain_id = Some(description.domain_id);
            }
        }

        if dapi::parse_service_announcement(frame).is_some() {
            outgoing.push(
                dapi::build_service_acknowledgement(frame).ok_or("invalid service announcement")?,
            );
        }

        if let Some(announcement) = dapi::parse_device_announcement(frame) {
            self.targets
                .insert(announcement.device_id, announcement.target_selector);
        }

        let mut packet_hex = None;

        // Unsolicited publications cannot complete a command that has not been sent.
        if self.sent {
            let complete = match self.operation.as_ref() {
                Some(ManagedOperation::MonitorSignals) => false,
                Some(ManagedOperation::Identify { .. }) => dapi::parse_identify_confirmation(frame)
                    .is_some_and(|confirmation| confirmation.device_id == self.target),
                Some(ManagedOperation::Arc { packet, .. }) => {
                    packet_hex = dapi::correlate_arc_response(dapi::ArcCorrelationRequest {
                        request_packet: packet.clone(),
                        wrapper_id: self.wrapper_id,
                        response_frame: frame.to_vec(),
                    })?
                    .map(|response| response.packet_hex);
                    packet_hex.is_some()
                }
                Some(ManagedOperation::Settings { .. } | ManagedOperation::Reboot { .. }) => {
                    let exchange = self
                        .settings
                        .as_mut()
                        .ok_or("missing managed settings exchange")?;
                    exchange.accept(frame);
                    packet_hex = exchange.packet_hex.clone();
                    exchange.complete()
                }
                None => return Err("received a frame without an active managed operation".into()),
            };

            if complete {
                self.operation = None;
                self.settings = None;
            }
        }

        self.send_operation(outgoing)?;
        Ok(packet_hex)
    }
}

fn check_domain(actual: &str, expected: Option<&str>) -> Result<(), String> {
    if let Some(expected) = expected {
        if actual != expected {
            return Err(format!(
                "DDM Controller session selected domain {actual}, expected {expected}"
            ));
        }
    }

    Ok(())
}

pub fn advance(request: ManagedSessionRequest) -> Result<ManagedSessionStep, String> {
    let mut outgoing = Vec::new();
    let mut packet_hex = None;
    let mut signal_presence = None;
    let state = match request {
        ManagedSessionRequest::Begin {
            state,
            credential,
            local_ipv4,
            notification_port,
            expected_domain_id,
            operation,
        } => {
            let target = operation.validate()?;
            let expected = expected_domain_id
                .map(|value| {
                    device_identity::normalize(
                        device_identity::DeviceIdentityRequest::ManagedDomain(value.into()),
                    )
                    .ok_or("invalid managed domain identifier")
                })
                .transpose()?;
            let mut state = match state {
                Some(state) => {
                    if state.operation.is_some() || !state.frame.is_empty() {
                        return Err("managed operation already in progress".into());
                    }

                    let domain = state
                        .domain_id
                        .as_deref()
                        .ok_or("managed session is not initialized")?;
                    check_domain(domain, expected.as_deref())?;

                    if state.local_ipv4 != local_ipv4
                        || state.notification_port != notification_port
                    {
                        return Err(
                            "managed notification endpoint changed; open a new session".into()
                        );
                    }

                    state
                }
                None => {
                    let authentication = dapi::build_authentication(credential.as_bytes())
                        .filter(|_| credential.is_ascii())
                        .ok_or("invalid Controller credential")?;
                    outgoing.extend([dapi::build_session_open(), authentication]);
                    ManagedSessionState {
                        domain_id: None,
                        expected_domain_id: expected,
                        local_ipv4,
                        notification_port,
                        targets: BTreeMap::new(),
                        wrapper_id: 0,
                        operation: None,
                        target: String::new(),
                        sent: false,
                        settings: None,
                        frame: Vec::new(),
                    }
                }
            };
            state.operation = Some(operation);
            state.target = target;
            state.sent = false;
            state.send_operation(&mut outgoing)?;
            state
        }
        ManagedSessionRequest::Receive { mut state, data } => {
            if state.operation.is_none() {
                return Err("no managed operation is in progress".into());
            }

            if data.is_empty() || data.len() > state.receive_bytes()? {
                return Err("received data must fit the requested Controller frame segment".into());
            }

            state.frame.extend(data);

            if state.receive_bytes()? == 0 {
                let frame = std::mem::take(&mut state.frame);
                packet_hex = state.accept_frame(&frame, &mut outgoing)?;

                if state.domain_id.is_some()
                    && matches!(state.operation, Some(ManagedOperation::MonitorSignals))
                {
                    signal_presence = dapi::parse_signal_presence_publication(&frame);
                }
            }

            state
        }
    };
    let complete = state.operation.is_none();
    let receive_bytes = if complete { 0 } else { state.receive_bytes()? };
    Ok(ManagedSessionStep {
        state,
        outgoing,
        receive_bytes,
        complete,
        packet_hex,
        signal_presence,
    })
}
