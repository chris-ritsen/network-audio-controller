use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::num::NonZeroU16;
use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use netaudio_core::video::source_control;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::net::UdpSocket;
use tokio::sync::watch;

#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Handle {
    pub global_flow_identifier: u16,
    pub ownership_value: u32,
}

impl From<source_control::AllocationHandle> for Handle {
    fn from(value: source_control::AllocationHandle) -> Self {
        Self {
            global_flow_identifier: value.global_flow_identifier,
            ownership_value: value.ownership_value,
        }
    }
}

impl From<Handle> for source_control::AllocationHandle {
    fn from(value: Handle) -> Self {
        Self {
            global_flow_identifier: value.global_flow_identifier,
            ownership_value: value.ownership_value,
        }
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Configuration {
    pub control_bind: SocketAddrV4,
    pub source_control: SocketAddrV4,
    pub expected_allocation: Handle,
    pub receiver_host_label: String,
    pub allocation_transaction: u16,
    pub stop_transaction: u16,
    pub operation_timeout_milliseconds: u64,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum State {
    Prepared,
    AllocationPending {
        transaction: u16,
    },
    Allocated {
        handle: Handle,
    },
    AllocationOutcomeUnknown {
        transaction: u16,
        detail: String,
    },
    StopPending {
        transaction: u16,
        handle: Handle,
    },
    StoppedControl {
        handle: Handle,
    },
    StopOutcomeUnknown {
        transaction: u16,
        handle: Handle,
        detail: String,
    },
    Rejected {
        operation: u16,
        transaction: u16,
        raw_result: u16,
        handle: Option<Handle>,
        effect_unknown: bool,
    },
}

impl State {
    pub(super) fn is_allocated(&self) -> bool {
        matches!(self, Self::Allocated { .. })
    }

    pub(super) fn outcome_unknown(&self) -> bool {
        matches!(
            self,
            Self::AllocationOutcomeUnknown { .. }
                | Self::StopOutcomeUnknown { .. }
                | Self::Rejected {
                    effect_unknown: true,
                    ..
                }
        )
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum Event {
    State {
        state: State,
    },
    Receipt {
        peer: SocketAddrV4,
        bytes: Vec<u8>,
        allocation_reply: Option<source_control::AllocationReply>,
        stop_reply: Option<source_control::StopReply>,
    },
}

struct ReceivedReply {
    bytes: Vec<u8>,
    peer: SocketAddrV4,
}

pub(super) struct Client {
    configuration: Configuration,
    socket: UdpSocket,
    state: State,
    state_sender: watch::Sender<State>,
    history: Vec<Event>,
    timeout: Duration,
}

impl Configuration {
    pub(super) fn validate(&self) -> Result<()> {
        ensure!(
            valid_unicast(*self.control_bind.ip()) && self.control_bind.port() != 0,
            "control bind must be explicit unicast IPv4 with a nonzero port"
        );
        ensure!(
            valid_unicast(*self.source_control.ip()) && self.source_control.port() != 0,
            "source control must be explicit unicast IPv4 with a nonzero port"
        );
        ensure!(
            self.control_bind != self.source_control,
            "control bind and source control cannot be the same endpoint"
        );
        ensure!(
            self.expected_allocation.global_flow_identifier != 0
                && self.expected_allocation.ownership_value != 0,
            "expected allocation must contain a current nonzero owned handle"
        );
        ensure!(
            !self.receiver_host_label.is_empty()
                && !self.receiver_host_label.as_bytes().contains(&0),
            "receiver host label must be nonempty and contain no NUL"
        );
        ensure!(
            self.allocation_transaction != self.stop_transaction,
            "allocation and stop transactions must be distinct"
        );
        ensure!(self.operation_timeout_milliseconds > 0, "operation timeout is required");
        Ok(())
    }
}

impl Client {
    pub(super) fn bind(configuration: Configuration, receiver_interface: &str) -> Result<Self> {
        configuration.validate()?;
        ensure!(
            !receiver_interface.is_empty() && !receiver_interface.as_bytes().contains(&0),
            "receiver interface must be a complete name"
        );
        let socket = super::bind_control(configuration.control_bind, receiver_interface)?;
        let state = State::Prepared;
        let (state_sender, _) = watch::channel(state.clone());
        Ok(Self {
            timeout: Duration::from_millis(configuration.operation_timeout_milliseconds),
            socket,
            configuration,
            state: state.clone(),
            state_sender,
            history: vec![Event::State {
                state,
            }],
        })
    }

    pub(super) fn state(&self) -> &State {
        &self.state
    }

    pub(super) fn subscribe_state(&self) -> watch::Receiver<State> {
        self.state_sender.subscribe()
    }

    pub(super) fn can_stop(&self) -> bool {
        self.state.is_allocated()
    }

    pub(super) fn take_history(&mut self) -> Vec<Event> {
        std::mem::take(&mut self.history)
    }

    pub(super) async fn allocate(
        &mut self,
        receiver_destination: SocketAddrV4,
        source_service_channel: NonZeroU16,
        cancellation: &mut watch::Receiver<bool>,
    ) -> Result<Handle> {
        ensure!(
            matches!(&self.state, State::Prepared),
            "allocation is only permitted from prepared state"
        );
        ensure!(
            valid_unicast(*receiver_destination.ip()) && receiver_destination.port() != 0,
            "receiver destination must be supplied explicit unicast IPv4"
        );
        ensure!(!*cancellation.borrow(), "allocation cancelled before any source-control send");
        let allocation = source_control::Allocation {
            receiver_host_label: self.configuration.receiver_host_label.as_bytes(),
            flow_label: None,
            source_service_channel,
            destination: receiver_destination,
            optional: source_control::OptionalFields {
                raw_at_8: None,
                raw_at_28: None,
                opaque_span: None,
                raw_option: None,
            },
        };
        let transaction = self.configuration.allocation_transaction;
        let request = allocation.encode(transaction).map_err(anyhow::Error::from)?;
        self.set_state(State::AllocationPending {
            transaction,
        });
        let mut cancellation = Some(cancellation.clone());
        let reply = self
            .exchange(request, transaction, source_control::ALLOCATION_OPERATION, &mut cancellation)
            .await?;
        let parsed = match source_control::decode_allocation_reply(&reply.bytes) {
            Ok(value) => value,
            Err(error) => {
                self.record_raw_receipt(&reply);
                self.unknown(
                    source_control::ALLOCATION_OPERATION,
                    transaction,
                    format!("matched allocation reply is malformed: {error}"),
                );
                return Err(error.into());
            }
        };
        self.record_allocation_receipt(&reply, &parsed);
        if parsed.transaction != transaction {
            self.unknown(
                source_control::ALLOCATION_OPERATION,
                transaction,
                "matched allocation reply has a different transaction".to_owned(),
            );
            bail!("matched allocation reply has a different transaction");
        }
        if parsed.result & 0x0fff != 1 {
            self.set_state(State::Rejected {
                operation: source_control::ALLOCATION_OPERATION,
                transaction,
                raw_result: parsed.result,
                handle: None,
                effect_unknown: true,
            });
            bail!(
                "allocation returned raw result {:#06x}; effect remains uncertain",
                parsed.result
            );
        }
        let handle = match allocation_handle(&parsed) {
            Ok(handle) => handle,
            Err(error) => {
                self.unknown(
                    source_control::ALLOCATION_OPERATION,
                    transaction,
                    format!("successful allocation reply is outside the selected own slot profile: {error:#}"),
                );
                return Err(error);
            }
        };
        if handle != self.configuration.expected_allocation {
            self.unknown(
                source_control::ALLOCATION_OPERATION,
                transaction,
                "allocation reply handle differs from supplied current owned authority".to_owned(),
            );
            bail!("allocation reply handle differs from supplied current owned authority");
        }
        self.set_state(State::Allocated {
            handle,
        });
        Ok(handle)
    }

    pub(super) async fn stop(&mut self) -> Result<()> {
        let handle = match &self.state {
            State::Allocated {
                handle,
            } => *handle,
            _ => bail!("STOP is only permitted for one currently allocated owned handle"),
        };
        let transaction = self.configuration.stop_transaction;
        self.set_state(State::StopPending {
            transaction,
            handle,
        });
        let request = source_control::StopRequest {
            transaction,
            allocation: handle.into(),
        }
        .encode();
        let mut cancellation = None;
        let reply = self
            .exchange(
                request.to_vec(),
                transaction,
                source_control::STOP_OPERATION,
                &mut cancellation,
            )
            .await?;
        let parsed = match source_control::decode_stop_reply(&reply.bytes) {
            Ok(value) => value,
            Err(error) => {
                self.record_raw_receipt(&reply);
                self.unknown(
                    source_control::STOP_OPERATION,
                    transaction,
                    format!("matched STOP reply is malformed: {error}"),
                );
                return Err(error.into());
            }
        };
        self.record_stop_receipt(&reply, parsed);
        if parsed.transaction != transaction {
            self.unknown(
                source_control::STOP_OPERATION,
                transaction,
                "matched STOP reply has a different transaction".to_owned(),
            );
            bail!("matched STOP reply has a different transaction");
        }
        if parsed.result != 1 {
            self.set_state(State::Rejected {
                operation: source_control::STOP_OPERATION,
                transaction: parsed.transaction,
                raw_result: parsed.result,
                handle: Some(handle),
                effect_unknown: true,
            });
            bail!(
                "STOP returned raw result {:#06x}; local or physical retirement is not established",
                parsed.result
            );
        }
        self.set_state(State::StoppedControl {
            handle,
        });
        Ok(())
    }

    async fn exchange(
        &mut self,
        request: Vec<u8>,
        transaction: u16,
        operation: u16,
        cancellation: &mut Option<watch::Receiver<bool>>,
    ) -> Result<ReceivedReply> {
        let deadline = match tokio::time::Instant::now().checked_add(self.timeout) {
            Some(deadline) => deadline,
            None => {
                self.unknown(
                    operation,
                    transaction,
                    "source-control deadline overflow before send".to_owned(),
                );
                bail!("source-control deadline overflow");
            }
        };
        if cancellation.as_ref().is_some_and(|cancellation| *cancellation.borrow()) {
            self.unknown(
                operation,
                transaction,
                "source-control cancellation before send selection".to_owned(),
            );
            bail!("source-control operation cancelled");
        }
        let sent = tokio::select! {
            result = self.socket.send_to(&request, self.configuration.source_control) => match result {
                Ok(sent) => sent,
                Err(error) => {
                    self.unknown(operation, transaction, format!("send attempt failed after pending state: {error}"));
                    return Err(error.into());
                }
            },
            _ = cancellation_requested(cancellation) => {
                self.unknown(operation, transaction, "cancellation while source-control operation was pending".to_owned());
                bail!("source-control operation cancelled with unknown effect");
            }
            _ = tokio::time::sleep_until(deadline) => {
                self.unknown(operation, transaction, "send deadline expired".to_owned());
                bail!("source-control send deadline expired with unknown effect");
            }
        };
        if sent != request.len() {
            self.unknown(
                operation,
                transaction,
                format!(
                    "source-control datagram send was short: sent {sent} of {} bytes",
                    request.len()
                ),
            );
            bail!("source-control datagram send was short");
        }

        let mut bytes = [0u8; source_control::MAXIMUM_DATAGRAM_BYTES + 1];
        loop {
            if tokio::time::Instant::now() >= deadline {
                self.unknown(operation, transaction, "response deadline expired".to_owned());
                bail!("source-control response deadline expired with unknown effect");
            }
            let received = tokio::select! {
                result = self.socket.recv_from(&mut bytes) => result,
                _ = cancellation_requested(cancellation) => {
                    self.unknown(operation, transaction, "cancellation after source-control send".to_owned());
                    bail!("source-control operation cancelled after send with unknown effect");
                }
                _ = tokio::time::sleep_until(deadline) => {
                    self.unknown(operation, transaction, "response deadline expired".to_owned());
                    bail!("source-control response deadline expired with unknown effect");
                }
            };
            let (length, peer) = match received {
                Ok(value) => value,
                Err(error) => {
                    self.unknown(
                        operation,
                        transaction,
                        format!("receive failed after source-control send: {error}"),
                    );
                    return Err(error.into());
                }
            };
            if peer != SocketAddr::V4(self.configuration.source_control)
                || !matches_header(&bytes[..length], transaction, operation)
            {
                continue;
            }
            return Ok(ReceivedReply {
                bytes: bytes[..length].to_vec(),
                peer: self.configuration.source_control,
            });
        }
    }

    fn set_state(&mut self, state: State) {
        self.state = state.clone();
        self.state_sender.send_replace(state.clone());
        self.history.push(Event::State {
            state,
        });
    }

    fn record_raw_receipt(&mut self, reply: &ReceivedReply) {
        self.history.push(Event::Receipt {
            peer: reply.peer,
            bytes: reply.bytes.clone(),
            allocation_reply: None,
            stop_reply: None,
        });
    }

    fn record_allocation_receipt(
        &mut self,
        receipt: &ReceivedReply,
        reply: &source_control::AllocationReply,
    ) {
        self.history.push(Event::Receipt {
            peer: receipt.peer,
            bytes: receipt.bytes.clone(),
            allocation_reply: Some(reply.clone()),
            stop_reply: None,
        });
    }

    fn record_stop_receipt(&mut self, receipt: &ReceivedReply, reply: source_control::StopReply) {
        self.history.push(Event::Receipt {
            peer: receipt.peer,
            bytes: receipt.bytes.clone(),
            allocation_reply: None,
            stop_reply: Some(reply),
        });
    }

    fn unknown(&mut self, operation: u16, transaction: u16, detail: String) {
        let state = match operation {
            source_control::ALLOCATION_OPERATION => State::AllocationOutcomeUnknown {
                transaction,
                detail,
            },
            source_control::STOP_OPERATION => {
                let handle = match &self.state {
                    State::StopPending {
                        handle,
                        ..
                    } => *handle,
                    _ => return,
                };
                State::StopOutcomeUnknown {
                    transaction,
                    handle,
                    detail,
                }
            }
            _ => return,
        };
        self.set_state(state);
    }
}

fn allocation_handle(reply: &source_control::AllocationReply) -> Result<Handle> {
    ensure!(reply.trailing_datagram_bytes.is_empty(), "reply contains trailing datagram bytes");
    ensure!(reply.extension.is_none(), "reply contains an unsupported allocation extension");
    ensure!(reply.raw_base_tail == Some([0, 0]), "reply has an unexpected base tail");
    let handle = reply.allocation.context("reply lacks the allocation handle")?;
    let outcomes = reply.interface_outcomes.as_deref().context("reply lacks interface outcomes")?;
    ensure!(outcomes == [1], "reply does not establish the one selected usable own interface");
    let handle = Handle::from(handle);
    ensure!(
        handle.global_flow_identifier != 0 && handle.ownership_value != 0,
        "reply contains a zero allocation handle"
    );
    Ok(handle)
}

async fn cancellation_requested(cancellation: &mut Option<watch::Receiver<bool>>) {
    let Some(cancellation) = cancellation.as_mut() else {
        return std::future::pending::<()>().await;
    };
    loop {
        if *cancellation.borrow() {
            return;
        }
        if cancellation.changed().await.is_err() {
            return;
        }
    }
}

fn matches_header(bytes: &[u8], transaction: u16, operation: u16) -> bool {
    bytes.len() >= 8
        && word(bytes, 0) == Some(source_control::REVISION)
        && word(bytes, 4) == Some(transaction)
        && word(bytes, 6) == Some(operation)
}

fn word(bytes: &[u8], offset: usize) -> Option<u16> {
    Some(u16::from_be_bytes(bytes.get(offset..offset + 2)?.try_into().ok()?))
}

fn valid_unicast(address: Ipv4Addr) -> bool {
    !address.is_unspecified() && !address.is_multicast() && !address.is_broadcast()
}
