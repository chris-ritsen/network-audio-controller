use std::net::Ipv4Addr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use netaudio_core::native_audio::MAXIMUM_UDP_PAYLOAD_BYTES;
use netaudio_core::native_audio::bootstrap::{
    MEDIA_RESOURCES_CAPABILITY, Reply, Request, STRUCTURED_IDENTITY_CAPABILITY, Stage, Value,
};
use netaudio_core::native_audio::eligibility::{self, SourceFormat};
use netaudio_core::native_audio::properties::AudioProperties;
use netaudio_core::protocol::RESULT_CODE_MORE_PAGES;
use serde::Serialize;
use tokio::net::UdpSocket;
use tokio::time::Instant;

use super::selection::Candidate;
use super::sockets::{self, Monotonic};

const REQUEST_DEADLINE: Duration = Duration::from_secs(2);
const MAXIMUM_ACQUISITION_BYTES: usize = 8 * 1024 * 1024;

#[derive(Clone, Debug, Serialize)]
pub struct Observation {
    pub observed_at_nanoseconds: u64,
    pub reply: Reply,
}

#[derive(Clone, Debug, Serialize)]
pub struct Failure {
    pub request: Option<Request>,
    pub reason: String,
    pub datagram: Option<Vec<u8>>,
}

#[derive(Clone, Debug, Serialize)]
pub struct Progress {
    pub candidate: Candidate,
    pub started_at_nanoseconds: u64,
    pub completed_at_nanoseconds: Option<u64>,
    pub observations: Vec<Observation>,
    pub pending: Option<Request>,
    pub failure: Option<Failure>,
    pub discarded_datagrams: u64,
    pub retained_bytes: usize,
    pub audio_properties: Option<AudioProperties>,
    pub source_format: Option<SourceFormat>,
    pub source_format_unavailable: Option<String>,
}

pub struct Client {
    socket: Arc<UdpSocket>,
    time: Monotonic,
    next_transaction: Option<u16>,
    progress: Option<Progress>,
    deadline: Option<Instant>,
    buffer: Vec<u8>,
}

impl Client {
    pub async fn new(address: Ipv4Addr, time: Monotonic) -> Result<Self> {
        Ok(Self {
            socket: sockets::bind(address).await?,
            time,
            next_transaction: Some(0),
            progress: None,
            deadline: None,
            buffer: vec![0; MAXIMUM_UDP_PAYLOAD_BYTES],
        })
    }

    pub fn synchronize(&mut self, candidate: Option<&Candidate>) {
        let Some(candidate) =
            candidate.filter(|candidate| self.time.now() <= candidate.usable_through)
        else {
            self.progress = None;
            self.deadline = None;
            return;
        };

        if let Some(progress) = &mut self.progress
            && progress.candidate.same_context(candidate)
        {
            progress.candidate = candidate.clone();
            return;
        }

        self.deadline = None;
        self.progress = Some(Progress {
            candidate: candidate.clone(),
            started_at_nanoseconds: self.time.now(),
            completed_at_nanoseconds: None,
            observations: Vec::new(),
            pending: None,
            failure: None,
            discarded_datagrams: 0,
            retained_bytes: 0,
            audio_properties: None,
            source_format: None,
            source_format_unavailable: None,
        });

        if let Err(error) = self.request(Stage::Capabilities) {
            self.fail(format!("{error:#}"), None);
        }
    }

    pub fn progress(&self, candidate: Option<&Candidate>) -> Option<&Progress> {
        self.progress.as_ref().filter(|progress| {
            candidate.is_some_and(|candidate| {
                self.time.now() <= candidate.usable_through
                    && progress.candidate.same_context(candidate)
            })
        })
    }

    fn request(&mut self, stage: Stage) -> Result<()> {
        let transaction =
            self.next_transaction.context("source acquisition transaction space exhausted")?;
        self.next_transaction = transaction.checked_add(1);
        let request = Request {
            protocol_revision: self
                .progress
                .as_ref()
                .context("source acquisition has no selected context")?
                .candidate
                .routing
                .declaration
                .current_revision,
            stage,
            transaction,
        };
        let packet = request.encode()?;
        let progress =
            self.progress.as_mut().context("source acquisition has no selected context")?;
        progress.pending = Some(request);

        if self.time.now() > progress.candidate.usable_through {
            bail!("selected source expired before acquisition request");
        }

        let deadline = Instant::now() + REQUEST_DEADLINE;
        let length =
            self.socket.try_send_to(&packet, progress.candidate.routing.endpoint.into())?;

        if length != packet.len() {
            bail!("source acquisition submitted an incomplete datagram");
        }

        self.deadline = Some(deadline);
        Ok(())
    }

    fn fail(&mut self, reason: String, datagram: Option<Vec<u8>>) {
        eprintln!("receiver source acquisition failed: {reason}");
        self.deadline = None;

        if let Some(progress) = &mut self.progress {
            progress.failure = Some(Failure {
                request: progress.pending.take(),
                reason,
                datagram,
            });
        }
    }

    fn accept(&mut self, datagram: &[u8]) -> Result<()> {
        let progress =
            self.progress.as_mut().context("source acquisition has no selected context")?;
        let request = progress.pending.context("source acquisition has no outstanding request")?;

        if self.time.now() > progress.candidate.usable_through {
            bail!("selected source expired before acquisition reply");
        }

        let Some(reply) = request.decode_reply(datagram)? else {
            progress.discarded_datagrams += 1;
            return Ok(());
        };
        let retained_bytes = progress
            .retained_bytes
            .checked_add(datagram.len())
            .context("source acquisition byte count overflow")?;

        if retained_bytes > MAXIMUM_ACQUISITION_BYTES {
            bail!("source acquisition exceeds its retained reply budget");
        }

        progress.retained_bytes = retained_bytes;
        let capability_word = match &reply.value {
            Value::Capabilities(capabilities) => {
                Some(capabilities.transmit_flow_authoring_capability_word)
            }
            _ => None,
        };
        progress.observations.push(Observation {
            observed_at_nanoseconds: self.time.now(),
            reply,
        });

        if let Some(word) = capability_word
            && word & MEDIA_RESOURCES_CAPABILITY != 0
        {
            bail!("selected source requires media-specific resource acquisition");
        }

        let next_stage = progress.next_stage(request.stage)?;
        progress.pending = None;
        self.deadline = None;

        match next_stage {
            Some(stage) => self.request(stage)?,
            None => progress.finish(self.time.now())?,
        }

        Ok(())
    }

    pub async fn changed(&mut self) {
        let Some(deadline) = self.deadline else {
            return std::future::pending().await;
        };
        let received = tokio::select! {
            biased;
            _ = tokio::time::sleep_until(deadline) => {
                self.fail("source acquisition request timed out".into(), None);
                return;
            }
            received = self.socket.recv_from(&mut self.buffer) => received,
        };

        match received {
            Ok((length, peer)) => {
                if Instant::now() >= deadline {
                    self.fail("source acquisition request timed out".into(), None);
                    return;
                }

                let Some(progress) = &mut self.progress else {
                    return;
                };

                if peer != progress.candidate.routing.endpoint.into() {
                    progress.discarded_datagrams += 1;
                    return;
                }

                let datagram = self.buffer[..length].to_vec();

                if let Err(error) = self.accept(&datagram) {
                    self.fail(format!("{error:#}"), Some(datagram));
                }
            }
            Err(error) => self.fail(format!("source acquisition receive failed: {error}"), None),
        }
    }
}

impl Progress {
    fn next_stage(&self, completed: Stage) -> Result<Option<Stage>> {
        Ok(match completed {
            Stage::Capabilities => Some(
                if self.capabilities()?.transmit_flow_authoring_capability_word
                    & STRUCTURED_IDENTITY_CAPABILITY
                    != 0
                {
                    Stage::Identity
                } else {
                    Stage::Name
                },
            ),
            Stage::Identity | Stage::Name => Some(Stage::PropertyDirectory),
            Stage::PropertyDirectory => Some(Stage::AudioProperties),
            Stage::AudioProperties => {
                let count = self.capabilities()?.tx_count;

                if count == 0 {
                    None
                } else {
                    Some(Stage::TransmitChannels {
                        first: 1,
                        last: count,
                    })
                }
            }
            Stage::TransmitChannels {
                last,
                ..
            } => {
                let observation =
                    self.observations.last().context("source channel observation is absent")?;
                let Value::TransmitChannels(page) = &observation.reply.value else {
                    bail!("source channel observation has a different operation");
                };

                if page.result == RESULT_CODE_MORE_PAGES {
                    let highest = page
                        .channels
                        .last()
                        .context("partial source channel page made no progress")?
                        .identifier;
                    let first = highest.checked_add(1).context("source channel cursor overflow")?;

                    if first > last {
                        bail!("partial source channel page exhausted its requested interval");
                    }

                    Some(Stage::TransmitChannels {
                        first,
                        last,
                    })
                } else {
                    None
                }
            }
        })
    }

    fn capabilities(&self) -> Result<&netaudio_core::parser::ChannelCount> {
        self.observations
            .iter()
            .find_map(|observation| match &observation.reply.value {
                Value::Capabilities(value) => Some(value.as_ref()),
                _ => None,
            })
            .context("source capability observation is absent")
    }

    fn finish(&mut self, observed_at: u64) -> Result<()> {
        let settings = self
            .observations
            .iter()
            .find_map(|observation| match &observation.reply.value {
                Value::AudioProperties(value) => Some(value.as_ref()),
                _ => None,
            })
            .context("source audio property observation is absent")?;
        let properties = AudioProperties::from_settings(settings);
        let channels = self
            .observations
            .iter()
            .flat_map(|observation| match &observation.reply.value {
                Value::TransmitChannels(page) => page.channels.iter(),
                _ => [].iter(),
            })
            .collect::<Vec<_>>();
        let selected = self
            .candidate
            .slots
            .iter()
            .map(|slot| (slot.channel_name.as_str(), &slot.source))
            .collect::<Vec<_>>();

        match eligibility::qualify(
            self.capabilities()?,
            &properties,
            &channels,
            &selected,
            self.candidate.requested_frames_per_packet,
            self.candidate.receiver_domain,
        ) {
            Ok(format) => self.source_format = Some(format),
            Err(error) => self.source_format_unavailable = Some(error.to_string()),
        }

        self.audio_properties = Some(properties);
        self.completed_at_nanoseconds = Some(observed_at);
        Ok(())
    }
}
