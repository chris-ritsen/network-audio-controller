use super::{process::Process, provider::Outcome};
use anyhow::{Context, Result, bail, ensure};
use netaudio_core::host_audio::{
    measurement::Request,
    protocol::{Event, Provider, Purpose, RequestAction},
};
use serde_json::json;
use std::{path::PathBuf, time::Duration};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

struct Pending {
    request: Request,
    response: oneshot::Sender<Result<Outcome>>,
}

#[derive(Clone)]
pub struct Handle(mpsc::Sender<Pending>);

pub struct Worker {
    path: PathBuf,
    host: String,
    requests: mpsc::Receiver<Pending>,
}

impl Worker {
    pub fn new(path: PathBuf, host: String) -> (Handle, Self) {
        let (requests, received) = mpsc::channel(16);
        (
            Handle(requests),
            Self {
                path,
                host,
                requests: received,
            },
        )
    }

    pub async fn run(mut self, cancellation: CancellationToken) -> Result<()> {
        loop {
            let mut pending = tokio::select! {
                biased;
                _ = cancellation.cancelled() => break,
                request = self.requests.recv() => match request { Some(request) => request, None => break },
            };
            if pending.response.is_closed() {
                continue;
            }
            let result = self.capture(&mut pending, &cancellation).await;
            let outcome = result.unwrap_or_else(|error| Outcome {
                status: 503,
                body: json!({"error":format!("{error:#}")}),
            });
            let _ = pending.response.send(Ok(outcome));
        }
        Ok(())
    }

    async fn capture(
        &self,
        pending: &mut Pending,
        cancellation: &CancellationToken,
    ) -> Result<Outcome> {
        if pending.request.targets.is_empty() {
            return Ok(Outcome {
                status: 200,
                body: json!({}),
            });
        }
        let mut process =
            Process::spawn(&self.path, Provider::Jack, Purpose::Measure, &self.host).await?;
        let deadline =
            tokio::time::Instant::now() + Duration::from_secs_f64(pending.request.seconds + 10.0);
        let result = tokio::select! {
            biased;
            _ = cancellation.cancelled() => Err(anyhow::anyhow!("JACK measurement owner stopped")),
            _ = pending.response.closed() => Err(anyhow::anyhow!("JACK measurement consumer disconnected")),
            _ = tokio::time::sleep_until(deadline) => Err(anyhow::anyhow!("JACK measurement worker deadline expired")),
            result = Self::read(&mut process, &pending.request) => result,
        };
        process
            .stop()
            .await
            .context("Clean up the JACK measurement worker")?;
        result
    }

    async fn read(process: &mut Process, request: &Request) -> Result<Outcome> {
        ensure!(
            matches!(process.receive().await?.event, Event::Hello { .. }),
            "JACK measurement worker did not identify itself"
        );
        let sequence = process.next_sequence();
        process
            .send(
                sequence,
                RequestAction::Measure {
                    request: request.clone(),
                },
            )
            .await?;
        match process.receive().await?.event {
            Event::Measurement {
                request_sequence,
                readings,
            } => {
                ensure!(
                    request_sequence == sequence,
                    "JACK measurement worker answered a different request"
                );
                ensure!(
                    readings.keys().eq(request.targets.keys()),
                    "JACK measurement worker returned different targets"
                );
                Ok(Outcome {
                    status: 200,
                    body: serde_json::to_value(readings)?,
                })
            }
            Event::Unavailable { reason } => bail!("{reason}"),
            _ => bail!("JACK measurement worker returned an unexpected event"),
        }
    }
}

impl Handle {
    pub async fn measure(&self, mut request: Request) -> Result<Outcome> {
        if let Err(error) = request.validate() {
            return Ok(Outcome {
                status: 400,
                body: json!({"error":error}),
            });
        }
        let (response, received) = oneshot::channel();
        if let Err(error) = self.0.try_send(Pending { request, response }) {
            return Ok(Outcome {
                status: if matches!(error, mpsc::error::TrySendError::Full(_)) {
                    429
                } else {
                    503
                },
                body: json!({"error":"The JACK measurement queue is unavailable or full"}),
            });
        }
        received
            .await
            .context("JACK measurement owner stopped before answering")?
    }
}
