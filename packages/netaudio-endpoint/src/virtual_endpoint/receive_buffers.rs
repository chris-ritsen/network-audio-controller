use anyhow::{Context, Result};
use netaudio_core::native_audio::Format;
use netaudio_core::native_audio::playout::{Buffer, Owner};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::selection::Candidate;

pub struct Preparation {
    pub candidate: Candidate,
    pub owner: Owner,
    pub format: Format,
    pub inventory_channels: Vec<u16>,
}

pub struct Prepared {
    pub preparation: Preparation,
    pub buffer: Buffer,
}

enum Request {
    Prepare(Preparation),
    Dispose(Buffer),
}

pub struct Worker {
    requests: mpsc::Sender<Request>,
    results: mpsc::Receiver<Result<Box<Prepared>>>,
    worker: JoinHandle<Result<()>>,
}

impl Worker {
    pub fn new() -> Self {
        let (requests, mut pending) = mpsc::channel(2);
        let (completed, results) = mpsc::channel(1);
        let worker = super::platform::spawn_background(move || {
            while let Some(request) = pending.blocking_recv() {
                match request {
                    Request::Prepare(preparation) => {
                        let context = &preparation.candidate.context;
                        let slots = context
                            .slots
                            .iter()
                            .map(|slot| slot.receivers.clone())
                            .collect::<Vec<_>>();
                        let result = Buffer::with_profile(
                            preparation.owner,
                            preparation.format,
                            context.requested_frames_per_packet,
                            &slots,
                            context.profile,
                        )
                        .map(|buffer| {
                            Box::new(Prepared {
                                preparation,
                                buffer,
                            })
                        })
                        .map_err(Into::into);
                        if completed.blocking_send(result).is_err() {
                            return Ok(());
                        }
                    }
                    Request::Dispose(buffer) => drop(buffer),
                }
            }
            Ok(())
        });
        Self {
            requests,
            results,
            worker,
        }
    }

    pub fn prepare(&self, preparation: Preparation) -> Result<()> {
        self.requests
            .try_send(Request::Prepare(preparation))
            .map_err(|_| anyhow::anyhow!("receive buffer worker cannot accept preparation"))
    }

    pub fn dispose(&self, buffer: Buffer) -> Result<()> {
        self.requests
            .try_send(Request::Dispose(buffer))
            .map_err(|_| anyhow::anyhow!("receive buffer worker cannot accept disposal"))
    }

    pub async fn next(&mut self) -> Result<Result<Box<Prepared>>> {
        self.results.recv().await.context("receive buffer worker stopped")
    }

    pub async fn close(self) -> Result<()> {
        drop(self.results);
        drop(self.requests);
        self.worker.await.context("receive buffer worker failed during shutdown")?
    }
}
