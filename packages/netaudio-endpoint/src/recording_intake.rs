use std::net::{SocketAddr, SocketAddrV4};
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use chrono::{SecondsFormat, Utc};
use tokio::net::UdpSocket;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio::task::JoinHandle;

const DATAGRAM_BUFFER_BYTES: usize = 65_536;
const MAXIMUM_QUEUED_DATAGRAMS: usize = 2_048;
const MAXIMUM_QUEUED_BYTES: usize = 8 * 1_024 * 1_024;

pub struct Datagram {
    pub bytes: Vec<u8>,
    pub peer: SocketAddr,
    pub received_at: String,
    _capacity: OwnedSemaphorePermit,
}

pub struct Intake {
    socket: Arc<UdpSocket>,
    datagrams: mpsc::Receiver<Datagram>,
    stop: Option<oneshot::Sender<()>>,
    worker: Option<JoinHandle<Result<()>>>,
}

impl Intake {
    pub async fn start(socket: std::net::UdpSocket) -> Result<Self> {
        socket.set_nonblocking(true)?;
        let (startup, ready) = oneshot::channel();
        let (stop, mut stopped) = oneshot::channel();
        let (sender, datagrams) = mpsc::channel(MAXIMUM_QUEUED_DATAGRAMS);
        let worker = tokio::task::spawn_blocking(move || {
            let runtime = tokio::runtime::Builder::new_current_thread().enable_io().build()?;
            runtime.block_on(async move {
                let socket = Arc::new(UdpSocket::from_std(socket)?);

                if startup.send(socket.clone()).is_err() {
                    return Ok(());
                }

                let capacity = Arc::new(Semaphore::new(MAXIMUM_QUEUED_BYTES));
                let mut buffer = vec![0; DATAGRAM_BUFFER_BYTES];

                loop {
                    let (length, peer) = tokio::select! {
                        biased;
                        _ = &mut stopped => return Ok(()),
                        received = socket.recv_from(&mut buffer) => received?,
                    };
                    let received_at = Utc::now().to_rfc3339_opts(SecondsFormat::Nanos, true);
                    let permit = capacity
                        .clone()
                        .try_acquire_many_owned(u32::try_from(length)?)
                        .context("recording intake exceeded its queued-byte budget")?;
                    let datagram = Datagram {
                        bytes: buffer[..length].to_vec(),
                        peer,
                        received_at,
                        _capacity: permit,
                    };

                    if let Err(error) = sender.try_send(datagram) {
                        match error {
                            mpsc::error::TrySendError::Closed(_) => return Ok(()),
                            mpsc::error::TrySendError::Full(_) => {
                                bail!("recording intake exceeded its queued-datagram budget");
                            }
                        }
                    }
                }
            })
        });
        let socket = match ready.await {
            Ok(socket) => socket,
            Err(_) => {
                worker.await.context("recording intake failed during startup")??;
                bail!("recording intake stopped before opening its socket");
            }
        };

        Ok(Self {
            socket,
            datagrams,
            stop: Some(stop),
            worker: Some(worker),
        })
    }

    pub fn local_address(&self) -> Result<SocketAddr> {
        Ok(self.socket.local_addr()?)
    }

    pub async fn receive(&mut self) -> Result<Datagram> {
        if let Some(datagram) = self.datagrams.recv().await {
            return Ok(datagram);
        }

        if let Some(worker) = self.worker.as_mut() {
            let result = worker.await;
            self.worker.take();
            result.context("recording intake worker failed")??;
        }

        bail!("recording intake stopped before the recording finished");
    }

    pub async fn send(&self, bytes: &[u8], destination: SocketAddrV4) -> Result<usize> {
        Ok(self.socket.send_to(bytes, destination).await?)
    }

    pub async fn close(mut self) -> Result<()> {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }

        self.datagrams.close();

        if let Some(worker) = self.worker.take() {
            worker.await.context("recording intake failed during shutdown")??;
        }

        Ok(())
    }
}
