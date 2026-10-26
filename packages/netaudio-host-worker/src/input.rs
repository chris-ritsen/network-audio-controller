use anyhow::{Context, Result, bail, ensure};
use netaudio_core::host_audio::protocol::{MAXIMUM_FRAME_BYTES, Request};
use std::{
    collections::VecDeque,
    io::{ErrorKind, Read},
    os::{fd::AsRawFd, unix::net::UnixStream},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

pub struct Wake {
    writer: UnixStream,
    revision: AtomicU64,
}

impl Wake {
    pub fn notify(&self) {
        self.revision.fetch_add(1, Ordering::Release);
        let value = 1_u8;
        unsafe {
            libc::write(self.writer.as_raw_fd(), (&value as *const u8).cast(), 1);
        }
    }

    pub fn revision(&self) -> u64 {
        self.revision.load(Ordering::Acquire)
    }
}

pub enum Event {
    Request(Request),
    Changed,
    Deadline,
    Closed,
}

pub struct Input {
    reader: UnixStream,
    wake: Arc<Wake>,
    bytes: Vec<u8>,
    sequence: u64,
    closed: bool,
    deferred: VecDeque<Request>,
}

impl Input {
    pub fn new() -> Result<Self> {
        let (reader, writer) = UnixStream::pair().context("create worker notification channel")?;
        reader.set_nonblocking(true)?;
        writer.set_nonblocking(true)?;
        let flags = unsafe { libc::fcntl(libc::STDIN_FILENO, libc::F_GETFL) };
        ensure!(flags >= 0, "read worker input descriptor flags");
        ensure!(
            unsafe { libc::fcntl(libc::STDIN_FILENO, libc::F_SETFL, flags | libc::O_NONBLOCK) }
                >= 0,
            "set worker input nonblocking"
        );
        Ok(Self {
            reader,
            wake: Arc::new(Wake {
                writer,
                revision: AtomicU64::new(0),
            }),
            bytes: Vec::new(),
            sequence: 0,
            closed: false,
            deferred: VecDeque::new(),
        })
    }

    pub fn wake(&self) -> Arc<Wake> {
        self.wake.clone()
    }

    fn request(&mut self) -> Result<Option<Request>> {
        if self.bytes.len() < 4 {
            return Ok(None);
        }
        let length = u32::from_be_bytes(self.bytes[..4].try_into()?) as usize;
        ensure!(
            length > 0 && length <= MAXIMUM_FRAME_BYTES,
            "invalid worker request frame length"
        );
        if self.bytes.len() < length + 4 {
            return Ok(None);
        }
        let request: Request = serde_json::from_slice(&self.bytes[4..length + 4])?;
        self.bytes.drain(..length + 4);
        ensure!(
            request.sequence > self.sequence,
            "worker request sequence was replayed or reordered"
        );
        self.sequence = request.sequence;
        Ok(Some(request))
    }

    pub fn receive(&mut self, deadline: Option<Instant>) -> Result<Event> {
        if let Some(request) = self.deferred.pop_front() {
            return Ok(Event::Request(request));
        }
        self.receive_source(deadline)
    }

    pub fn defer(&mut self, request: Request) -> Result<()> {
        ensure!(
            self.deferred.len() < 16,
            "host audio worker pending request budget exceeded"
        );
        self.deferred.push_back(request);
        Ok(())
    }

    pub fn receive_source(&mut self, deadline: Option<Instant>) -> Result<Event> {
        loop {
            if let Some(request) = self.request()? {
                return Ok(Event::Request(request));
            }
            if self.closed {
                ensure!(self.bytes.is_empty(), "worker input ended inside a frame");
                return Ok(Event::Closed);
            }
            let timeout = match deadline {
                Some(deadline) => {
                    let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                        return Ok(Event::Deadline);
                    };
                    remaining
                        .as_millis()
                        .saturating_add(1)
                        .min(i32::MAX as u128) as i32
                }
                None => -1,
            };
            let mut descriptors = [
                libc::pollfd {
                    fd: libc::STDIN_FILENO,
                    events: libc::POLLIN,
                    revents: 0,
                },
                libc::pollfd {
                    fd: self.reader.as_raw_fd(),
                    events: libc::POLLIN,
                    revents: 0,
                },
            ];
            let count =
                unsafe { libc::poll(descriptors.as_mut_ptr(), descriptors.len() as _, timeout) };
            if count < 0 {
                let error = std::io::Error::last_os_error();
                if error.kind() == ErrorKind::Interrupted {
                    continue;
                }
                return Err(error.into());
            }
            if count == 0 {
                return Ok(Event::Deadline);
            }
            if descriptors
                .iter()
                .any(|entry| entry.revents & (libc::POLLERR | libc::POLLNVAL) != 0)
            {
                bail!("host audio worker input channel failed");
            }
            if descriptors[0].revents & (libc::POLLIN | libc::POLLHUP) != 0 {
                let mut bytes = [0; 4096];
                let count = unsafe {
                    libc::read(libc::STDIN_FILENO, bytes.as_mut_ptr().cast(), bytes.len())
                };
                if count > 0 {
                    self.bytes.extend_from_slice(&bytes[..count as usize]);
                    ensure!(
                        self.bytes.len() <= MAXIMUM_FRAME_BYTES + 4100,
                        "worker input exceeds frame budget"
                    );
                } else if count == 0 {
                    self.closed = true;
                } else {
                    let error = std::io::Error::last_os_error();
                    if !matches!(error.kind(), ErrorKind::WouldBlock | ErrorKind::Interrupted) {
                        return Err(error.into());
                    }
                }
            }
            if descriptors[1].revents & libc::POLLIN != 0 {
                let mut bytes = [0; 1024];
                loop {
                    match self.reader.read(&mut bytes) {
                        Ok(0) => bail!("worker notification channel closed"),
                        Ok(_) => {}
                        Err(error) if error.kind() == ErrorKind::WouldBlock => break,
                        Err(error) if error.kind() == ErrorKind::Interrupted => continue,
                        Err(error) => return Err(error.into()),
                    }
                }
                return Ok(Event::Changed);
            }
        }
    }
}
