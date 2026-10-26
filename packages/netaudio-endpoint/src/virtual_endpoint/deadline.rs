use std::io;
use std::mem::{size_of, zeroed};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

use anyhow::{Result, bail};
use netaudio_core::native_audio::clock::NANOSECONDS_PER_SECOND;
use tokio::io::unix::AsyncFd;

use super::sockets::Monotonic;

pub struct Deadline {
    descriptor: AsyncFd<OwnedFd>,
    time: Monotonic,
    armed: Option<u64>,
}

impl Deadline {
    pub fn new(time: Monotonic) -> Result<Self> {
        let descriptor = unsafe {
            libc::timerfd_create(libc::CLOCK_MONOTONIC, libc::TFD_CLOEXEC | libc::TFD_NONBLOCK)
        };

        if descriptor < 0 {
            return Err(io::Error::last_os_error().into());
        }

        let owned = unsafe { OwnedFd::from_raw_fd(descriptor) };

        Ok(Self {
            descriptor: unsafe { AsyncFd::register(owned)? },
            time,
            armed: None,
        })
    }

    pub fn arm(&mut self, deadline: Option<u64>) -> Result<()> {
        if self.armed == deadline {
            return Ok(());
        }

        let mut specification: libc::itimerspec = unsafe { zeroed() };

        if let Some(deadline) = deadline {
            let remaining = deadline.saturating_sub(self.time.now()).max(1);
            specification.it_value.tv_sec = (remaining / NANOSECONDS_PER_SECOND).try_into()?;
            specification.it_value.tv_nsec = (remaining % NANOSECONDS_PER_SECOND).try_into()?;
        }

        let result = unsafe {
            libc::timerfd_settime(
                self.descriptor.as_raw_fd(),
                0,
                &specification,
                std::ptr::null_mut(),
            )
        };

        if result != 0 {
            return Err(io::Error::last_os_error().into());
        }

        self.armed = deadline;
        Ok(())
    }

    pub async fn expired(&mut self) -> Result<()> {
        loop {
            let mut ready = self.descriptor.readable().await?;
            let observed = ready.try_io(|descriptor| {
                let mut expirations = 0u64;
                let length = unsafe {
                    libc::read(
                        descriptor.as_raw_fd(),
                        (&mut expirations as *mut u64).cast(),
                        size_of::<u64>(),
                    )
                };

                if length < 0 {
                    return Err(io::Error::last_os_error());
                }

                Ok((length, expirations))
            });

            let (length, expirations) = match observed {
                Ok(result) => result?,
                Err(_) => continue,
            };

            if length as usize != size_of::<u64>() || expirations != 1 {
                bail!("media deadline provider returned an invalid one-shot expiration");
            }

            self.armed = None;
            return Ok(());
        }
    }
}
