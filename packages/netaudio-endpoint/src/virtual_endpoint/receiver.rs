use std::net::Ipv4Addr;

use anyhow::Result;
use netaudio_core::native_audio::endpoint_control::Endpoint;

use super::resolution::{Event, Resolver};
use super::selection::{Candidate, Selection};
use super::sockets::Monotonic;

pub struct Discovery {
    address: Ipv4Addr,
    interface_index: u32,
    time: Monotonic,
    selected: Option<(Selection, Resolver)>,
    candidate: Option<Candidate>,
    unavailable: Option<String>,
}

impl Discovery {
    pub fn new(address: Ipv4Addr, interface_index: u32, time: Monotonic) -> Self {
        Self {
            address,
            interface_index,
            time,
            selected: None,
            candidate: None,
            unavailable: None,
        }
    }

    pub fn select(&mut self, endpoint: &Endpoint) -> Result<()> {
        self.selected = None;
        self.candidate = None;
        self.unavailable = None;

        let selection = match Selection::from_endpoint(endpoint) {
            Ok(selection) => selection,
            Err(error) => {
                self.unavailable = Some(format!("{error:#}"));
                return Ok(());
            }
        };

        if let Some(selection) = selection {
            let resolver = match Resolver::new(
                self.address,
                self.interface_index,
                selection.names(),
                self.time.clone(),
            ) {
                Ok(resolver) => resolver,
                Err(error) => {
                    self.unavailable = Some(format!("source resolver could not start: {error:#}"));
                    return Err(error);
                }
            };
            self.selected = Some((selection, resolver));
            self.refresh();
        }

        Ok(())
    }

    fn refresh(&mut self) {
        let Some((selection, resolver)) = &self.selected else {
            return;
        };

        match selection.resolve(|name| resolver.resolve(name)) {
            Ok(candidate) => {
                self.candidate = Some(candidate);
                self.unavailable = None;
            }
            Err(error) => {
                self.candidate = None;
                let reason = format!("{error:#}");

                if self.unavailable.as_deref() != Some(reason.as_str()) {
                    eprintln!("receiver source unavailable: {reason}");
                }

                self.unavailable = Some(reason);
            }
        }
    }

    pub fn candidate(&self) -> Option<&Candidate> {
        self.candidate.as_ref().filter(|candidate| self.time.now() <= candidate.usable_through)
    }

    pub fn unavailable(&self) -> Option<&str> {
        self.unavailable.as_deref().or_else(|| {
            self.candidate
                .as_ref()
                .filter(|candidate| self.time.now() > candidate.usable_through)
                .map(|_| "selected source records have expired")
        })
    }

    pub async fn changed(&mut self) -> Result<()> {
        loop {
            let event = match &mut self.selected {
                Some((_, resolver)) => resolver.next_event().await?,
                None => std::future::pending().await,
            };

            match event {
                Event::RecordsChanged => {
                    self.refresh();
                    return Ok(());
                }
                Event::Rejected(reason) => {
                    eprintln!("source discovery datagram rejected: {reason}")
                }
            }
        }
    }
}
