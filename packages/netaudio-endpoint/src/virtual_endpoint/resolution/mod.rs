pub mod cache;
pub mod records;
mod transport;

use anyhow::Result;
use hickory_proto::rr::Name;
use std::net::Ipv4Addr;

use super::deadline::Deadline;
use super::sockets::Monotonic;
use cache::{Cache, ResolvedService};
use transport::Transport;

pub enum Event {
    RecordsChanged,
    Rejected(String),
}

pub struct Resolver {
    cache: Cache,
    transport: Transport,
    time: Monotonic,
    deadline: Deadline,
    next_deadline: Option<u64>,
    schedule_changed: bool,
    interface_index: u32,
}

impl Resolver {
    pub fn new(
        address: Ipv4Addr,
        interface_index: u32,
        services: impl IntoIterator<Item = Name>,
        time: Monotonic,
    ) -> Result<Self> {
        Ok(Self {
            cache: Cache::new(services, time.now())?,
            transport: Transport::bind(address, interface_index)?,
            deadline: Deadline::new(time.clone())?,
            next_deadline: None,
            schedule_changed: true,
            time,
            interface_index,
        })
    }

    pub fn interface_index(&self) -> u32 {
        self.interface_index
    }

    pub fn resolve(&self, name: &Name) -> Result<ResolvedService> {
        self.cache.resolve(name, self.time.now())
    }

    pub async fn next_event(&mut self) -> Result<Event> {
        loop {
            if self.schedule_changed {
                let questions = self.cache.questions_due(self.time.now());

                if !questions.is_empty() {
                    self.transport.send(&records::query(questions.iter().cloned())?).await?;
                    self.cache.questions_sent(&questions, self.time.now());
                }

                self.next_deadline = self.cache.next_deadline();
                self.deadline.arm(self.next_deadline)?;
                self.schedule_changed = false;
            }

            tokio::select! {
                bytes = self.transport.receive() => {
                    let bytes = bytes?;
                    let records = match records::response(&bytes) {
                        Ok(records) => records,
                        Err(error) => return Ok(Event::Rejected(format!("{error:#}"))),
                    };

                    if !records.is_empty() {
                        self.schedule_changed = true;
                        if self.cache.observe(records, self.time.now())? {
                            return Ok(Event::RecordsChanged);
                        }
                    }
                }
                result = self.deadline.expired(), if self.next_deadline.is_some() => {
                    result?;
                    self.schedule_changed = true;
                    self.cache.expire(self.time.now());
                    return Ok(Event::RecordsChanged);
                }
            }
        }
    }
}
