use std::fs::{File, OpenOptions};
use std::io::Write;
use std::net::Ipv4Addr;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use if_addrs::IfAddr;
use netaudio_core::native_audio::endpoint_control::monitoring::{Identity, Interface, Statistics};
use netlink_sys::{Socket, SocketAddr, protocols::NETLINK_ROUTE};
use serde::{Deserialize, Serialize};
use tokio::io::unix::AsyncFd;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

const NETLINK_ERROR: u16 = libc::NLMSG_ERROR as u16;
const NETLINK_OVERRUN: u16 = libc::NLMSG_OVERRUN as u16;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct DurableState {
    format_revision: u16,
    name: String,
    identity: Identity,
    next_generation: u64,
}

pub struct GenerationIssuer {
    directory: PathBuf,
    state: DurableState,
    _lease: File,
}

impl GenerationIssuer {
    pub fn open(directory: &Path, name: &str, identity: Identity) -> Result<Self> {
        let created = match std::fs::create_dir(directory) {
            Ok(()) => true,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => false,
            Err(error) => return Err(error.into()),
        };

        let lease = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(directory.join("owner.lock"))?;
        if unsafe { libc::flock(lease.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            bail!("endpoint identity state already has a running owner");
        }

        let path = directory.join("identity.json");
        let state = if created {
            DurableState {
                format_revision: 1,
                name: name.to_owned(),
                identity,
                next_generation: 1,
            }
        } else {
            serde_json::from_reader(File::open(&path).context("existing endpoint state has lost its generation issuer; explicit identity recovery is required")?)?
        };

        if state.format_revision != 1
            || state.name != name
            || state.identity != identity
            || state.next_generation == 0
        {
            bail!("endpoint identity state does not match the configured owner");
        }

        let mut issuer = Self {
            directory: directory.to_owned(),
            state,
            _lease: lease,
        };

        if created {
            issuer.persist()?;
        }

        Ok(issuer)
    }

    fn persist(&mut self) -> Result<()> {
        let temporary = self.directory.join("identity.pending.json");
        let mut output = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .context("generation issuer has an unresolved interrupted write")?;
        serde_json::to_writer(&mut output, &self.state)?;
        output.write_all(b"\n")?;
        output.sync_all()?;
        std::fs::rename(&temporary, self.directory.join("identity.json"))?;
        File::open(&self.directory)?.sync_all()?;
        Ok(())
    }

    pub fn reserve(&mut self) -> Result<u32> {
        let generation = u32::try_from(self.state.next_generation)
            .context("endpoint source generation space is exhausted")?;
        self.state.next_generation = self
            .state
            .next_generation
            .checked_add(1)
            .context("endpoint generation issuer overflow")?;
        self.persist()?;
        Ok(generation)
    }
}

pub struct Generations {
    receiver: mpsc::Receiver<Result<u32>>,
    worker: JoinHandle<()>,
}

impl Generations {
    pub fn start(mut issuer: GenerationIssuer) -> Self {
        let (sender, receiver) = mpsc::channel(1);
        let worker = tokio::task::spawn_blocking(move || {
            loop {
                let result = issuer.reserve();
                let failed = result.is_err();

                if sender.blocking_send(result).is_err() || failed {
                    break;
                }
            }
        });

        Self {
            receiver,
            worker,
        }
    }

    pub async fn next(&mut self) -> Result<u32> {
        self.receiver.recv().await.context("durable source generation provider stopped")?
    }

    pub async fn close(mut self) -> Result<()> {
        self.receiver.close();
        self.worker.await?;
        Ok(())
    }
}

fn read_number(path: impl AsRef<Path>) -> Result<u64> {
    Ok(std::fs::read_to_string(path)?.trim().parse()?)
}

pub fn interface(name: &str, address: Ipv4Addr) -> Result<Interface> {
    if name.is_empty()
        || name.bytes().any(|byte| byte == b'/' || byte == 0)
        || matches!(name, "." | "..")
    {
        bail!("invalid interface name");
    }

    let interfaces = if_addrs::get_if_addrs()?;
    let selected = interfaces
        .iter()
        .find(|interface| interface.name == name && interface.ip() == std::net::IpAddr::V4(address))
        .context("selected IPv4 address is not assigned to the selected interface")?;
    let IfAddr::V4(ipv4) = &selected.addr else {
        bail!("selected interface is not IPv4");
    };

    let index = selected
        .index
        .filter(|index| *index != 0)
        .context("selected interface index is unavailable")?;
    let hardware_address = netaudio_core::netif::host_mac_for_ipv4(address)
        .context("selected interface hardware address is unavailable")?;
    let flags = u32::from_str_radix(
        std::fs::read_to_string(format!("/sys/class/net/{name}/flags"))?
            .trim()
            .trim_start_matches("0x"),
        16,
    )?;
    let speed = read_number(format!("/sys/class/net/{name}/speed"))
        .ok()
        .and_then(|value| u32::try_from(value).ok())
        .filter(|value| *value != 0);
    Ok(Interface {
        index,
        hardware_address,
        address,
        netmask: ipv4.netmask,
        gateway: Ipv4Addr::UNSPECIFIED,
        name_server: Ipv4Addr::UNSPECIFIED,
        link_up: flags & libc::IFF_UP as u32 != 0
            && read_number(format!("/sys/class/net/{name}/carrier"))? != 0,
        speed_megabits_per_second: speed,
    })
}

struct Counters {
    transmit: u64,
    receive: u64,
    transmit_errors: u64,
    receive_errors: u64,
    observed: u64,
}

pub struct CounterProvider {
    name: String,
    previous: Option<Counters>,
}

impl CounterProvider {
    pub fn new(name: String) -> Self {
        Self {
            name,
            previous: None,
        }
    }

    pub fn measure(&mut self, now: u64) -> Result<Option<Statistics>> {
        let root = PathBuf::from(format!("/sys/class/net/{}/statistics", self.name));
        let current = Counters {
            transmit: read_number(root.join("tx_bytes"))?,
            receive: read_number(root.join("rx_bytes"))?,
            transmit_errors: read_number(root.join("tx_errors"))?,
            receive_errors: read_number(root.join("rx_errors"))?,
            observed: now,
        };

        let observation = self.previous.as_ref().and_then(|previous| {
            let elapsed = now.checked_sub(previous.observed).filter(|elapsed| *elapsed > 0)?;
            let transmit = u128::from(current.transmit.checked_sub(previous.transmit)?)
                * 1_000_000_000
                / u128::from(elapsed);
            let receive = u128::from(current.receive.checked_sub(previous.receive)?)
                * 1_000_000_000
                / u128::from(elapsed);
            Some(Statistics {
                transmit_bytes_per_second: u32::try_from(transmit).ok()?,
                receive_bytes_per_second: u32::try_from(receive).ok()?,
                transmit_errors: u32::try_from(current.transmit_errors).ok()?,
                receive_errors: u32::try_from(current.receive_errors).ok()?,
            })
        });
        self.previous = Some(current);
        Ok(observation)
    }
}

pub struct InterfaceObserver {
    pub initial: Interface,
    worker: JoinHandle<Result<Interface>>,
}

impl InterfaceObserver {
    pub async fn start(name: String, address: Ipv4Addr) -> Result<Self> {
        let notifications = InterfaceNotifications::new()?;
        let selected_name = name.clone();
        let initial =
            tokio::task::spawn_blocking(move || interface(&selected_name, address)).await??;
        let baseline = initial.clone();
        let worker = tokio::spawn(async move {
            loop {
                notifications.changed(baseline.index).await?;
                let selected_name = name.clone();
                let current =
                    tokio::task::spawn_blocking(move || interface(&selected_name, address))
                        .await??;

                if current != baseline {
                    return Ok(current);
                }
            }
        });

        Ok(Self {
            initial,
            worker,
        })
    }

    pub async fn changed(&mut self) -> Result<Interface> {
        (&mut self.worker).await.context("interface observer stopped")?
    }
}

impl Drop for InterfaceObserver {
    fn drop(&mut self) {
        self.worker.abort();
    }
}

struct InterfaceNotifications {
    socket: AsyncFd<Socket>,
}

impl InterfaceNotifications {
    fn new() -> Result<Self> {
        let mut socket = Socket::new(NETLINK_ROUTE)?;
        let groups = (libc::RTMGRP_LINK | libc::RTMGRP_IPV4_IFADDR) as u32;
        socket.bind(&SocketAddr::new(0, groups))?;
        socket.set_non_blocking(true)?;
        Ok(Self {
            socket: unsafe { AsyncFd::register(socket)? },
        })
    }

    async fn changed(&self, selected_index: u32) -> Result<()> {
        let mut buffer = vec![0; 65_536];
        loop {
            let mut ready = self.socket.readable().await?;
            let (length, sender) =
                match ready.try_io(|socket| socket.get_ref().recv_from(&mut &mut buffer[..], 0)) {
                    Ok(result) => result?,
                    Err(_) => continue,
                };

            if sender.port_number() != 0 {
                continue;
            }

            let mut offset = 0;
            while offset + 16 <= length {
                let extent = u32::from_ne_bytes(buffer[offset..offset + 4].try_into()?) as usize;
                let kind = u16::from_ne_bytes(buffer[offset + 4..offset + 6].try_into()?);
                if extent < 16 || offset.checked_add(extent).is_none_or(|end| end > length) {
                    bail!("interface observer received an incomplete notification");
                }

                if matches!(kind, NETLINK_ERROR | NETLINK_OVERRUN) {
                    bail!("interface observer lost authoritative notification state");
                }

                if matches!(
                    kind,
                    libc::RTM_NEWLINK | libc::RTM_DELLINK | libc::RTM_NEWADDR | libc::RTM_DELADDR
                ) {
                    if extent < 24 {
                        bail!("interface observer notification lacks its index");
                    }

                    let index = u32::from_ne_bytes(buffer[offset + 20..offset + 24].try_into()?);
                    if index == selected_index {
                        return Ok(());
                    }
                }
                offset += (extent + 3) & !3;
            }
        }
    }
}
