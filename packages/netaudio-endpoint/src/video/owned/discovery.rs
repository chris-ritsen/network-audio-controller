use std::net::{IpAddr, Ipv4Addr};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, ensure};
use mdns_sd::{IfKind, ResolvedService, ServiceDaemon, ServiceEvent, TxtProperties};
use netaudio_core::video::discovery;
use tokio::sync::watch;

use super::{Acquisition, Receiver};

const CHANNEL_PORT: u16 = 4321;
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum DiscoveryState {
    Waiting,
    Ready,
    Lost(String),
}

pub(super) async fn watch_source(
    receiver: Receiver,
    mut cancellation: watch::Receiver<bool>,
    observed: watch::Sender<DiscoveryState>,
) -> Result<()> {
    validate_receiver(&receiver)?;
    observed.send_replace(DiscoveryState::Waiting);

    let daemon = ServiceDaemon::new().context("start retained H.264 source discovery")?;
    let mut channel_browse_started = false;
    let mut flow_browse_started = false;
    let setup = (|| -> Result<_> {
        daemon.disable_interface(IfKind::All)?;
        daemon
            .enable_interface(IfKind::Addr(IpAddr::V4(receiver.configuration.interface_address)))?;
        daemon.set_ip_check_interval(0)?;
        let channels = daemon.browse(discovery::CHANNEL_SERVICE)?;
        channel_browse_started = true;
        let flows = if matches!(receiver.acquisition, Acquisition::Multicast { .. }) {
            let flows = daemon.browse(discovery::FLOW_SERVICE)?;
            flow_browse_started = true;
            Some(flows)
        } else {
            None
        };
        let channel_name = service_name(
            &receiver.source_channel,
            &receiver.source_device,
            discovery::CHANNEL_SERVICE,
        );
        let flow_name = match &receiver.acquisition {
            Acquisition::Multicast {
                source_flow,
                ..
            } => service_name(source_flow, &receiver.source_device, discovery::FLOW_SERVICE),
            Acquisition::Unicast {
                ..
            } => String::new(),
        };
        let target = format!("{}.local.", receiver.source_device);
        let deadline = tokio::time::Instant::now()
            .checked_add(Duration::from_millis(receiver.discovery_timeout_milliseconds))
            .context("H.264 source discovery deadline overflow")?;
        Ok((channels, flows, channel_name, flow_name, target, deadline))
    })();

    let result = match setup {
        Ok((channels, flows, channel_name, flow_name, target, deadline)) => {
            let mut channel_ready = false;
            let mut flow_ready = flows.is_none();
            loop {
                tokio::select! {
                    _ = cancelled(&mut cancellation) => break Ok(()),
                    _ = tokio::time::sleep_until(deadline), if !(channel_ready && flow_ready) => {
                        break lost(
                            &observed,
                            "fresh matching H.264 channel and flow discovery did not arrive before its deadline",
                        );
                    }
                    event = channels.recv_async() => {
                        match event {
                            Ok(ServiceEvent::ServiceResolved(info)) if info.get_fullname() == channel_name => {
                                if let Err(error) = channel_matches(&receiver, &target, &info) {
                                    break lost(&observed, &format!("matching H.264 channel resolution conflicts with the configured source: {error:#}"));
                                }
                                channel_ready = true;
                                if flow_ready {
                                    observed.send_replace(DiscoveryState::Ready);
                                }
                            }
                            Ok(ServiceEvent::ServiceRemoved(_, name)) if name == channel_name && channel_ready => {
                                break lost(&observed, "qualified H.264 channel discovery was withdrawn or expired");
                            }
                            Ok(_) => {}
                            Err(error) => break Err(error).context("H.264 channel discovery stream ended"),
                        }
                    }
                    event = async { match &flows { Some(flows) => flows.recv_async().await, None => std::future::pending().await } } => {
                        match event {
                            Ok(ServiceEvent::ServiceResolved(info)) if info.get_fullname() == flow_name => {
                                if let Err(error) = flow_matches(&receiver, &target, &info) {
                                    break lost(&observed, &format!("matching H.264 flow resolution conflicts with the configured source: {error:#}"));
                                }
                                flow_ready = true;
                                if channel_ready {
                                    observed.send_replace(DiscoveryState::Ready);
                                }
                            }
                            Ok(ServiceEvent::ServiceRemoved(_, name)) if name == flow_name && flow_ready => {
                                break lost(&observed, "qualified H.264 flow discovery was withdrawn or expired");
                            }
                            Ok(_) => {}
                            Err(error) => break Err(error).context("H.264 flow discovery stream ended"),
                        }
                    }
                }
            }
        }
        Err(error) => Err(error),
    };

    let cleanup = async {
        let mut failures = Vec::new();
        if channel_browse_started && let Err(error) = daemon.stop_browse(discovery::CHANNEL_SERVICE)
        {
            failures.push(format!("stop channel browse: {error}"));
        }
        if flow_browse_started && let Err(error) = daemon.stop_browse(discovery::FLOW_SERVICE) {
            failures.push(format!("stop flow browse: {error}"));
        }
        match daemon.shutdown() {
            Ok(shutdown) => {
                if let Err(error) = tokio::time::timeout(SHUTDOWN_TIMEOUT, shutdown.recv_async())
                    .await
                    .context("retained H.264 discovery shutdown exceeded its deadline")
                    .and_then(|result| {
                        result.context("retained H.264 discovery shutdown stream ended")
                    })
                {
                    failures.push(format!("shutdown: {error:#}"));
                }
            }
            Err(error) => failures.push(format!("shutdown: {error}")),
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(anyhow!(failures.join("; ")))
        }
    }
    .await;

    match (result, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
        (Err(result), Err(cleanup)) => {
            Err(result
                .context(format!("retained H.264 discovery cleanup also failed: {cleanup:#}")))
        }
    }
}

fn validate_receiver(receiver: &Receiver) -> Result<()> {
    ensure!(
        receiver.discovery_timeout_milliseconds > 0,
        "H.264 receiver discovery timeout is required"
    );
    for value in [&receiver.source_device, &receiver.source_channel] {
        ensure!(
            !value.is_empty() && !value.as_bytes().contains(&0),
            "H.264 receiver source names must be complete and NUL-free"
        );
    }
    ensure!(
        receiver.configuration.destination.port() != 0,
        "H.264 receiver discovery requires an explicit destination"
    );
    Ok(())
}

fn service_name(label: &str, device: &str, service: &str) -> String {
    format!("{label}@{device}.{service}")
}

fn channel_matches(receiver: &Receiver, target: &str, info: &ResolvedService) -> Result<()> {
    let (address, port) = match &receiver.acquisition {
        Acquisition::Multicast {
            ..
        } => (*receiver.configuration.source.ip(), CHANNEL_PORT),
        Acquisition::Unicast {
            control,
        } => (*control.source_control.ip(), control.source_control.port()),
    };
    ensure!(info.get_port() == port, "unexpected channel source-control service port");
    ensure!(
        info.get_hostname() == target,
        "channel service target hostname differs from the configured source"
    );
    ensure!(
        exact_source_address(info, address),
        "channel service does not resolve to the configured source IPv4 address"
    );
    let channel = discovery::Channel::decode(&txt(info.get_properties())?)
        .map_err(|error| anyhow!(error.0))?;
    ensure!(
        channel.source_service_channel == receiver.source_service_channel,
        "channel service identifier differs from the configured source"
    );
    ensure!(
        channel.channel_count == receiver.source_channel_count,
        "channel service count differs from the configured source"
    );
    ensure!(channel.codec_identifier == 1, "channel service is not H264:1");
    ensure!(
        !channel.protection_metadata_present,
        "channel service has unsupported protection metadata"
    );
    match &receiver.acquisition {
        Acquisition::Multicast {
            source_flow,
            source_association_slot,
            ..
        } => {
            let association = channel
                .association
                .context("channel service lacks the configured flow association")?;
            ensure!(
                association.flow_label == source_flow.as_bytes(),
                "channel service flow differs from the configured source"
            );
            ensure!(
                association.one_based_slot == *source_association_slot,
                "channel service slot differs from the configured source"
            );
        }
        Acquisition::Unicast {
            ..
        } => {
            ensure!(
                channel.association.is_none(),
                "unicast source unexpectedly advertises a multicast flow association"
            );
        }
    }
    Ok(())
}

fn flow_matches(receiver: &Receiver, target: &str, info: &ResolvedService) -> Result<()> {
    let Acquisition::Multicast {
        source_local_flow,
        ..
    } = &receiver.acquisition
    else {
        return Err(anyhow!("unicast source has no multicast flow record"));
    };
    ensure!(info.get_port() == discovery::FLOW_SERVICE_PORT, "unexpected flow service port");
    ensure!(
        info.get_hostname() == target,
        "flow service target hostname differs from the configured source"
    );
    ensure!(
        exact_source_address(info, *receiver.configuration.source.ip()),
        "flow service does not resolve to the configured source IPv4 address"
    );
    let flow =
        discovery::Flow::decode(&txt(info.get_properties())?).map_err(|error| anyhow!(error.0))?;
    ensure!(
        flow.identifier == *source_local_flow,
        "flow service identifier differs from the configured source"
    );
    ensure!(
        flow.destinations[0] == Some(receiver.configuration.destination),
        "flow service primary destination differs from the configured receiver"
    );
    ensure!(
        flow.destinations[1].is_none(),
        "flow service exceeds the selected one-destination profile"
    );
    ensure!(
        flow.protection_value.is_none() && !flow.protection_port_present,
        "flow service has unsupported protection metadata"
    );
    Ok(())
}

fn exact_source_address(info: &ResolvedService, expected: Ipv4Addr) -> bool {
    let addresses = info.get_addresses_v4();
    addresses.len() == 1 && addresses.contains(&expected)
}

fn txt(properties: &TxtProperties) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    for property in properties.iter() {
        let key = property.key().as_bytes();
        let value = property.val();
        let length = key
            .len()
            .checked_add(value.map_or(0, |value| value.len() + 1))
            .context("H.264 TXT field extent overflow")?;
        bytes.push(u8::try_from(length).context("H.264 TXT field exceeds DNS string extent")?);
        bytes.extend_from_slice(key);
        if let Some(value) = value {
            bytes.push(b'=');
            bytes.extend_from_slice(value);
        }
    }
    ensure!(!bytes.is_empty(), "H.264 TXT record is empty");
    Ok(bytes)
}

fn lost<T>(observed: &watch::Sender<DiscoveryState>, message: &str) -> Result<T> {
    observed.send_replace(DiscoveryState::Lost(message.to_owned()));
    Err(anyhow!(message.to_owned()))
}

async fn cancelled(receiver: &mut watch::Receiver<bool>) {
    let _ = receiver.wait_for(|value| *value).await;
}
