#[cfg(target_os = "linux")]
mod provider;

fn main() -> anyhow::Result<()> {
    let mut output = netaudio_host_worker::output::Output::new(
        netaudio_core::host_audio::protocol::Provider::Pulse,
        netaudio_core::host_audio::protocol::Purpose::Observe,
    )?;
    #[cfg(target_os = "linux")]
    let result = provider::run(&mut output);
    #[cfg(not(target_os = "linux"))]
    let result: anyhow::Result<()> = Err(anyhow::anyhow!(
        "PulseAudio observation is unavailable on this platform"
    ));
    if let Err(error) = &result {
        let _ = output.send(netaudio_core::host_audio::protocol::Event::Unavailable {
            reason: format!("{error:#}"),
        });
    }
    result
}
