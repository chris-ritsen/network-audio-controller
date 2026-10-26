#[cfg(unix)]
mod capture;
#[cfg(unix)]
mod measurement;
#[cfg(unix)]
mod provider;
#[cfg(unix)]
mod recording;

fn main() -> anyhow::Result<()> {
    let purpose = match std::env::args().skip(1).collect::<Vec<_>>().as_slice() {
        [] => netaudio_core::host_audio::protocol::Purpose::Observe,
        [argument] if argument == "--measure" => {
            netaudio_core::host_audio::protocol::Purpose::Measure
        }
        [argument] if argument == "--record" => {
            netaudio_core::host_audio::protocol::Purpose::Record
        }
        _ => anyhow::bail!("Unsupported JACK worker arguments"),
    };
    let mut output = netaudio_host_worker::output::Output::new(
        netaudio_core::host_audio::protocol::Provider::Jack,
        purpose,
    )?;
    #[cfg(unix)]
    let result = match purpose {
        netaudio_core::host_audio::protocol::Purpose::Observe => provider::run(&mut output),
        netaudio_core::host_audio::protocol::Purpose::Measure => measurement::run(&mut output),
        netaudio_core::host_audio::protocol::Purpose::Record => recording::run(&mut output),
    };
    #[cfg(not(unix))]
    let result: anyhow::Result<()> = Err(anyhow::anyhow!(
        "JACK observation is unavailable on this platform"
    ));
    if let Err(error) = &result {
        let _ = output.send(netaudio_core::host_audio::protocol::Event::Unavailable {
            reason: format!("{error:#}"),
        });
    }
    result
}
