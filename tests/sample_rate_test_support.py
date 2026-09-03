from types import SimpleNamespace


async def fake_sample_rate_change(application, device, sample_rate, confirm_destructive=False):
    from netaudio.dante.sample_rate_topology import (
        SampleRateTopologyChangedButUnverifiedError,
        SampleRateTopologyMutationOutcomeUnknownError,
        SampleRateTopologyUnsupportedError,
    )

    current_sample_rate, supported_sample_rates = await application.probe_sample_rate_status(device)
    if sample_rate not in supported_sample_rates:
        raise SampleRateTopologyUnsupportedError(
            f"requested sample rate {sample_rate} is not supported; device reports {supported_sample_rates}"
        )
    capacity = SimpleNamespace(transmit_channel_count=0, receive_channel_count=0)
    preflight = SimpleNamespace(
        current_sample_rate_hertz=current_sample_rate,
        target_sample_rate_hertz=sample_rate,
        target_capacity=capacity,
        reversible_receiver_clipping=(),
        destructive_transmitter_membership_loss=(),
        uncharacterized_transmitter_flows=(),
        topology_characterized=current_sample_rate != sample_rate,
        to_dict=lambda: {"target_sample_rate_hertz": sample_rate},
    )
    if current_sample_rate == sample_rate:
        return SimpleNamespace(
            preflight=preflight,
            observed_sample_rate_hertz=current_sample_rate,
            observed_supported_sample_rates_hertz=tuple(supported_sample_rates),
            resulting_snapshot=None,
            changed=False,
            to_dict=lambda: {
                "success": True,
                "changed": False,
                "preflight": preflight.to_dict(),
                "readback": {"sample_rate_hertz": current_sample_rate},
            },
        )
    try:
        application._record("set_sample_rate", device, sample_rate)
    except OSError as exception:
        raise SampleRateTopologyMutationOutcomeUnknownError(
            f"sample-rate mutation failed after it was attempted; device state is unknown: {exception}",
            preflight,
        ) from exception
    observed_sample_rate, observed_supported_sample_rates = await application.probe_sample_rate_status(device)
    if observed_sample_rate != sample_rate:
        raise SampleRateTopologyChangedButUnverifiedError(
            f"sample-rate change was sent, but complete post-write verification failed: "
            f"device reports {observed_sample_rate} Hz instead of {sample_rate} Hz",
            preflight,
            observed_sample_rate,
        )
    resulting_snapshot = SimpleNamespace(capacity=capacity)
    return SimpleNamespace(
        preflight=preflight,
        observed_sample_rate_hertz=observed_sample_rate,
        observed_supported_sample_rates_hertz=tuple(observed_supported_sample_rates),
        resulting_snapshot=resulting_snapshot,
        changed=current_sample_rate != observed_sample_rate,
        to_dict=lambda: {
            "success": True,
            "changed": True,
            "preflight": preflight.to_dict(),
            "readback": {"sample_rate_hertz": observed_sample_rate},
        },
    )
