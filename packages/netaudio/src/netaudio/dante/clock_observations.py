from netaudio import core


def clock_tracker(device) -> core.ClockTracker:
    tracker = getattr(device, "_clock_tracker", None)
    if tracker is None:
        tracker = core.ClockTracker()
        device._clock_tracker = tracker

    return tracker
