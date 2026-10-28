# Configure one coherent network audio format

Status: implementation proposed October 28, 2026; awaiting user acceptance.

The endpoint previously assumed 48 kHz signed 24-bit PCM across discovery,
control, sample clocks, WAVE files and native audio conversion. The approved
native-v2 contracts already specify positive sample rates and signed 16-, 24-
and 32-bit samples. These are own implementation capabilities, not missing
foreign protocol research.

Configure the sample rate and width once in the endpoint profile. Carry the
same typed sample format through channel and flow replies, discovery,
allocation eligibility, packet geometry, timestamps, receive placement and
audio adapters. Channel counts remain directional. Changing format requires
restarting the endpoint; this does not add remote device configuration.

The implementation accepts 8000 through 192000 Hz with explicit 16-, 24- or
32-bit signed samples. Existing packet size, sender spacing and buffer
budgets still apply. Higher rates require enough frames per packet to respect
the sender spacing budget. Invalid geometry fails before publication.
Native devices sharing the network clock must use the same sample rate.
Independent clocks use the existing resampler with the selected network rate.

Processing uses 48-frame multiples scaled by the ceiling of rate/48000, keeping
the established one-millisecond block at 48, 96 and 192 kHz. Capture, playout,
native conversion and receipt ownership share that geometry. An initial
192 kHz trial with fixed 48-frame blocks missed its 250-microsecond delivery
budget; retaining that geometry would make host scheduling four times more
demanding. Wire packetization remains independent and receive latency is not
increased. Priming follows the processing geometry; missing-audio tolerance
retains its 50-millisecond duration.

Playout starts at the first admitted sample coordinate plus configured receive
latency. It does not pad to a global processing-block boundary, which would
introduce initial silence for rates such as 44.1 kHz.

WAVE transport retains integer sample precision. Native float callbacks and
resampling cannot retain every signed 32-bit integer exactly; conversion clamps
to the selected width and exposes the actual received samples for validation.
An advertised format is not proof of hardware or foreign DVS compatibility.

This stage retains one flow per direction. Multiple independently owned flows
require a separate coherent ownership change. Silently converting incompatible
network formats or advertising a format while sending another was rejected.
