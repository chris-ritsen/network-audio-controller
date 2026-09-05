# Subscription status observations

`status-observations.json` is a minimal projection of the observation-only
handoff `a32-ddm-status-enumeration-20260905-01`. Its adjacent provenance file
records source SHA-256 digests, scope, exclusions and the transformation.
The grouped API identifiers, messages, summaries and representative enum error
are unchanged evidence. Device identities and credentials are absent.

Evidence class: **observed**. A synthetic A32 4.0.8.2 target emitted each value
from 0x0000 through 0x00ff while receiver health remained 0x0101. The observations
show the DDM deployment's classifications on 2026-09-05. The DDM build was not
independently identified. These results do not demonstrate natural hardware
failures, successful media transport, or a universal status precedence rule.
The stimulus was prepared using excluded internals; exporting external responses
does not remove that provenance limitation. Firmware, executable internals,
RAM dumps, injection scripts and treatment details were excluded from this work.

Code 1 returned DYNAMIC in the sweep. Two supplemental cleanup records returned
UNRESOLVED at receiver health 0. Cleanup used a single API sample followed by
wire readback, unlike the sweep's two samples bounded by binary reads. The
fixture retains those records separately. Other code-1 receiver contexts and
all numeric meanings above 0x00ff remain **unknown**.

The historical warning presentation summarized by the handoff supports keeping
success and additional warnings separate. It does not establish a wire-level
warning bit or a complete receiver-health decoder. Warning round-trip tests use
application model data; they do not claim an observed numeric warning mapping.
