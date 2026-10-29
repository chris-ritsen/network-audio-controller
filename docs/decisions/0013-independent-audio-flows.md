# Own audio flows independently of host adapters

Status: requested multiple-flow requirement accepted October 28, 2026;
implementation proposed and awaiting user acceptance.

The requested virtual soundcard stage needs simultaneous flows from multiple
sources, independent release and reacquisition, and correct host channel
placement. The previous endpoint had one allocation and output generation in
each direction. Repeating that endpoint would duplicate host adapters and
reset unrelated channels during route changes.

Configure transmit and receive flow capacities separately from channel counts
and slots per flow. Defaults are eight flows per direction. Advertise those
capacities and publish paginated live inventories with actual resource IDs.
Allocation handles retain distinct durable generations. Each directional flow
owns media ports; two ports per flow provide bounded reuse headroom under the
existing quarantine. Exhaustion remains an explicit unavailable result.

One capture adapter supplies a common sample sequence to independently paced
transmit flows. Earliest deadlines select the next flow, with rotating ties.
A flow that loses receiver activity or exceeds its queue or capture-age budget
is suspended independently. A host adapter or shared clock failure still
invalidates all dependent media.

Receive groups use source identity and stable local-channel blocks bounded by
configured slots per flow, further split by actual source slot capacity.
Duplicate sources within a group occupy one slot with multiple receivers.
Unchanged groups retain discovery generations and allocation ownership.
Each group has independent acquisition, request correlation, packet admission,
playout, failure state and owned STOP. Uncertain mutations remain fenced and
are never automatically replayed. Configured capacity is separate from current
allocation occupancy, successful media and host throughput.

The host output has one sample timeline. Per-flow buffers use compact local
maps and merge into disjoint endpoint channels at the same sample coordinates.
The initial output starts at the first primed flow's sample coordinate. Later
flows join that timeline; unready channels supply explicit local silence.
Per-channel generations fence queued samples on release or replacement without
resetting sibling channels or the shared adapter. An independent-clock
resampler preserves its shared phase and ratio while changed channels emit
local silence until its two-filter-length retained input has been replaced.
The epoch fence covers queued output and filter history. Consumption receipts
retain per-channel ownership and received-sample counts. Output receipts are joined
back to the flow generations that supplied each block. Packet counts,
flow-delivered frames and host-output frames remain distinct.

These decisions supersede the single-flow restriction in decision 0012 while
preserving its coherent rate/width model, clock guards, deadline limits and
native float precision constraints. Multiple adapters, global route-generation
resets, and a fixed four-by-48 capacity were rejected. This implementation does
not qualify foreign DVS peers, system virtual-audio drivers, physical output,
or sustained operation on every configured resource envelope.
