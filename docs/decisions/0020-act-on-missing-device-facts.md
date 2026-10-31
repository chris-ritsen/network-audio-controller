# Act when device facts are missing

Status: accepted October 31, 2026.

Writes were refused whenever a fact could not be read: the lock state, a
capability bit, a managed operation permission, the controller's own MAC
address, the transmit-flow inventory, or a sample-rate topology prediction.
A Mac that hides interface MAC addresses could not change a sample rate, and
an AVIO behind DDM 1.9, which never answers the lock query, could not change
most settings. Reported as GitHub #68 and #67. The user's direction: "We
don't want to block competent users from doing things like this", "We should
assume unlocked by default", and "make it as easy as possible for the user to
do the thing in this situation. They shouldn't have to hunt for stupid flags
or get blocked because of some nonsense."

A write is sent unless the device or the managed server has reported a fact
that rules it out: a lock it reported, an operation it reported unsupported,
a permission it denied, a value outside a list it advertised, or a fixed
update mode. An unknown lock state is treated as unlocked, and writes no
longer query the lock first. An unknown capability, a missing managed
permission, and an unknown redundancy or interface state no longer refuse.
Commands that need the controller's MAC address use the interface address
when it can be read and the locally administered address 02:00:5E:00:00:01
otherwise; devices answer settings queries regardless of the value. One
malformed mDNS value is ignored instead of stopping device discovery.

Sample-rate topology is informational. The change is sent when the current
rate, the channel capacity or the flow inventory cannot be read, and when the
predicted effect on transmit flows is unproven or removes flow members. The
prediction and any reading that failed are reported with the result. The
`confirm_destructive` request field and the hidden `--confirm-destructive`
flag are still accepted and have no effect.

The device remains the authority. When a device rejects a write, for example
because it is locked, the rejection is reported. A sent write whose readback disagrees is still
reported as unverified or contradicted, never as success.
