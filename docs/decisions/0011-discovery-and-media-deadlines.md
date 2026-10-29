# Keep discovery work outside media deadlines

Status: implementation proposed October 28, 2026; awaiting user acceptance.

Multichannel network qualification exposed repeated comparisons of unchanged
source TXT records on each audio event. Resolving discovery updates on the media
thread also caused ordinary discovery requests to interrupt otherwise continuous
audio. Increasing receive buffering would not remove this control work from the
transmit deadline.

Share immutable candidate context across discovery, acquisition and the receive
flow. Keep record expiry separate. Reuse context only after a complete comparison
on a discovery update. Recompute DNS query and expiry schedules when records or
their deadlines change, rather than whenever an unrelated audio future wakes.

An owned discovery worker resolves records and route selections outside the
media executor. Bounded latest-state channels coalesce obsolete work. Route
generations reject results from replaced selections. Discovery generations
preserve an intervening invalidation even if its individual notification was
coalesced before the media executor consumed it. Clock and record freshness are
still checked at use. Worker failure stops the endpoint; shutdown joins its
worker and releases the resolver.

Receive buffer preparation and disposal also run in an owned worker. At most one
preparation is outstanding. A route or clock invalidation revokes its result;
allocation is sent only after the completed buffer and current context agree.
Shutdown closes the bounded channels and joins the worker.

Blocking endpoint workers explicitly use ordinary scheduling. They do not inherit
FIFO priority from a launcher that gives the media dispatcher real-time priority.
The mDNS publisher is created from ordinary-priority work so its own thread
inherits that policy.
The media thread and native device callbacks retain their configured policies.
Failure to establish worker scheduling is reported through its owned lifecycle.
No new privilege or system scheduling setting is requested.

This adds two background workers per endpoint and explicit observation ownership.
It reduces repeated control work and isolates burst costs without changing media
packetization, age limits, missing-frame handling or playout deadlines. It does
not guarantee deadlines under arbitrary host load. Busy waiting, weaker failure
criteria and a channel cap were rejected because they would obscure the cause.
