# Own video recording artifact lifetime

Status: proposed October 30, 2026; implementation awaits Chris's acceptance.

The first bounded sustained-video observation recorded a synchronous `create_new`
open of about 2.559 seconds in each receiver media task. Both tasks then
reached the existing decoder queue limit. The packet archive accepted all 951
source RTP datagrams, so the observation establishes receiver-task recording
I/O as the immediate boundary. It does not identify the storage cause or
qualify a new layout.

Each receiver generation has one runtime-owned recording supervisor. It creates
and owns the artifact root, payload and index files, terminal receipt, writer
thread and path lease. Receiver media work owns only a sole nonblocking ingress
sender and a writer-status subscription. Media code does not open, write,
flush, rename, index or finalize recording artifacts.

Recorded frames use one append-only NV12 payload artifact and a paired
append-only JSONL index. The index records the ordinal, frame identity,
geometry, pixel format, selected time, completion time, byte offset and byte
length. Frame and byte ingress limits are explicit configuration when frame
recording is requested. A full ingress is a visible receiver failure; it does
not wait for capacity or silently drop an accepted record.

The writer receives a final summary in FIFO order but does not treat it as
ingress closure. It continues receiving until the sole sender closes, then
freezes accepted counts, drains accepted work or retains failed-frame identity
and accepted-versus-paired boundaries in an incomplete manifest. An ingress
rejection records its unaccepted ordinal, identity, selected time, payload size
and admission reason separately from accepted-but-unwritten work. A successful
manifest is published only after the payload and index have drained. The
metadata-only case is also supervised, so no caller writes a competing
configuration or lifecycle artifact.

An owned-multicast receiver does not complete until both its media task and
recording supervisor have ended. A successor generation cannot start while an
older recording supervisor retains the receiver path. The terminal receipt
remains in the supervisor rather than an abortable watcher; a watcher may be
restarted without losing the thread join path. On retirement timeout, the
runtime records quiescence as unknown and continues draining and joining the
writer before it returns an error. It does not detach a writer or reuse that
generation's path.

This adds one writer thread and bounded retained frame memory per active
recording generation. A filesystem operation blocked indefinitely can prevent
the process entry point from returning after it has reported unknown
quiescence. Moving the writer into the receiver task would permit a bounded
return but detaches the task's filesystem ownership. Retaining per-frame files
behind a worker would remove media-task blocking but preserves filesystem
open/close churn and weakens the explicit byte-range artifact contract.
