use std::fs::{File, OpenOptions};
use std::io::{Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;

use anyhow::{Context, Result, anyhow, bail, ensure};
use serde::Serialize;
use tokio::sync::{oneshot, watch};
use tokio::time::Instant;

use super::configuration::Configuration;
use super::{FrameIdentity, Summary};

#[derive(Clone, Debug, Default)]
pub(crate) struct Status {
    pub active: bool,
    pub selected: u64,
    pub queued: u64,
    pub payload_written: u64,
    pub index_written: u64,
    pub drained_frames: u64,
    pub drained: bool,
    pub accepted_last_ordinal: Option<u64>,
    pub written_last_ordinal: Option<u64>,
    pub failed_ordinal: Option<u64>,
    pub failed_identity: Option<FrameIdentity>,
    pub accepted_not_fully_recorded_frames: u64,
    pub failure: Option<String>,
}

pub(crate) struct Supervisor {
    terminal: oneshot::Receiver<Result<Terminal>>,
    completion: watch::Receiver<bool>,
    watching: Arc<std::sync::atomic::AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}

pub(crate) struct TerminalWatcher {
    completion: watch::Receiver<bool>,
    watching: Arc<std::sync::atomic::AtomicBool>,
}

pub(crate) struct Client {
    commands: mpsc::SyncSender<Command>,
    bytes: Arc<ByteBudget>,
    maximum_frames: usize,
    queued_frames: Arc<AtomicUsize>,
    accepted: Arc<AtomicU64>,
    accepted_last_ordinal: Arc<AtomicU64>,
    status: watch::Receiver<Status>,
}

pub(crate) struct Session {
    pub client: Client,
    pub origin: Instant,
    pub generation: String,
}

struct Writer {
    directory: PathBuf,
    configuration: Configuration,
    record_frames: bool,
    status_sender: watch::Sender<Status>,
    origin: Instant,
    accepted: Arc<AtomicU64>,
    accepted_last_ordinal: Arc<AtomicU64>,
}

#[derive(Debug)]
pub(crate) struct Terminal {
    pub status: Status,
}

struct ByteBudget {
    maximum: usize,
    reserved: AtomicUsize,
}

struct Reservation {
    budget: Arc<ByteBudget>,
    bytes: usize,
}

pub(crate) struct FrameDescriptor {
    pub(crate) identity: FrameIdentity,
    pub(crate) width: u32,
    pub(crate) height: u32,
    pub(crate) colorimetry: String,
    pub(crate) selected_nanoseconds: i64,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct RecordingIngressRejection {
    pub(crate) ordinal: u64,
    pub(crate) identity: FrameIdentity,
    pub(crate) selected_nanoseconds: i64,
    pub(crate) payload_bytes: usize,
    pub(crate) reason: String,
}

struct Frame {
    ordinal: u64,
    descriptor: FrameDescriptor,
    bytes: Vec<u8>,
    _reservation: Reservation,
    _frame_reservation: FrameReservation,
}

struct FrameReservation {
    queued_frames: Arc<AtomicUsize>,
}

enum Command {
    Activate {
        summary: Summary,
        reply: oneshot::Sender<Result<()>>,
    },
    Frame(Frame),
    Finish {
        summary: Summary,
    },
}

impl ByteBudget {
    fn reserve(self: &Arc<Self>, bytes: usize) -> Result<Reservation> {
        ensure!(bytes <= self.maximum, "recording frame exceeds the explicit byte budget");
        let mut current = self.reserved.load(Ordering::Acquire);
        loop {
            let next = current.checked_add(bytes).context("recording byte reservation overflow")?;
            ensure!(next <= self.maximum, "recording byte queue is full");
            match self.reserved.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Ok(Reservation {
                        budget: Arc::clone(self),
                        bytes,
                    });
                }
                Err(observed) => current = observed,
            }
        }
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.budget.reserved.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

impl Drop for FrameReservation {
    fn drop(&mut self) {
        self.queued_frames.fetch_sub(1, Ordering::AcqRel);
    }
}

impl RecordingIngressRejection {
    fn from_descriptor(
        ordinal: u64,
        descriptor: FrameDescriptor,
        payload_bytes: usize,
        reason: String,
    ) -> Box<Self> {
        Box::new(Self {
            ordinal,
            identity: descriptor.identity,
            selected_nanoseconds: descriptor.selected_nanoseconds,
            payload_bytes,
            reason,
        })
    }

    fn from_frame(frame: Frame, reason: String) -> Box<Self> {
        let Frame {
            ordinal,
            descriptor,
            bytes,
            ..
        } = frame;
        Self::from_descriptor(ordinal, descriptor, bytes.len(), reason)
    }
}

impl std::fmt::Display for RecordingIngressRejection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.reason)
    }
}

impl std::error::Error for RecordingIngressRejection {}

impl Supervisor {
    pub(crate) fn start(
        directory: PathBuf,
        configuration: Configuration,
        record_frames: bool,
        origin: Instant,
    ) -> Result<(Self, Client)> {
        if record_frames {
            ensure!(
                configuration.recording.is_some(),
                "recorded frames require explicit independent recording limits"
            );
        }
        let limits = configuration.recording.clone();
        let maximum_frames = limits.as_ref().map(|value| value.maximum_pending_frames).unwrap_or(0);
        let maximum_bytes = limits.as_ref().map(|value| value.maximum_pending_bytes).unwrap_or(0);
        let capacity =
            maximum_frames.checked_add(1).context("recording command capacity overflow")?;
        let (commands, receiver) = mpsc::sync_channel(capacity.max(1));
        let (status_sender, status) = watch::channel(Status::default());
        let (terminal_sender, terminal) = oneshot::channel();
        let (completion_sender, completion) = watch::channel(false);
        let bytes = Arc::new(ByteBudget {
            maximum: maximum_bytes,
            reserved: AtomicUsize::new(0),
        });
        let queued_frames = Arc::new(AtomicUsize::new(0));
        let accepted = Arc::new(AtomicU64::new(0));
        let accepted_last_ordinal = Arc::new(AtomicU64::new(u64::MAX));
        let writer_accepted = Arc::clone(&accepted);
        let writer_accepted_last_ordinal = Arc::clone(&accepted_last_ordinal);
        let thread = thread::Builder::new()
            .name("netaudio-video-recording".to_owned())
            .spawn(move || {
                let result = Writer {
                    directory,
                    configuration,
                    record_frames,
                    status_sender,
                    origin,
                    accepted: writer_accepted,
                    accepted_last_ordinal: writer_accepted_last_ordinal,
                }
                .run(receiver);
                let _ = terminal_sender.send(result);
                completion_sender.send_replace(true);
            })
            .context("start owned video recording writer")?;
        Ok((
            Self {
                terminal,
                completion,
                watching: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                thread: Some(thread),
            },
            Client {
                commands,
                bytes,
                maximum_frames,
                queued_frames,
                accepted,
                accepted_last_ordinal,
                status,
            },
        ))
    }

    pub(crate) fn watcher(&self) -> Result<Option<TerminalWatcher>> {
        ensure!(self.thread.is_some(), "recording writer handle missing");
        if self.watching.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire).is_err()
        {
            return Ok(None);
        }
        Ok(Some(TerminalWatcher {
            completion: self.completion.clone(),
            watching: Arc::clone(&self.watching),
        }))
    }

    pub(crate) fn join(&mut self) -> Result<Terminal> {
        let terminal = self.terminal.try_recv();
        let thread = self.thread.take().context("recording writer handle missing")?;
        let joined = thread.join().map_err(|_| anyhow!("recording writer panicked"));
        match (terminal, joined) {
            (Ok(Ok(terminal)), Ok(())) => Ok(terminal),
            (Ok(Err(error)), Ok(())) => Err(error),
            (Ok(Ok(_)), Err(error)) => Err(error),
            (Ok(Err(error)), Err(join)) => {
                Err(error.context(format!("recording writer join also failed: {join:#}")))
            }
            (Err(error), Ok(())) => {
                Err(anyhow!("recording terminal result is unavailable: {error}"))
            }
            (Err(error), Err(join)) => Err(anyhow!(
                "recording terminal result is unavailable: {error}; recording writer join also failed: {join:#}"
            )),
        }
    }

    pub(crate) async fn terminal(&mut self) -> Result<Terminal> {
        let observed = wait_for_completion(&mut self.completion).await;
        let joined = self.join();
        match (observed, joined) {
            (Ok(()), Ok(terminal)) => Ok(terminal),
            (Err(error), Ok(_)) => Err(error),
            (Ok(()), Err(error)) => Err(error),
            (Err(error), Err(join)) => {
                Err(error.context(format!("recording writer join also failed: {join:#}")))
            }
        }
    }
}

async fn wait_for_completion(completion: &mut watch::Receiver<bool>) -> Result<()> {
    loop {
        if *completion.borrow_and_update() {
            return Ok(());
        }
        completion
            .changed()
            .await
            .context("recording writer ended before its completion notification")?;
    }
}

impl TerminalWatcher {
    pub(crate) async fn wait(mut self) -> Result<()> {
        wait_for_completion(&mut self.completion).await
    }
}

impl Drop for TerminalWatcher {
    fn drop(&mut self) {
        self.watching.store(false, Ordering::Release);
    }
}

impl Client {
    pub(crate) fn status(&self) -> watch::Receiver<Status> {
        self.status.clone()
    }

    pub(crate) async fn activate(&self, summary: Summary) -> Result<()> {
        let (reply, received) = oneshot::channel();
        self.commands
            .try_send(Command::Activate {
                summary,
                reply,
            })
            .map_err(|error| anyhow!("recording activation command was not accepted: {error}"))?;
        received
            .await
            .context("recording writer ended before active manifest acknowledgement")??;
        Ok(())
    }

    pub(crate) fn enqueue(
        &self,
        ordinal: u64,
        descriptor: FrameDescriptor,
        bytes: Vec<u8>,
    ) -> std::result::Result<(), Box<RecordingIngressRejection>> {
        let frame_bytes = bytes.len();
        if self.maximum_frames == 0 {
            return Err(RecordingIngressRejection::from_descriptor(
                ordinal,
                descriptor,
                frame_bytes,
                "recording frame ingress is disabled".to_owned(),
            ));
        }
        let reservation = match self.bytes.reserve(frame_bytes) {
            Ok(reservation) => reservation,
            Err(error) => {
                return Err(RecordingIngressRejection::from_descriptor(
                    ordinal,
                    descriptor,
                    frame_bytes,
                    format!("{error:#}"),
                ));
            }
        };
        let previous = self.queued_frames.fetch_add(1, Ordering::AcqRel);
        if previous >= self.maximum_frames {
            self.queued_frames.fetch_sub(1, Ordering::AcqRel);
            drop(reservation);
            return Err(RecordingIngressRejection::from_descriptor(
                ordinal,
                descriptor,
                frame_bytes,
                "recording frame queue is full".to_owned(),
            ));
        }
        let frame = Frame {
            ordinal,
            descriptor,
            bytes,
            _reservation: reservation,
            _frame_reservation: FrameReservation {
                queued_frames: Arc::clone(&self.queued_frames),
            },
        };
        match self.commands.try_send(Command::Frame(frame)) {
            Ok(()) => {
                self.accepted.fetch_add(1, Ordering::AcqRel);
                self.accepted_last_ordinal.store(ordinal, Ordering::Release);
                Ok(())
            }
            Err(mpsc::TrySendError::Full(Command::Frame(frame))) => {
                Err(RecordingIngressRejection::from_frame(
                    frame,
                    "recording frame command queue is full".to_owned(),
                ))
            }
            Err(mpsc::TrySendError::Disconnected(Command::Frame(frame))) => {
                Err(RecordingIngressRejection::from_frame(
                    frame,
                    "recording writer command receiver closed".to_owned(),
                ))
            }
            Err(mpsc::TrySendError::Full(_)) | Err(mpsc::TrySendError::Disconnected(_)) => {
                unreachable!("the frame ingress only sends frame commands")
            }
        }
    }

    pub(crate) fn finish(&mut self, summary: Summary) -> Result<()> {
        self.commands
            .try_send(Command::Finish {
                summary,
            })
            .map_err(|error| anyhow!("recording finalization command was not accepted: {error}"))?;
        Ok(())
    }
}

#[derive(Default)]
struct WriterState {
    status: Status,
    summary: Option<Summary>,
}

impl Writer {
    fn run(self, receiver: mpsc::Receiver<Command>) -> Result<Terminal> {
        let mut state = WriterState::default();
        if let Err(error) =
            std::fs::create_dir(&self.directory).context("create a new video output directory")
        {
            state.status.failure = Some(format!("{error:#}"));
            self.status_sender.send_replace(state.status);
            return Err(error);
        }
        let result = self.write_session(receiver, &mut state);
        if let Err(error) = result {
            append_failure(&mut state.status, format!("{error:#}"));
        }
        if state.status.failure.is_some() {
            state.status.drained = false;
            if let Some(summary) = &mut state.summary {
                summary.apply_recording_status(&state.status);
            }
            if let Err(error) = write_incomplete_manifest(&self.directory, &state) {
                append_failure(
                    &mut state.status,
                    format!("cannot retain incomplete recording manifest: {error:#}"),
                );
            }
        }
        self.status_sender.send_replace(state.status.clone());
        if let Some(failure) = &state.status.failure {
            bail!("recording writer failed: {failure}");
        }
        Ok(Terminal {
            status: state.status,
        })
    }

    fn write_session(
        &self,
        receiver: mpsc::Receiver<Command>,
        state: &mut WriterState,
    ) -> Result<()> {
        let Self {
            directory,
            configuration,
            record_frames,
            status_sender,
            origin,
            accepted,
            accepted_last_ordinal,
        } = self;
        write_json_new(&directory.join("configuration.json"), configuration)?;
        write_json_new(
            &directory.join("starting.json"),
            &serde_json::json!({"state": "starting"}),
        )?;
        let mut raw = record_frames
            .then(|| {
                OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(directory.join("frames.nv12.partial"))
            })
            .transpose()
            .context("create recording payload artifact")?;
        let mut index = record_frames
            .then(|| {
                OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(directory.join("frames.index.jsonl.partial"))
            })
            .transpose()
            .context("create recording index artifact")?;
        while let Ok(command) = receiver.recv() {
            match command {
                Command::Activate {
                    summary,
                    reply,
                } => {
                    let result = (|| {
                        ensure!(!reply.is_closed(), "recording activation was cancelled");
                        ensure!(
                            state.status.failure.is_none(),
                            "recording writer already failed before activation"
                        );
                        ensure!(!state.status.active, "recording writer was activated twice");
                        write_json_new(&directory.join("active.json"), &summary)?;
                        state.status.active = true;
                        Ok(())
                    })();
                    if let Err(error) = &result {
                        append_failure(&mut state.status, format!("{error:#}"));
                    }
                    status_sender.send_replace(state.status.clone());
                    let _ = reply.send(result);
                }
                Command::Frame(frame) => {
                    let observed_accepted =
                        frame.ordinal.checked_add(1).context("recording ordinal exhausted")?;
                    state.status.queued = observed_accepted.max(accepted.load(Ordering::Acquire));
                    state.status.accepted_last_ordinal = state.status.queued.checked_sub(1);
                    if state.status.failure.is_some() {
                        retain_unwritten(&mut state.status, &frame);
                        continue;
                    }
                    let result = (|| {
                        ensure!(state.status.active, "recording frame arrived before activation");
                        ensure!(
                            frame.ordinal == state.status.index_written,
                            "recording ordinal is not contiguous"
                        );
                        write_frame(
                            raw.as_mut().context("recording payload writer is unavailable")?,
                            index.as_mut().context("recording index writer is unavailable")?,
                            &frame,
                            &mut state.status,
                            *origin,
                        )
                    })();
                    if let Err(error) = result {
                        append_failure(&mut state.status, format!("{error:#}"));
                        retain_unwritten(&mut state.status, &frame);
                    }
                    status_sender.send_replace(state.status.clone());
                }
                Command::Finish {
                    summary,
                } => {
                    if let Some(failure) = summary.failure() {
                        append_failure(&mut state.status, failure.to_owned());
                    }
                    state.status.selected = summary.selected_frames();
                    if state.summary.replace(summary).is_some() {
                        append_failure(
                            &mut state.status,
                            "recording ingress sent more than one final summary".to_owned(),
                        );
                    }
                    status_sender.send_replace(state.status.clone());
                }
            }
        }
        state.status.queued = accepted.load(Ordering::Acquire);
        let last_ordinal = accepted_last_ordinal.load(Ordering::Acquire);
        state.status.accepted_last_ordinal = (last_ordinal != u64::MAX).then_some(last_ordinal);
        state.status.accepted_not_fully_recorded_frames = state
            .status
            .queued
            .checked_sub(state.status.index_written)
            .context("recording paired index count exceeds accepted ingress count")?;
        if state.summary.is_none() {
            append_failure(
                &mut state.status,
                "recording ingress ended without final summary".to_owned(),
            );
        }
        if let Some(summary) = &mut state.summary {
            summary.apply_recording_status(&state.status);
        }
        let mut synchronization_failures = Vec::new();
        if let Some(raw) = raw
            && let Err(error) = raw.sync_all().context("sync recording payload")
        {
            synchronization_failures.push(format!("{error:#}"));
        }
        if let Some(index) = index
            && let Err(error) = index.sync_all().context("sync recording index")
        {
            synchronization_failures.push(format!("{error:#}"));
        }
        for failure in synchronization_failures {
            append_failure(&mut state.status, failure);
        }
        if state.status.failure.is_some() {
            return Ok(());
        }
        ensure!(
            state.status.payload_written == state.status.queued
                && state.status.index_written == state.status.queued,
            "recording completion does not account for every accepted frame"
        );
        if *record_frames {
            std::fs::rename(directory.join("frames.nv12.partial"), directory.join("frames.nv12"))
                .context("publish recording payload")?;
            std::fs::rename(
                directory.join("frames.index.jsonl.partial"),
                directory.join("frames.index.jsonl"),
            )
            .context("publish recording index")?;
        }
        sync_directory(directory).context("sync recording directory before completed manifest")?;
        state.status.drained_frames = state.status.index_written;
        state.status.drained = true;
        if let Some(summary) = &mut state.summary {
            summary.apply_recording_status(&state.status);
        }
        publish_final_manifest(
            directory,
            &serde_json::json!({
                "state": "drained",
                "selected": state.status.selected,
                "queued": state.status.queued,
                "payload_written": state.status.payload_written,
                "index_written": state.status.index_written,
                "drained_frames": state.status.drained_frames,
                "drained": state.status.drained,
                "accepted_last_ordinal": state.status.accepted_last_ordinal,
                "written_last_ordinal": state.status.written_last_ordinal,
                "summary": &state.summary,
            }),
        )
    }
}

fn write_frame(
    raw: &mut File,
    index: &mut File,
    frame: &Frame,
    status: &mut Status,
    origin: Instant,
) -> Result<()> {
    let offset = raw.stream_position().context("read recording payload offset")?;
    raw.write_all(&frame.bytes).context("write recording payload")?;
    raw.flush().context("flush recording payload")?;
    status.payload_written += 1;
    write_index_record(
        index,
        &serde_json::json!({
            "kind": "descriptor",
            "ordinal": frame.ordinal,
            "identity": &frame.descriptor.identity,
            "width": frame.descriptor.width,
            "height": frame.descriptor.height,
            "colorimetry": &frame.descriptor.colorimetry,
            "pixel_format": "NV12",
            "selected_nanoseconds": frame.descriptor.selected_nanoseconds,
            "offset": offset,
            "bytes": frame.bytes.len(),
        }),
    )?;
    let completed_nanoseconds = clock_sample(origin)?;
    write_index_record(
        index,
        &serde_json::json!({
            "kind": "completion_observation",
            "ordinal": frame.ordinal,
            "completed_nanoseconds": completed_nanoseconds,
        }),
    )?;
    status.index_written += 1;
    status.written_last_ordinal = Some(frame.ordinal);
    Ok(())
}

fn write_index_record(index: &mut File, record: &impl Serialize) -> Result<()> {
    let mut bytes = serde_json::to_vec(record)?;
    bytes.push(b'\n');
    index.write_all(&bytes).context("write recording index record")?;
    index.flush().context("flush recording index record")?;
    Ok(())
}

fn retain_unwritten(status: &mut Status, frame: &Frame) {
    if status.failed_ordinal.is_none() {
        status.failed_ordinal = Some(frame.ordinal);
        status.failed_identity = Some(frame.descriptor.identity.clone());
    }
    status.accepted_not_fully_recorded_frames += 1;
}

fn append_failure(status: &mut Status, failure: String) {
    status.failure = Some(match status.failure.take() {
        Some(previous) if previous == failure => previous,
        Some(previous) => format!("{previous}; {failure}"),
        None => failure,
    });
}

fn write_incomplete_manifest(directory: &Path, state: &WriterState) -> Result<()> {
    let accepted_but_unwritten = state.status.accepted_last_ordinal.and_then(|last| {
        let first = state
            .status
            .written_last_ordinal
            .and_then(|written| written.checked_add(1))
            .unwrap_or(0);
        (first <= last).then_some(serde_json::json!({
            "first_ordinal": first,
            "last_ordinal": last,
            "count": state.status.accepted_not_fully_recorded_frames,
        }))
    });
    write_json_new(
        &directory.join("incomplete-manifest.json"),
        &serde_json::json!({
            "state": "failed",
            "selected": state.status.selected,
            "accepted": state.status.queued,
            "queued": state.status.queued,
            "payload_written": state.status.payload_written,
            "index_written": state.status.index_written,
            "drained_frames": state.status.drained_frames,
            "drained": false,
            "accepted_not_fully_recorded_frames": state.status.accepted_not_fully_recorded_frames,
            "accepted_but_unwritten": accepted_but_unwritten,
            "rejected_at_ingress": state.summary.as_ref().and_then(Summary::recording_ingress_rejection),
            "failed_ordinal": state.status.failed_ordinal,
            "failed_identity": &state.status.failed_identity,
            "failure": &state.status.failure,
            "summary": &state.summary,
        }),
    )?;
    sync_directory(directory).context("sync incomplete recording manifest")
}

fn publish_final_manifest(directory: &Path, value: &impl Serialize) -> Result<()> {
    let partial = directory.join("manifest.json.partial");
    let manifest = directory.join("manifest.json");
    write_json_new(&partial, value)?;
    std::fs::rename(&partial, &manifest).context("publish completed recording manifest")?;
    if let Err(error) = sync_directory(directory).context("sync completed recording manifest") {
        let preserved = std::fs::rename(&manifest, directory.join("manifest.json.uncommitted"))
            .context("preserve uncommitted recording manifest");
        return match preserved {
            Ok(()) => Err(error),
            Err(preservation) => Err(error.context(format!("{preservation:#}"))),
        };
    }
    Ok(())
}

fn sync_directory(directory: &Path) -> Result<()> {
    File::open(directory)
        .context("open recording directory for synchronization")?
        .sync_all()
        .context("sync recording directory")
}

pub(super) fn write_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    serde_json::to_writer_pretty(&mut file, value)?;
    file.write_all(b"\n")?;
    Ok(())
}

fn write_json_new(path: &Path, value: &impl Serialize) -> Result<()> {
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    serde_json::to_writer_pretty(&mut file, value)?;
    file.write_all(b"\n")?;
    file.flush()?;
    file.sync_all()?;
    Ok(())
}

fn clock_sample(origin: Instant) -> Result<i64> {
    i64::try_from(Instant::now().duration_since(origin).as_nanos())
        .context("monotonic recording clock exceeded its range")
}
