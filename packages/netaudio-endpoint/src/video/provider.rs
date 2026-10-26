use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use gstreamer as media;
use gstreamer::prelude::*;
use gstreamer_video::prelude::*;
use tokio::sync::{mpsc, watch};

use super::configuration::Configuration;

pub struct Decoder {
    pipeline: media::Pipeline,
    input: gstreamer_app::AppSrc,
    accepting: Arc<AtomicBool>,
    maximum_queued_bytes: u64,
    maximum_queued_frames: u64,
}

pub struct Output {
    pub samples: mpsc::Receiver<media::Sample>,
    pub failure: watch::Receiver<Option<String>>,
}

impl Decoder {
    pub async fn start(configuration: &Configuration, timeout: Duration) -> Result<(Self, Output)> {
        let configuration = configuration.clone();
        let accepting = Arc::new(AtomicBool::new(true));
        let setup_accepting = accepting.clone();
        let setup = tokio::task::spawn_blocking(move || Self::new(&configuration, setup_accepting));

        match tokio::time::timeout(timeout, setup).await {
            Ok(result) => result.context("decoder setup task failed")?,
            Err(error) => {
                accepting.store(false, Ordering::Release);
                Err(error).context("decoder setup deadline expired; quiescence is unknown")
            }
        }
    }

    fn new(configuration: &Configuration, accepting: Arc<AtomicBool>) -> Result<(Self, Output)> {
        media::init().context("initialize GStreamer")?;
        let pipeline = media::Pipeline::new();
        let input = gstreamer_app::AppSrc::builder()
            .caps(
                &media::Caps::builder("video/x-h264")
                    .field("stream-format", "byte-stream")
                    .field("alignment", "au")
                    .field(
                        "framerate",
                        media::Fraction::new(
                            configuration.cadence_numerator as i32,
                            configuration.cadence_denominator as i32,
                        ),
                    )
                    .build(),
            )
            .format(media::Format::Time)
            .is_live(true)
            .block(false)
            .build();
        let maximum_queued_bytes = u64::try_from(
            configuration
                .limits
                .maximum_access_unit_bytes
                .checked_mul(configuration.limits.maximum_pending_frames)
                .context("provider queue size overflow")?,
        )?;
        let maximum_queued_frames = u64::try_from(configuration.limits.maximum_pending_frames)?;
        input.set_max_bytes(maximum_queued_bytes);
        input.set_max_buffers(maximum_queued_frames);
        let parser = media::ElementFactory::make("h264parse")
            .build()
            .context("H.264 parser plugin is unavailable")?;
        let decoder = media::ElementFactory::make("avdec_h264")
            .property("max-threads", 1_i32)
            .property("output-corrupt", false)
            .property("discard-corrupted-frames", true)
            .property("max-errors", 0_i32)
            .build()
            .context("GStreamer libav H.264 decoder is unavailable")?;
        let converter = media::ElementFactory::make("videoconvert").build()?;
        let colorimetry: gstreamer_video::VideoColorimetry =
            configuration.colorimetry.parse().context("invalid GStreamer colorimetry")?;
        let sink = gstreamer_app::AppSink::builder()
            .caps(
                &media::Caps::builder("video/x-raw")
                    .field("format", "NV12")
                    .field("width", i32::try_from(configuration.width)?)
                    .field("height", i32::try_from(configuration.height)?)
                    .field("interlace-mode", "progressive")
                    .field("colorimetry", colorimetry.to_string())
                    .build(),
            )
            .sync(false)
            .async_(false)
            .max_buffers(u32::try_from(configuration.limits.maximum_pending_frames)?)
            .wait_on_eos(false)
            .enable_last_sample(false)
            .build();
        pipeline.add_many([
            input.upcast_ref(),
            &parser,
            &decoder,
            &converter,
            sink.upcast_ref(),
        ])?;
        media::Element::link_many([
            input.upcast_ref(),
            &parser,
            &decoder,
            &converter,
            sink.upcast_ref(),
        ])?;
        let (sender, samples) = mpsc::channel(configuration.limits.maximum_pending_frames);
        let (failure_sender, failure) = watch::channel(None);
        let callback_accepting = accepting.clone();
        let callback_failure = failure_sender.clone();
        sink.set_callbacks(
            gstreamer_app::AppSinkCallbacks::builder()
                .new_sample(move |sink| {
                    let sample = sink.pull_sample().map_err(|_| media::FlowError::Eos)?;

                    if !callback_accepting.load(Ordering::Acquire) {
                        return Err(media::FlowError::Flushing);
                    }

                    if sender.try_send(sample).is_err() {
                        callback_failure.send_replace(Some(
                            "decoded output queue is full or closed".to_owned(),
                        ));
                        return Err(media::FlowError::Error);
                    }

                    Ok(media::FlowSuccess::Ok)
                })
                .build(),
        );
        pipeline.bus().context("decoder pipeline has no bus")?.set_sync_handler(
            move |_, message| {
                if let media::MessageView::Error(error) = message.view() {
                    failure_sender.send_replace(Some(format!(
                        "{}: {:?}",
                        error.error(),
                        error.debug()
                    )));
                }

                media::BusSyncReply::Drop
            },
        );
        let instance = Self {
            pipeline,
            input,
            accepting,
            maximum_queued_bytes,
            maximum_queued_frames,
        };

        if !instance.accepting.load(Ordering::Acquire) {
            anyhow::bail!("decoder setup was cancelled");
        }

        if let Err(error) = instance.pipeline.set_state(media::State::Playing) {
            instance.accepting.store(false, Ordering::Release);
            instance
                .pipeline
                .set_state(media::State::Null)
                .context("retire failed decoder setup")?;
            return Err(error).context("activate H.264 decoder");
        }

        if !instance.accepting.load(Ordering::Acquire) {
            instance
                .pipeline
                .set_state(media::State::Null)
                .context("retire cancelled decoder setup")?;
            anyhow::bail!("decoder setup was cancelled");
        }

        Ok((
            instance,
            Output {
                samples,
                failure,
            },
        ))
    }

    pub fn submit(
        &self,
        bytes: Vec<u8>,
        identity_timestamp: u64,
        duration_nanoseconds: u64,
    ) -> Result<()> {
        ensure!(self.accepting.load(Ordering::Acquire), "decoder is stopping");
        ensure!(
            self.input.current_level_buffers() < self.maximum_queued_frames,
            "encoded provider queue is full"
        );
        ensure!(
            self.input
                .current_level_bytes()
                .checked_add(bytes.len() as u64)
                .is_some_and(|length| length <= self.maximum_queued_bytes),
            "encoded provider byte queue is full"
        );
        let mut buffer = media::Buffer::from_mut_slice(bytes);
        let owned = buffer.get_mut().context("new decoder input buffer is not uniquely owned")?;
        owned.set_pts(media::ClockTime::from_nseconds(identity_timestamp));
        owned.set_duration(media::ClockTime::from_nseconds(duration_nanoseconds));
        self.input.push_buffer(buffer).context("H.264 provider rejected input")?;
        Ok(())
    }

    pub async fn stop(self, timeout: Duration) -> Result<()> {
        self.accepting.store(false, Ordering::Release);
        let retirement = tokio::task::spawn_blocking(move || {
            self.pipeline.set_state(media::State::Null).context("stop decoder pipeline")?;
            let (result, state, _) = self.pipeline.state(media::ClockTime::ZERO);
            result.context("read decoder retirement state")?;
            ensure!(state == media::State::Null, "decoder has not reached the stopped state");

            if let Some(bus) = self.pipeline.bus() {
                bus.unset_sync_handler();
            }

            Ok(())
        });
        tokio::time::timeout(timeout, retirement)
            .await
            .context("decoder quiescence is unknown: cancellation deadline expired")??
    }
}

pub fn copy_frame(sample: &media::Sample, configuration: &Configuration) -> Result<(u64, Vec<u8>)> {
    let buffer = sample.buffer().context("decoded sample has no owned buffer")?;
    ensure!(
        !buffer.flags().contains(media::BufferFlags::CORRUPTED),
        "decoder marked output corrupted"
    );
    let identity = buffer.pts().context("decoder output lost its submission identity")?.nseconds();
    let information = gstreamer_video::VideoInfo::from_caps(
        sample.caps().context("decoded output has no format")?,
    )?;
    let colorimetry: gstreamer_video::VideoColorimetry = configuration.colorimetry.parse()?;
    ensure!(
        information.format() == gstreamer_video::VideoFormat::Nv12
            && information.width() == configuration.width
            && information.height() == configuration.height
            && information.colorimetry() == colorimetry
            && !information.is_interlaced(),
        "decoded layout or colorimetry differs from the admitted configuration"
    );

    if let Some(crop) = buffer.meta::<gstreamer_video::VideoCropMeta>() {
        ensure!(
            crop.rect() == (0, 0, configuration.width, configuration.height),
            "cropped decoder output is outside this profile"
        );
    }

    let frame = gstreamer_video::VideoFrameRef::from_buffer_ref_readable(buffer, &information)?;
    let width = configuration.width as usize;
    let height = configuration.height as usize;
    let mut output = Vec::with_capacity(configuration.frame_bytes()?);

    for (plane, rows) in [(0, height), (1, height / 2)] {
        let stride =
            usize::try_from(frame.plane_stride()[plane]).context("negative decoded row stride")?;
        ensure!(stride >= width, "decoded row stride is too short");
        let bytes = frame.plane_data(plane as u32)?;

        for row in 0..rows {
            let start = row.checked_mul(stride).context("decoded row offset overflow")?;
            let end = start.checked_add(width).context("decoded row extent overflow")?;
            output.extend_from_slice(
                bytes.get(start..end).context("decoded plane is not fully readable")?,
            );
        }
    }

    ensure!(output.len() == configuration.frame_bytes()?, "decoded frame is not complete");
    Ok((identity, output))
}
