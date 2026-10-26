use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use anyhow::{Context, Result, ensure};
use gstreamer as media;
use gstreamer::prelude::*;
use tokio::sync::{mpsc, watch};

use super::configuration::{
    Aggregation, Configuration, FRAME_BYTES, FRAME_RATE, HEIGHT, WIDTH, frame_timestamp,
};

pub struct Encoder {
    pipeline: media::Pipeline,
    input: gstreamer_app::AppSrc,
    raw_filter: media::Element,
    accepting: Arc<AtomicBool>,
}

pub struct Datagram {
    pub presentation_nanoseconds: u64,
    pub bytes: Vec<u8>,
}

pub struct Output {
    pub datagrams: mpsc::Receiver<Datagram>,
    pub failure: watch::Receiver<Option<String>>,
}

impl Encoder {
    pub async fn start(configuration: &Configuration) -> Result<(Self, Output)> {
        let accepting = Arc::new(AtomicBool::new(true));
        let setup_accepting = accepting.clone();
        let snapshot = configuration.clone();
        let setup = tokio::task::spawn_blocking(move || Self::new(&snapshot, setup_accepting));

        match tokio::time::timeout(configuration.operation_timeout(), setup).await {
            Ok(result) => result.context("encoder setup task failed")?,
            Err(error) => {
                accepting.store(false, Ordering::Release);
                Err(error).context("encoder setup deadline expired; quiescence is unknown")
            }
        }
    }

    fn new(configuration: &Configuration, accepting: Arc<AtomicBool>) -> Result<(Self, Output)> {
        media::init().context("initialize video provider")?;
        let pipeline = media::Pipeline::new();
        let input = gstreamer_app::AppSrc::builder()
            .caps(
                &media::Caps::builder("video/x-raw")
                    .field("format", "BGRA")
                    .field("width", WIDTH as i32)
                    .field("height", HEIGHT as i32)
                    .field("framerate", media::Fraction::new(FRAME_RATE as i32, 1))
                    .field("interlace-mode", "progressive")
                    .field("colorimetry", configuration.input.colorimetry.as_str())
                    .build(),
            )
            .format(media::Format::Time)
            .is_live(true)
            .block(false)
            .max_bytes(FRAME_BYTES as u64)
            .max_buffers(1)
            .build();
        let converter = media::ElementFactory::make("videoconvert").build()?;
        let raw_filter = media::ElementFactory::make("capsfilter")
            .property(
                "caps",
                media::Caps::builder("video/x-raw")
                    .field("format", "I420")
                    .field("colorimetry", configuration.encoder.colorimetry.as_str())
                    .build(),
            )
            .build()?;
        let encoder = media::ElementFactory::make("openh264enc")
            .property("bitrate", configuration.encoder.bitrate)
            .property("max-bitrate", configuration.encoder.bitrate)
            .property("gop-size", configuration.encoder.keyframe_interval)
            .property("enable-frame-skip", false)
            .property("multi-thread", 1_u32)
            .property_from_str("rate-control", "bitrate")
            .build()
            .context("OpenH264 encoder plugin is unavailable")?;
        let parser = media::ElementFactory::make("h264parse").build()?;
        let encoded_filter = media::ElementFactory::make("capsfilter")
            .property(
                "caps",
                media::Caps::builder("video/x-h264")
                    .field("stream-format", "byte-stream")
                    .field("alignment", "au")
                    .field("profile", "constrained-baseline")
                    .build(),
            )
            .build()?;
        let packetizer = media::ElementFactory::make("rtph264pay")
            .property("pt", u32::from(configuration.payload_type))
            .property("ssrc", configuration.synchronization_source)
            .property("seqnum-offset", i32::from(configuration.initial_sequence))
            .property("timestamp-offset", configuration.initial_timestamp)
            .property("mtu", u32::try_from(configuration.limits.maximum_datagram_bytes)?)
            .property("config-interval", -1_i32)
            .property("auto-header-extension", false)
            .property("source-info", false)
            .property_from_str(
                "aggregate-mode",
                match configuration.aggregation {
                    Aggregation::None => "none",
                    Aggregation::ZeroLatency => "zero-latency",
                },
            )
            .build()?;
        let sink = gstreamer_app::AppSink::builder()
            .sync(false)
            .async_(false)
            .max_buffers(1)
            .wait_on_eos(false)
            .enable_last_sample(false)
            .build();
        let elements: [&media::Element; 8] = [
            input.upcast_ref(),
            &converter,
            &raw_filter,
            &encoder,
            &parser,
            &encoded_filter,
            &packetizer,
            sink.upcast_ref(),
        ];
        pipeline.add_many(elements)?;
        media::Element::link_many(elements)?;
        let (sender, datagrams) = mpsc::channel(configuration.limits.maximum_packets_per_frame);
        let (failure_sender, failure) = watch::channel(None);
        let callback_failure = failure_sender.clone();
        let callback_accepting = accepting.clone();
        let maximum_bytes = configuration.limits.maximum_datagram_bytes;
        sink.set_callbacks(
            gstreamer_app::AppSinkCallbacks::builder()
                .new_sample(move |sink| {
                    let sample = sink.pull_sample().map_err(|_| media::FlowError::Eos)?;

                    if !callback_accepting.load(Ordering::Acquire) {
                        return Err(media::FlowError::Flushing);
                    }

                    let result = copy_datagram(&sample, maximum_bytes).and_then(|datagram| {
                        sender
                            .try_send(datagram)
                            .context("encoded datagram queue is full or closed")
                    });

                    if let Err(error) = result {
                        callback_failure.send_replace(Some(format!("{error:#}")));
                        return Err(media::FlowError::Error);
                    }

                    Ok(media::FlowSuccess::Ok)
                })
                .build(),
        );
        pipeline.bus().context("encoder pipeline has no bus")?.set_sync_handler(
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
            raw_filter,
            accepting,
        };
        ensure!(instance.accepting.load(Ordering::Acquire), "encoder setup was cancelled");

        if let Err(error) = instance.pipeline.set_state(media::State::Playing) {
            instance.accepting.store(false, Ordering::Release);
            instance
                .pipeline
                .set_state(media::State::Null)
                .context("retire failed encoder setup")?;
            return Err(error).context("activate H.264 encoder");
        }

        if !instance.accepting.load(Ordering::Acquire) {
            instance
                .pipeline
                .set_state(media::State::Null)
                .context("retire cancelled encoder setup")?;
            anyhow::bail!("encoder setup was cancelled");
        }

        Ok((
            instance,
            Output {
                datagrams,
                failure,
            },
        ))
    }

    pub fn submit(&self, bytes: Vec<u8>, index: u64) -> Result<()> {
        ensure!(self.accepting.load(Ordering::Acquire), "encoder is stopping");
        ensure!(bytes.len() == FRAME_BYTES, "source frame must contain exactly 8294400 BGRA bytes");
        ensure!(
            self.input.current_level_buffers() == 0,
            "encoder already owns a queued source frame"
        );
        let presentation = frame_timestamp(index)?;
        let duration =
            frame_timestamp(index.checked_add(1).context("source frame identity overflow")?)?
                - presentation;
        let mut buffer = media::Buffer::from_mut_slice(bytes);
        let owned = buffer.get_mut().context("source buffer is not uniquely owned")?;
        owned.set_pts(media::ClockTime::from_nseconds(presentation));
        owned.set_duration(media::ClockTime::from_nseconds(duration));
        self.input.push_buffer(buffer).context("encoder rejected owned source frame")?;
        Ok(())
    }

    pub fn verify_input(&self, configuration: &Configuration) -> Result<()> {
        let caps = self
            .input
            .static_pad("src")
            .and_then(|pad| pad.current_caps())
            .context("encoder has not accepted raw input caps")?;
        let information = gstreamer_video::VideoInfo::from_caps(&caps)?;
        ensure!(
            information.format() == gstreamer_video::VideoFormat::Bgra
                && information.width() == WIDTH
                && information.height() == HEIGHT
                && information.size() == FRAME_BYTES,
            "accepted source layout differs from packed BGRA"
        );
        let input_colorimetry: gstreamer_video::VideoColorimetry =
            configuration.input.colorimetry.parse()?;
        ensure!(
            information.colorimetry() == input_colorimetry
                && information.fps() == media::Fraction::new(FRAME_RATE as i32, 1)
                && information.stride() == [WIDTH as i32 * 4]
                && information.offset() == [0],
            "accepted raw colorimetry, cadence or planes differ from configuration"
        );
        let converted_caps = self
            .raw_filter
            .static_pad("src")
            .and_then(|pad| pad.current_caps())
            .context("encoder has no accepted conversion caps")?;
        let converted = gstreamer_video::VideoInfo::from_caps(&converted_caps)?;
        let encoded_colorimetry: gstreamer_video::VideoColorimetry =
            configuration.encoder.colorimetry.parse()?;
        ensure!(
            converted.format() == gstreamer_video::VideoFormat::I420
                && converted.width() == WIDTH
                && converted.height() == HEIGHT
                && converted.colorimetry() == encoded_colorimetry
                && converted.fps() == information.fps(),
            "accepted encoder input differs from the configured color conversion"
        );
        Ok(())
    }

    pub async fn stop(self, configuration: &Configuration) -> Result<()> {
        self.accepting.store(false, Ordering::Release);
        let retirement = tokio::task::spawn_blocking(move || {
            self.pipeline.set_state(media::State::Null).context("stop encoder pipeline")?;
            let (result, state, _) = self.pipeline.state(media::ClockTime::ZERO);
            result.context("read encoder retirement state")?;
            ensure!(state == media::State::Null, "encoder has not reached the stopped state");

            if let Some(bus) = self.pipeline.bus() {
                bus.unset_sync_handler();
            }

            Ok(())
        });
        tokio::time::timeout(configuration.operation_timeout(), retirement)
            .await
            .context("encoder quiescence is unknown: cancellation deadline expired")??
    }
}

fn copy_datagram(sample: &media::Sample, maximum_bytes: usize) -> Result<Datagram> {
    let buffer = sample.buffer().context("packetizer returned no datagram buffer")?;
    ensure!(
        !buffer.flags().contains(media::BufferFlags::CORRUPTED),
        "packetizer marked output corrupted"
    );
    ensure!(buffer.size() <= maximum_bytes, "packetizer output exceeds the datagram limit");
    let presentation_nanoseconds =
        buffer.pts().context("packetizer lost source frame identity")?.nseconds();
    let mut bytes = Vec::with_capacity(buffer.size());

    for memory in buffer.iter_memories() {
        let mapped = memory.map_readable().context("cannot map complete packetizer output")?;
        ensure!(
            bytes.len().checked_add(mapped.size()).is_some_and(|size| size <= maximum_bytes),
            "mapped packetizer spans exceed the datagram limit"
        );
        bytes.extend_from_slice(mapped.as_slice());
    }

    ensure!(bytes.len() == buffer.size(), "packetizer output mapping is incomplete");
    Ok(Datagram {
        presentation_nanoseconds,
        bytes,
    })
}
