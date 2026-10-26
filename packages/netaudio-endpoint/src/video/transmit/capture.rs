use std::path::PathBuf;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use gstreamer as media;
use gstreamer::prelude::*;
use gstreamer_video::prelude::*;
use tokio::sync::watch;

use super::configuration::{FRAME_BYTES, FRAME_RATE, HEIGHT, WIDTH};

#[derive(Clone)]
pub enum Input {
    Desktop {
        window: u64,
    },
    Camera(PathBuf),
    File(PathBuf),
}

#[derive(Clone)]
enum State {
    Starting,
    Frame(media::Sample),
    Ended,
    Failed(String),
}

pub struct Capture {
    pipeline: media::Pipeline,
    state: watch::Receiver<State>,
}

impl Capture {
    pub async fn start(input: &Input, timeout: Duration) -> Result<Self> {
        let accepting = Arc::new(AtomicBool::new(true));
        let setup_accepting = accepting.clone();
        let input = input.clone();
        let setup = tokio::task::spawn_blocking(move || {
            let capture = Self::new(&input)?;
            ensure!(setup_accepting.load(Ordering::Acquire), "capture setup was cancelled");
            Ok(capture)
        });

        match tokio::time::timeout(timeout, setup).await {
            Ok(result) => result.context("capture setup task failed")?,
            Err(error) => {
                accepting.store(false, Ordering::Release);
                Err(error).context("capture setup deadline expired; quiescence is unknown")
            }
        }
    }

    fn new(input: &Input) -> Result<Self> {
        media::init()?;
        let sink = gstreamer_app::AppSink::builder()
            .caps(
                &media::Caps::builder("video/x-raw")
                    .field("format", "BGRA")
                    .field("width", WIDTH as i32)
                    .field("height", HEIGHT as i32)
                    .field("framerate", media::Fraction::new(FRAME_RATE as i32, 1))
                    .field("interlace-mode", "progressive")
                    .field("pixel-aspect-ratio", media::Fraction::new(1, 1))
                    .field("colorimetry", "sRGB")
                    .build(),
            )
            .sync(true)
            .max_buffers(1)
            .drop(true)
            .wait_on_eos(false)
            .enable_last_sample(false)
            .build();
        let converter = media::ElementFactory::make("videoconvert").build()?;
        let scaler =
            media::ElementFactory::make("videoscale").property("add-borders", true).build()?;
        let rate = media::ElementFactory::make("videorate").build()?;
        let bin = media::Bin::new();
        let elements = [&converter, &scaler, &rate, sink.upcast_ref()];
        bin.add_many(elements)?;
        media::Element::link_many(elements)?;
        let pad = media::GhostPad::with_target(
            &converter.static_pad("sink").context("converter has no input")?,
        )?;
        bin.add_pad(&pad)?;
        let pipeline = match input {
            Input::File(path) => {
                let path = path.canonicalize().context("open video file")?;
                ensure!(path.is_file(), "video input must be a regular file");
                let uri = media::glib::filename_to_uri(path, None)?;
                media::ElementFactory::make("playbin")
                    .property("uri", uri.as_str())
                    .property_from_str("flags", "video")
                    .property("video-sink", &bin)
                    .build()?
                    .downcast::<media::Pipeline>()
                    .map_err(|_| anyhow::anyhow!("playbin is not a pipeline"))?
            }
            Input::Desktop {
                window,
            } => {
                let source = media::ElementFactory::make("ximagesrc")
                    .property("xid", *window)
                    .property("use-damage", false)
                    .build()
                    .context("X11 desktop capture is unavailable")?;
                let pipeline = media::Pipeline::new();
                pipeline.add_many([&source, bin.upcast_ref()])?;
                source.link(&bin)?;
                pipeline
            }
            Input::Camera(path) => {
                let source = media::ElementFactory::make("v4l2src")
                    .property("device", path.to_str().context("camera path is not Unicode")?)
                    .build()?;
                let pipeline = media::Pipeline::new();
                pipeline.add_many([&source, bin.upcast_ref()])?;
                source.link(&bin)?;
                pipeline
            }
        };
        let (sender, state) = watch::channel(State::Starting);
        let frames = sender.clone();
        sink.set_callbacks(
            gstreamer_app::AppSinkCallbacks::builder()
                .new_sample(move |sink| {
                    let sample = sink.pull_sample().map_err(|_| media::FlowError::Eos)?;
                    frames.send_replace(State::Frame(sample));
                    Ok(media::FlowSuccess::Ok)
                })
                .build(),
        );
        pipeline.bus().context("capture pipeline has no bus")?.set_sync_handler(
            move |_, message| {
                match message.view() {
                    media::MessageView::Error(error) => {
                        sender.send_replace(State::Failed(format!(
                            "{}: {:?}",
                            error.error(),
                            error.debug()
                        )));
                    }
                    media::MessageView::Eos(_) => {
                        sender.send_replace(State::Ended);
                    }
                    _ => {}
                }

                media::BusSyncReply::Drop
            },
        );
        let capture = Self {
            pipeline,
            state,
        };
        capture.pipeline.set_state(media::State::Playing).context("start video capture")?;
        Ok(capture)
    }

    pub async fn next(&mut self) -> Result<Option<Vec<u8>>> {
        self.state.changed().await.context("capture provider closed")?;
        let state = self.state.borrow_and_update().clone();

        match state {
            State::Frame(sample) => copy(&sample).map(Some),
            State::Ended => Ok(None),
            State::Failed(error) => anyhow::bail!("capture failed: {error}"),
            State::Starting => anyhow::bail!("capture returned to an invalid starting state"),
        }
    }

    pub async fn stop(self, timeout: Duration) -> Result<()> {
        tokio::time::timeout(
            timeout,
            tokio::task::spawn_blocking(move || {
                self.pipeline.set_state(media::State::Null).context("stop video capture")?;
                self.pipeline.bus().context("capture pipeline has no bus")?.unset_sync_handler();
                Ok::<_, anyhow::Error>(())
            }),
        )
        .await
        .context("capture retirement deadline expired")??
    }
}

impl Drop for Capture {
    fn drop(&mut self) {
        let _ = self.pipeline.set_state(media::State::Null);
    }
}

fn copy(sample: &media::Sample) -> Result<Vec<u8>> {
    let buffer = sample.buffer().context("capture frame has no buffer")?;
    ensure!(
        !buffer.flags().contains(media::BufferFlags::CORRUPTED),
        "capture provider marked the frame corrupted"
    );

    if let Some(crop) = buffer.meta::<gstreamer_video::VideoCropMeta>() {
        ensure!(
            crop.rect() == (0, 0, WIDTH, HEIGHT),
            "capture provider returned an unsupported crop"
        );
    }
    let information = gstreamer_video::VideoInfo::from_caps(
        sample.caps().context("capture frame has no format")?,
    )?;
    let colorimetry: gstreamer_video::VideoColorimetry = "sRGB".parse()?;
    ensure!(
        information.format() == gstreamer_video::VideoFormat::Bgra
            && information.width() == WIDTH
            && information.height() == HEIGHT
            && information.fps() == media::Fraction::new(FRAME_RATE as i32, 1)
            && information.colorimetry() == colorimetry
            && !information.is_interlaced(),
        "capture did not negotiate the required BGRA format"
    );
    let frame = gstreamer_video::VideoFrameRef::from_buffer_ref_readable(buffer, &information)?;
    let stride = usize::try_from(frame.plane_stride()[0]).context("negative capture row stride")?;
    let row_bytes = WIDTH as usize * 4;
    ensure!(stride >= row_bytes, "capture stride is too short");
    let plane = frame.plane_data(0)?;
    let mut bytes = Vec::with_capacity(FRAME_BYTES);

    for row in 0..HEIGHT as usize {
        let start = row.checked_mul(stride).context("capture offset overflow")?;
        bytes.extend_from_slice(
            plane.get(start..start + row_bytes).context("capture row is incomplete")?,
        );
    }

    Ok(bytes)
}
