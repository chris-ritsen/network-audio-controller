use std::path::PathBuf;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use gstreamer as media;
use gstreamer::prelude::*;
use tokio::sync::{oneshot, watch};

use super::configuration::Configuration;

const QUEUED_FRAMES: u64 = 2;
const WINDOW_EVENT_MILLISECONDS: u32 = 16;
const DISPLAY_INITIAL_WIDTH: u32 = 960;
const DISPLAY_INITIAL_HEIGHT: u32 = 540;

#[derive(Clone, Default)]
pub struct Options {
    pub display: bool,
    pub fullscreen: bool,
    pub camera: Option<PathBuf>,
}

pub struct Presentation {
    camera: Option<Camera>,
    display_frames: Option<watch::Sender<Option<Arc<Vec<u8>>>>>,
    display_task: Option<tokio::task::JoinHandle<Result<()>>>,
    accepting: Arc<AtomicBool>,
    failure_sender: watch::Sender<Option<String>>,
    pub failure: watch::Receiver<Option<String>>,
    closed_sender: watch::Sender<bool>,
    pub closed: watch::Receiver<bool>,
}

struct Camera {
    pipeline: media::Pipeline,
    input: gstreamer_app::AppSrc,
}

impl Presentation {
    pub async fn start(options: &Options, configuration: &Configuration) -> Result<Self> {
        let (failure_sender, failure) = watch::channel(None);
        let (closed_sender, closed) = watch::channel(false);
        let accepting = Arc::new(AtomicBool::new(true));
        let mut instance = Self {
            camera: None,
            display_frames: None,
            display_task: None,
            accepting,
            failure_sender,
            failure,
            closed_sender,
            closed,
        };

        let timeout =
            Duration::from_millis(u64::from(configuration.limits.provider_timeout_milliseconds));

        if let Some(path) = &options.camera {
            let path = path.clone();
            let snapshot = configuration.clone();
            let failure = instance.failure_sender.clone();
            let accepting = instance.accepting.clone();
            let setup = tokio::task::spawn_blocking(move || {
                let camera = Camera::start(&path, &snapshot, failure)?;
                ensure!(accepting.load(Ordering::Acquire), "camera setup was cancelled");
                Ok::<_, anyhow::Error>(camera)
            });
            instance.camera = Some(
                tokio::time::timeout(timeout, setup)
                    .await
                    .context("camera setup deadline expired; quiescence is unknown")???,
            );
        }

        if options.display || options.fullscreen {
            let (sender, frames) = watch::channel(None);
            let (ready_sender, ready) = oneshot::channel();
            let accepting = instance.accepting.clone();
            let configuration = configuration.clone();
            let fullscreen = options.fullscreen;
            let closed = instance.closed_sender.clone();
            let failure = instance.failure_sender.clone();
            instance.display_frames = Some(sender);
            instance.display_task = Some(tokio::task::spawn_blocking(move || {
                let result =
                    display(frames, accepting, &configuration, fullscreen, ready_sender, closed);

                if let Err(error) = &result {
                    failure.send_replace(Some(format!("{error:#}")));
                }

                result
            }));

            match tokio::time::timeout(timeout, ready).await {
                Ok(Ok(())) => {}
                result => {
                    instance.accepting.store(false, Ordering::Release);
                    let detail =
                        instance.failure.borrow().clone().unwrap_or_else(|| format!("{result:?}"));
                    instance.stop(timeout).await.context("retire failed display setup")?;
                    anyhow::bail!("display setup failed: {detail}");
                }
            }
        }

        Ok(instance)
    }

    pub fn publish(&self, bytes: Vec<u8>) -> Result<()> {
        if let Some(failure) = self.failure.borrow().as_ref() {
            anyhow::bail!("video output failed: {failure}");
        }

        if let Some(camera) = &self.camera {
            ensure!(
                camera.input.current_level_buffers() < QUEUED_FRAMES,
                "virtual camera stopped consuming frames"
            );
            camera
                .input
                .push_buffer(media::Buffer::from_mut_slice(bytes.clone()))
                .context("publish virtual camera frame")?;
        }

        if let Some(sender) = &self.display_frames {
            sender.send_replace(Some(Arc::new(bytes)));
        }

        Ok(())
    }

    pub async fn stop(mut self, timeout: Duration) -> Result<()> {
        self.accepting.store(false, Ordering::Release);
        self.display_frames.take();

        if let Some(camera) = self.camera.take() {
            tokio::time::timeout(timeout, tokio::task::spawn_blocking(move || camera.stop()))
                .await
                .context("virtual camera retirement deadline expired")???;
        }

        if let Some(task) = self.display_task.take() {
            tokio::time::timeout(timeout, task)
                .await
                .context("display retirement deadline expired")???;
        }

        Ok(())
    }
}

impl Drop for Presentation {
    fn drop(&mut self) {
        self.accepting.store(false, Ordering::Release);
    }
}

impl Camera {
    fn start(
        path: &std::path::Path,
        configuration: &Configuration,
        failure: watch::Sender<Option<String>>,
    ) -> Result<Self> {
        media::init()?;
        let pipeline = media::Pipeline::new();
        let input = gstreamer_app::AppSrc::builder()
            .caps(
                &media::Caps::builder("video/x-raw")
                    .field("format", "NV12")
                    .field("width", i32::try_from(configuration.width)?)
                    .field("height", i32::try_from(configuration.height)?)
                    .field(
                        "framerate",
                        media::Fraction::new(
                            configuration.cadence_numerator as i32,
                            configuration.cadence_denominator as i32,
                        ),
                    )
                    .field("colorimetry", configuration.colorimetry.as_str())
                    .build(),
            )
            .format(media::Format::Time)
            .is_live(true)
            .do_timestamp(true)
            .block(false)
            .max_buffers(QUEUED_FRAMES)
            .max_bytes(configuration.frame_bytes()? as u64 * QUEUED_FRAMES)
            .build();
        let converter = media::ElementFactory::make("videoconvert").build()?;
        let format = media::ElementFactory::make("capsfilter")
            .property("caps", media::Caps::builder("video/x-raw").field("format", "YUY2").build())
            .build()?;
        let sink = media::ElementFactory::make("v4l2sink")
            .property("device", path.to_str().context("camera path is not Unicode")?)
            .property("sync", false)
            .property("async", false)
            .build()
            .context("GStreamer V4L2 camera output is unavailable")?;
        let elements = [input.upcast_ref(), &converter, &format, &sink];
        pipeline.add_many(elements)?;
        media::Element::link_many(elements)?;
        pipeline.bus().context("camera pipeline has no bus")?.set_sync_handler(
            move |_, message| {
                if let media::MessageView::Error(error) = message.view() {
                    failure.send_replace(Some(format!("{}: {:?}", error.error(), error.debug())));
                }

                media::BusSyncReply::Drop
            },
        );
        let camera = Self {
            pipeline,
            input,
        };
        camera.pipeline.set_state(media::State::Playing).context("open virtual camera output")?;
        Ok(camera)
    }

    fn stop(self) -> Result<()> {
        self.pipeline.set_state(media::State::Null).context("close virtual camera output")?;
        self.pipeline.bus().context("camera pipeline has no bus")?.unset_sync_handler();
        Ok(())
    }
}

impl Drop for Camera {
    fn drop(&mut self) {
        let _ = self.pipeline.set_state(media::State::Null);
    }
}

fn display(
    mut frames: watch::Receiver<Option<Arc<Vec<u8>>>>,
    accepting: Arc<AtomicBool>,
    configuration: &Configuration,
    fullscreen: bool,
    ready: oneshot::Sender<()>,
    closed: watch::Sender<bool>,
) -> Result<()> {
    use sdl2::event::Event;
    use sdl2::keyboard::Keycode;
    use sdl2::pixels::PixelFormatEnum;
    use sdl2::video::FullscreenType;

    let context = sdl2::init().map_err(anyhow::Error::msg)?;
    let video = context.video().map_err(anyhow::Error::msg)?;
    let mut window = video
        .window("NetAudio Video", DISPLAY_INITIAL_WIDTH, DISPLAY_INITIAL_HEIGHT)
        .resizable()
        .build()?;

    if fullscreen {
        window.set_fullscreen(FullscreenType::Desktop).map_err(anyhow::Error::msg)?;
    }

    let mut canvas = window.into_canvas().build()?;
    canvas.set_logical_size(configuration.width, configuration.height)?;
    let creator = canvas.texture_creator();
    let mut texture = creator.create_texture_streaming(
        PixelFormatEnum::IYUV,
        configuration.width,
        configuration.height,
    )?;
    let width = configuration.width as usize;
    let height = configuration.height as usize;
    let luminance_bytes = width * height;
    let mut blue_chroma = vec![0; luminance_bytes / 4];
    let mut red_chroma = vec![0; luminance_bytes / 4];
    let mut events = context.event_pump().map_err(anyhow::Error::msg)?;
    unsafe {
        sdl2::sys::SDL_SetYUVConversionMode(
            sdl2::sys::SDL_YUV_CONVERSION_MODE::SDL_YUV_CONVERSION_BT709,
        );
    }
    let _ = ready.send(());

    while accepting.load(Ordering::Acquire) {
        for event in events
            .wait_event_timeout(WINDOW_EVENT_MILLISECONDS)
            .into_iter()
            .chain(events.poll_iter())
        {
            match event {
                Event::Quit {
                    ..
                }
                | Event::KeyDown {
                    keycode: Some(Keycode::Escape),
                    ..
                } => {
                    closed.send_replace(true);
                    return Ok(());
                }
                Event::KeyDown {
                    keycode: Some(Keycode::F),
                    repeat: false,
                    ..
                } => {
                    let state = if canvas.window().fullscreen_state() == FullscreenType::Off {
                        FullscreenType::Desktop
                    } else {
                        FullscreenType::Off
                    };
                    canvas.window_mut().set_fullscreen(state).map_err(anyhow::Error::msg)?;
                }
                _ => {}
            }
        }

        if frames.has_changed().unwrap_or(false) {
            let frame = frames.borrow_and_update().clone();

            if let Some(bytes) = frame {
                ensure!(
                    bytes.len() == configuration.frame_bytes()?,
                    "display frame has an invalid extent"
                );

                for (index, pair) in bytes[luminance_bytes..].as_chunks::<2>().0.iter().enumerate()
                {
                    blue_chroma[index] = pair[0];
                    red_chroma[index] = pair[1];
                }

                texture.update_yuv(
                    None,
                    &bytes[..luminance_bytes],
                    width,
                    &blue_chroma,
                    width / 2,
                    &red_chroma,
                    width / 2,
                )?;
                canvas.clear();
                canvas.copy(&texture, None, None).map_err(anyhow::Error::msg)?;
                canvas.present();
            }
        }
    }

    Ok(())
}
