use anyhow::{Result, anyhow, ensure};
use h264_reader::nal::pps::PicParameterSet;
use h264_reader::nal::slice::{FieldPic, SliceFamily, SliceHeader};
use h264_reader::nal::sps::{ChromaFormat, FrameMbsFlags, SeqParameterSet};
use h264_reader::nal::{Nal, RefNal};
use netaudio_core::video::h264::{self, AccessUnit, START_CODE};

use super::configuration::{Configuration, decode_hex};

pub struct Parameters {
    context: h264_reader::Context,
    sequence: Vec<u8>,
    picture: Vec<u8>,
}

pub struct Format {
    pub width: u32,
    pub height: u32,
    pub profile_identifier: u8,
    pub level_identifier: u8,
    pub maximum_reference_frames: u32,
}

impl Parameters {
    pub fn new(configuration: &Configuration) -> Result<Self> {
        let sequence = decode_hex(&configuration.sequence_parameters_hex)?;
        let picture = decode_hex(&configuration.picture_parameters_hex)?;
        Self::from_bytes(
            sequence,
            picture,
            Format {
                width: configuration.width,
                height: configuration.height,
                profile_identifier: configuration.profile_identifier,
                level_identifier: configuration.level_identifier,
                maximum_reference_frames: configuration.limits.maximum_reference_frames,
            },
        )
    }

    pub fn from_bytes(sequence: Vec<u8>, picture: Vec<u8>, format: Format) -> Result<Self> {
        ensure!(!sequence.is_empty() && !picture.is_empty(), "parameter sets must be nonempty");
        ensure!(
            sequence[0] & h264::NAL_TYPE_MASK == h264::SEQUENCE_PARAMETERS
                && picture[0] & h264::NAL_TYPE_MASK == h264::PICTURE_PARAMETERS,
            "configuration must supply actual SPS and PPS NALs"
        );
        let sequence_nal = RefNal::new(&sequence, &[], true);
        sequence_nal.header().map_err(|error| anyhow!("invalid SPS header: {error:?}"))?;
        let parsed_sequence = SeqParameterSet::from_bits(sequence_nal.rbsp_bits())
            .map_err(|error| anyhow!("invalid SPS syntax: {error:?}"))?;
        ensure!(
            parsed_sequence.profile().profile_idc() == format.profile_identifier
                && parsed_sequence.level_idc == format.level_identifier,
            "SPS profile or level differs from configuration"
        );
        ensure!(
            parsed_sequence
                .pixel_dimensions()
                .map_err(|error| anyhow!("invalid SPS dimensions: {error:?}"))?
                == (format.width, format.height),
            "SPS dimensions differ from configuration"
        );
        ensure!(
            matches!(parsed_sequence.frame_mbs_flags, FrameMbsFlags::Frames),
            "interlaced H.264 is not supported"
        );
        ensure!(
            parsed_sequence.chroma_info.chroma_format == ChromaFormat::YUV420
                && parsed_sequence.chroma_info.bit_depth_luma_minus8 == 0
                && parsed_sequence.chroma_info.bit_depth_chroma_minus8 == 0,
            "only eight-bit 4:2:0 H.264 is supported"
        );
        ensure!(
            parsed_sequence.max_num_ref_frames <= format.maximum_reference_frames,
            "SPS reference pictures exceed the configured decoder bound"
        );
        let mut context = h264_reader::Context::new();
        context.put_seq_param_set(parsed_sequence);
        let picture_nal = RefNal::new(&picture, &[], true);
        picture_nal.header().map_err(|error| anyhow!("invalid PPS header: {error:?}"))?;
        let parsed_picture = PicParameterSet::from_bits(&context, picture_nal.rbsp_bits())
            .map_err(|error| anyhow!("invalid PPS syntax: {error:?}"))?;
        ensure!(
            parsed_picture.slice_groups.is_none(),
            "H.264 slice groups are outside this receiver profile"
        );
        context.put_pic_param_set(parsed_picture);
        Ok(Self {
            context,
            sequence,
            picture,
        })
    }

    pub fn validate(&self, access_unit: &AccessUnit) -> Result<()> {
        let mut first_slice = None;
        let mut previous_macroblock = None;

        for range in &access_unit.nal_ranges {
            let bytes = &access_unit.bytes[range.clone()];

            match bytes[0] & h264::NAL_TYPE_MASK {
                h264::SEQUENCE_PARAMETERS => ensure!(
                    bytes == self.sequence,
                    "in-band SPS changed; new configuration is required"
                ),
                h264::PICTURE_PARAMETERS => ensure!(
                    bytes == self.picture,
                    "in-band PPS changed; new configuration is required"
                ),
                h264::SLICE | h264::RANDOM_ACCESS_SLICE => {
                    let nal = RefNal::new(bytes, &[], true);
                    let header =
                        nal.header().map_err(|error| anyhow!("invalid slice NAL: {error:?}"))?;
                    let (slice, _, picture) =
                        SliceHeader::from_bits(&self.context, &mut nal.rbsp_bits(), header)
                            .map_err(|error| anyhow!("invalid slice header: {error:?}"))?;
                    ensure!(
                        matches!(slice.field_pic, FieldPic::Frame)
                            && matches!(slice.slice_type.family, SliceFamily::I | SliceFamily::P),
                        "only progressive I/P pictures are supported"
                    );
                    let identity = (
                        slice.frame_num,
                        slice.idr_pic_id,
                        picture.pic_parameter_set_id,
                        slice.pic_order_cnt_lsb,
                    );

                    if let Some(expected) = &first_slice {
                        ensure!(
                            *expected == identity,
                            "different coded pictures share one access unit"
                        );
                    } else {
                        ensure!(
                            slice.first_mb_in_slice == 0,
                            "access unit is missing its first slice"
                        );
                        first_slice = Some(identity);
                    }

                    ensure!(
                        previous_macroblock
                            .is_none_or(|previous| slice.first_mb_in_slice > previous),
                        "duplicate or reordered slices in access unit"
                    );
                    previous_macroblock = Some(slice.first_mb_in_slice);

                    if access_unit.random_access {
                        ensure!(
                            bytes[0] & h264::NAL_TYPE_MASK == h264::RANDOM_ACCESS_SLICE
                                && slice.slice_type.family == SliceFamily::I,
                            "recovery access unit contains non-IDR slices"
                        );
                    }
                }
                _ => {}
            }
        }

        ensure!(first_slice.is_some(), "access unit contains no validated coded picture");
        Ok(())
    }

    pub fn prefix(&self) -> Vec<u8> {
        [START_CODE.as_slice(), &self.sequence, START_CODE.as_slice(), &self.picture].concat()
    }
}
