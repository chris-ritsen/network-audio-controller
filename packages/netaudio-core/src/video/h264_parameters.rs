use super::h264::{self, AccessUnit, START_CODE};
use anyhow::{anyhow, ensure, Result};
use h264_reader::nal::pps::PicParameterSet;
use h264_reader::nal::slice::{FieldPic, SliceFamily, SliceHeader};
use h264_reader::nal::sps::{ChromaFormat, FrameMbsFlags, SeqParameterSet};
use h264_reader::nal::{Nal, RefNal};
use h264_reader::rbsp::BitRead;

pub struct Parameters {
    context: h264_reader::Context,
    sequence: Vec<u8>,
    picture: Vec<u8>,
    format: Format,
    interpretation: Interpretation,
    coded_dimensions: (u32, u32),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Format {
    pub width: u32,
    pub height: u32,
    pub profile_identifier: u8,
    pub level_identifier: u8,
    pub maximum_reference_frames: u32,
}

impl Parameters {
    pub fn from_bytes(sequence: Vec<u8>, picture: Vec<u8>, format: Format) -> Result<Self> {
        ensure!(
            !sequence.is_empty() && !picture.is_empty(),
            "parameter sets must be nonempty"
        );
        ensure!(
            sequence[0] & h264::NAL_TYPE_MASK == h264::SEQUENCE_PARAMETERS
                && picture[0] & h264::NAL_TYPE_MASK == h264::PICTURE_PARAMETERS,
            "configuration must supply actual SPS and PPS NALs"
        );
        let parsed = SequenceParameters::parse_with_format(
            sequence,
            format.maximum_reference_frames,
            Some(format),
        )?;
        Self::finish(parsed, picture, false)
    }

    pub fn from_sequence(sequence: SequenceParameters, picture: Vec<u8>) -> Result<Self> {
        Self::finish(sequence, picture, true)
    }

    fn finish(
        sequence: SequenceParameters,
        picture: Vec<u8>,
        bounded_picture: bool,
    ) -> Result<Self> {
        ensure!(
            !picture.is_empty() && picture[0] & h264::NAL_TYPE_MASK == h264::PICTURE_PARAMETERS,
            "configuration must supply actual SPS and PPS NALs"
        );
        let SequenceParameters {
            bytes: sequence,
            parsed: parsed_sequence,
            format,
            interpretation,
            coded_dimensions,
        } = sequence;
        let mut context = h264_reader::Context::new();
        context.put_seq_param_set(parsed_sequence);
        let picture_nal = RefNal::new(&picture, &[], true);
        picture_nal
            .header()
            .map_err(|error| anyhow!("invalid PPS header: {error:?}"))?;
        if bounded_picture {
            let mut prefix = picture_nal.rbsp_bits();
            prefix
                .read_ue("pic_parameter_set_id")
                .map_err(|error| anyhow!("invalid PPS syntax: {error:?}"))?;
            prefix
                .read_ue("seq_parameter_set_id")
                .map_err(|error| anyhow!("invalid PPS syntax: {error:?}"))?;
            prefix
                .read_bool("entropy_coding_mode_flag")
                .map_err(|error| anyhow!("invalid PPS syntax: {error:?}"))?;
            prefix
                .read_bool("bottom_field_pic_order_in_frame_present_flag")
                .map_err(|error| anyhow!("invalid PPS syntax: {error:?}"))?;
            ensure!(
                prefix
                    .read_ue("num_slice_groups_minus1")
                    .map_err(|error| anyhow!("invalid PPS syntax: {error:?}"))?
                    == 0,
                "H.264 slice groups are outside this receiver profile"
            );
        }
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
            format,
            interpretation,
            coded_dimensions,
        })
    }

    pub fn validate(&self, access_unit: &AccessUnit) -> Result<()> {
        self.validate_picture(access_unit, false)
    }

    pub fn validate_recovery(&self, access_unit: &AccessUnit) -> Result<()> {
        ensure!(
            access_unit.random_access,
            "bootstrap recovery must be an IDR access unit"
        );
        self.validate_picture(access_unit, true)
    }

    fn validate_picture(&self, access_unit: &AccessUnit, recovery: bool) -> Result<()> {
        let mut first_slice = None;
        let mut previous_macroblock = None;

        for range in &access_unit.nal_ranges {
            let bytes = access_unit
                .bytes
                .get(range.clone())
                .filter(|bytes| !bytes.is_empty())
                .ok_or_else(|| anyhow!("invalid H.264 access-unit NAL extent"))?;

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
                    let header = nal
                        .header()
                        .map_err(|error| anyhow!("invalid slice NAL: {error:?}"))?;
                    let (slice, sequence, picture) =
                        SliceHeader::from_bits(&self.context, &mut nal.rbsp_bits(), header)
                            .map_err(|error| anyhow!("invalid slice header: {error:?}"))?;
                    ensure!(
                        matches!(slice.field_pic, FieldPic::Frame)
                            && matches!(slice.slice_type.family, SliceFamily::I | SliceFamily::P),
                        "only progressive I/P pictures are supported"
                    );
                    if recovery {
                        ensure!(
                            header.nal_ref_idc() != 0
                                && slice.frame_num == 0
                                && slice
                                    .idr_pic_id
                                    .is_some_and(|identifier| identifier <= 65_535),
                            "invalid H.264 IDR picture identity"
                        );
                        ensure!(
                            sequence
                                .pic_width_in_mbs_minus1
                                .checked_add(1)
                                .and_then(|width| sequence
                                    .pic_height_in_map_units_minus1
                                    .checked_add(1)
                                    .and_then(|height| width.checked_mul(height)))
                                .is_some_and(|count| slice.first_mb_in_slice < count),
                            "recovery slice begins outside coded geometry"
                        );
                    }
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

        ensure!(
            first_slice.is_some(),
            "access unit contains no validated coded picture"
        );
        Ok(())
    }

    pub fn format(&self) -> Format {
        self.format
    }

    pub fn coded_dimensions(&self) -> (u32, u32) {
        self.coded_dimensions
    }

    pub fn interpretation(&self) -> Interpretation {
        self.interpretation
    }

    pub fn sequence(&self) -> &[u8] {
        &self.sequence
    }

    pub fn picture(&self) -> &[u8] {
        &self.picture
    }

    pub fn prefix(&self) -> Vec<u8> {
        [
            START_CODE.as_slice(),
            &self.sequence,
            START_CODE.as_slice(),
            &self.picture,
        ]
        .concat()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Cadence {
    pub numerator: u64,
    pub denominator: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ColorDescription {
    pub primaries: u8,
    pub transfer: u8,
    pub matrix: u8,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ColorInterpretation {
    pub full_range: bool,
    pub description: ColorDescription,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Timing {
    pub units_in_tick: u32,
    pub time_scale: u32,
    pub fixed_frame_rate: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Interpretation {
    pub timing: Option<Timing>,
    pub full_range: Option<bool>,
    pub color: Option<ColorDescription>,
}

pub struct SequenceParameters {
    bytes: Vec<u8>,
    parsed: SeqParameterSet,
    format: Format,
    interpretation: Interpretation,
    coded_dimensions: (u32, u32),
}

impl SequenceParameters {
    pub fn parse(sequence: Vec<u8>, maximum_reference_frames: u32) -> Result<Self> {
        Self::parse_with_format(sequence, maximum_reference_frames, None)
    }

    fn parse_with_format(
        sequence: Vec<u8>,
        maximum_reference_frames: u32,
        expected: Option<Format>,
    ) -> Result<Self> {
        ensure!(
            !sequence.is_empty() && sequence[0] & h264::NAL_TYPE_MASK == h264::SEQUENCE_PARAMETERS,
            "configuration must supply an actual SPS NAL"
        );
        let sequence_nal = RefNal::new(&sequence, &[], true);
        sequence_nal
            .header()
            .map_err(|error| anyhow!("invalid SPS header: {error:?}"))?;
        let parsed_sequence = SeqParameterSet::from_bits(sequence_nal.rbsp_bits())
            .map_err(|error| anyhow!("invalid SPS syntax: {error:?}"))?;
        if let Some(format) = expected {
            ensure!(
                parsed_sequence.profile().profile_idc() == format.profile_identifier
                    && parsed_sequence.level_idc == format.level_identifier,
                "SPS profile or level differs from configuration"
            );
            ensure!(
                checked_dimensions(&parsed_sequence, false)? == (format.width, format.height),
                "SPS dimensions differ from configuration"
            );
        }
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
            parsed_sequence.max_num_ref_frames <= maximum_reference_frames,
            "SPS reference pictures exceed the configured decoder bound"
        );

        let (width, height) = checked_dimensions(&parsed_sequence, expected.is_none())?;
        let coded_dimensions = (
            (parsed_sequence.pic_width_in_mbs_minus1 + 1) * 16,
            (parsed_sequence.pic_height_in_map_units_minus1 + 1) * 16,
        );
        let format = Format {
            width,
            height,
            profile_identifier: parsed_sequence.profile().profile_idc(),
            level_identifier: parsed_sequence.level_idc,
            maximum_reference_frames: parsed_sequence.max_num_ref_frames,
        };
        let mut interpretation = Interpretation {
            timing: None,
            full_range: None,
            color: None,
        };
        if let Some(information) = &parsed_sequence.vui_parameters {
            if let Some(timing) = &information.timing_info {
                interpretation.timing = Some(Timing {
                    units_in_tick: timing.num_units_in_tick,
                    time_scale: timing.time_scale,
                    fixed_frame_rate: timing.fixed_frame_rate_flag,
                });
            }
            if let Some(signal) = &information.video_signal_type {
                interpretation.full_range = Some(signal.video_full_range_flag);
                interpretation.color =
                    signal
                        .colour_description
                        .as_ref()
                        .map(|color| ColorDescription {
                            primaries: color.colour_primaries,
                            transfer: color.transfer_characteristics,
                            matrix: color.matrix_coefficients,
                        });
            }
        }
        Ok(Self {
            bytes: sequence,
            parsed: parsed_sequence,
            format,
            interpretation,
            coded_dimensions,
        })
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn format(&self) -> Format {
        self.format
    }

    pub fn coded_dimensions(&self) -> (u32, u32) {
        self.coded_dimensions
    }

    pub fn interpretation(&self) -> Interpretation {
        self.interpretation
    }
}

fn checked_dimensions(
    sequence: &SeqParameterSet,
    bound_macroblock_count: bool,
) -> Result<(u32, u32)> {
    let macroblock_width = sequence
        .pic_width_in_mbs_minus1
        .checked_add(1)
        .ok_or_else(|| anyhow!("SPS macroblock width overflow"))?;
    let macroblock_height = sequence
        .pic_height_in_map_units_minus1
        .checked_add(1)
        .ok_or_else(|| anyhow!("SPS macroblock height overflow"))?;
    if bound_macroblock_count {
        macroblock_width
            .checked_mul(macroblock_height)
            .ok_or_else(|| anyhow!("SPS macroblock count overflow"))?;
    }
    sequence
        .pixel_dimensions()
        .map_err(|error| anyhow!("invalid SPS dimensions: {error:?}"))
}
