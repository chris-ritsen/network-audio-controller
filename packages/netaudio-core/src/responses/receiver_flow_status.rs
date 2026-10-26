use super::{
    bytes_to_hex, read_u16, read_u32, ModernArcReceiverFlowStatus, ReceiverFlowChannelMapSlice,
    SegmentedReceiverFlowEndpoint, SegmentedReceiverFlowStatus,
};
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;

const COMMON_HEADER: u16 = 0x1626;
const RECEIVER_HEADER: u16 = 0x0c10;
const ENDPOINT_HEADER: u16 = 0x0404;
const MEDIA_VIDEO: u16 = 4;
const ROW_BYTES: usize = 76;
const ROW_ALIGNMENT: usize = 4;
const FORMAT_BYTES: usize = 16;
const FORMAT_HEADER: [u8; 8] = [2, 8, 0, 0, 6, 6, 0, 0];
const RECEIVER_SEGMENT: usize = 44;
const ENDPOINT_SEGMENT: usize = 68;
const MAXIMUM_ENDPOINTS: usize = 2;
const MAPPING_HEADER_BYTES: usize = 6;
const MINIMUM_MAPPING_SPAN: u16 = 2;
const IPV4_DESCRIPTOR_BYTES: usize = 8;
const IPV4_FAMILY: u8 = 2;

pub(super) fn is_video_segmented(packet: &[u8], pointer: u16) -> bool {
    let offset = usize::from(pointer);
    read_u16(packet, offset) == Some(COMMON_HEADER)
        && read_u16(packet, offset + 6) == Some(MEDIA_VIDEO)
        && read_u16(packet, offset + ENDPOINT_SEGMENT) == Some(ENDPOINT_HEADER)
}

struct Objects<'a> {
    packet: &'a [u8],
    directory_end: usize,
    row: Range<usize>,
    spans: Vec<Range<usize>>,
}

impl<'a> Objects<'a> {
    fn object(&mut self, pointer: u16, length: usize, alignment: usize) -> Option<&'a [u8]> {
        let start = usize::from(pointer);
        let end = start.checked_add(length)?;
        let span = start..end;

        if start < self.directory_end || start % alignment != 0 || overlaps(&span, &self.row) {
            return None;
        }

        let bytes = self.packet.get(span.clone())?;
        self.spans.push(span);
        Some(bytes)
    }

    fn name(&mut self, pointer: u16) -> Option<String> {
        let start = usize::from(pointer);
        let suffix = self.packet.get(start..)?;
        let length = suffix.iter().position(|byte| *byte == 0)?.checked_add(1)?;
        let bytes = self.object(pointer, length, 1)?;

        if length == 1 {
            return None;
        }

        std::str::from_utf8(&bytes[..length - 1])
            .ok()
            .map(str::to_owned)
    }

    fn mapping(
        &mut self,
        head: u16,
        slot_count: u16,
    ) -> Option<(Vec<ReceiverFlowChannelMapSlice>, Vec<Vec<u16>>)> {
        let mut pointer = head;
        let mut visited = BTreeSet::new();
        let mut channels = BTreeMap::new();
        let mut slices = Vec::new();
        let mut slots: Vec<Vec<u16>> = Vec::new();
        let mut nodes = Vec::new();

        while pointer != 0 {
            if !visited.insert(pointer) {
                return None;
            }

            let offset = usize::from(pointer);
            let first = read_u16(self.packet, offset)?;
            let span = read_u16(self.packet, offset + 2)?;
            let next = read_u16(self.packet, offset + 4)?;

            if first == 0 || span < MINIMUM_MAPPING_SPAN || span % 2 != 0 {
                return None;
            }

            let length = MAPPING_HEADER_BYTES.checked_add(usize::from(span))?;
            let range = offset..offset.checked_add(length)?;

            if nodes.iter().any(|previous| overlaps(previous, &range)) {
                return None;
            }

            let bytes = self.object(pointer, length, 2)?;
            nodes.push(range);
            let mapping = bytes.get(MAPPING_HEADER_BYTES..)?;

            for (index, slot) in mapping.iter().copied().enumerate() {
                if slot == 0 {
                    continue;
                }

                if u16::from(slot) > slot_count {
                    return None;
                }

                let channel = first.checked_add(u16::try_from(index).ok()?)?;

                if channels.insert(channel, slot).is_some() {
                    return None;
                }

                if slots.len() < usize::from(slot) {
                    slots.resize_with(usize::from(slot), Vec::new);
                }

                slots[usize::from(slot) - 1].push(channel);
            }

            slices.push(ReceiverFlowChannelMapSlice {
                pointer,
                starting_receiver_channel: first,
                byte_count: span,
                next_pointer: next,
                mapping_hexadecimal: bytes_to_hex(mapping),
            });
            pointer = next;
        }

        Some((slices, slots))
    }

    fn endpoint(&mut self, pointer: u16) -> Option<SegmentedReceiverFlowEndpoint> {
        let start = usize::from(pointer);
        let length = *self.packet.get(start)?;

        if length < 2 {
            return None;
        }

        let bytes = self.object(pointer, usize::from(length), ROW_ALIGNMENT)?;
        let kind = *bytes.get(1)?;
        let (port, address) = if kind == IPV4_FAMILY {
            if usize::from(length) != IPV4_DESCRIPTOR_BYTES {
                return None;
            }

            let address: [u8; 4] = bytes.get(4..8)?.try_into().ok()?;
            (
                Some(read_u16(bytes, 2)?),
                Some(std::net::Ipv4Addr::from(address).to_string()),
            )
        } else {
            (None, None)
        };

        Some(SegmentedReceiverFlowEndpoint {
            pointer,
            descriptor_length_bytes: length,
            kind,
            udp_port: port,
            ipv4_address: address,
            raw_descriptor_hexadecimal: bytes_to_hex(bytes),
        })
    }
}

pub(super) fn parse_video_segmented(
    packet: &[u8],
    pointer: u16,
    directory_end: usize,
) -> Option<ModernArcReceiverFlowStatus> {
    let start = usize::from(pointer);
    let row_end = start.checked_add(ROW_BYTES)?;
    let row = packet.get(start..row_end)?;

    if start % ROW_ALIGNMENT != 0
        || read_u16(row, 0)? != COMMON_HEADER
        || read_u16(row, RECEIVER_SEGMENT)? != RECEIVER_HEADER
        || read_u16(row, ENDPOINT_SEGMENT)? != ENDPOINT_HEADER
    {
        return None;
    }

    let global = read_u16(row, 2)?;
    let local = read_u16(row, 8)?;
    let endpoint_count = usize::from(*row.get(ENDPOINT_SEGMENT + 2)?);

    if global == 0 || local == 0 || endpoint_count > MAXIMUM_ENDPOINTS {
        return None;
    }

    let mut objects = Objects {
        packet,
        directory_end,
        row: start..row_end,
        spans: Vec::new(),
    };
    let name_pointer = read_u16(row, 20)?;
    let name = objects.name(name_pointer)?;
    let format_pointer = read_u16(row, 22)?;
    let format = if format_pointer == 0 {
        None
    } else {
        let bytes = objects.object(format_pointer, FORMAT_BYTES, ROW_ALIGNMENT)?;

        if bytes.get(..FORMAT_HEADER.len())? != FORMAT_HEADER {
            return None;
        }

        Some(bytes)
    };
    let slots = read_u16(row, RECEIVER_SEGMENT + 12)?;
    let mapping_head = read_u16(row, RECEIVER_SEGMENT + 14)?;
    let (mapping, channels) = objects.mapping(mapping_head, slots)?;
    let mut endpoints = Vec::with_capacity(endpoint_count);
    let mut unsupported = Vec::new();

    for index in 0..endpoint_count {
        let pointer = read_u16(row, ENDPOINT_SEGMENT + 4 + index * 2)?;
        let endpoint = if pointer == 0 {
            None
        } else {
            Some(objects.endpoint(pointer)?)
        };

        if endpoint
            .as_ref()
            .is_some_and(|endpoint| endpoint.kind != IPV4_FAMILY)
        {
            unsupported.push("endpoint_address_family");
        }

        endpoints.push(endpoint);
    }

    let selector = read_u16(row, RECEIVER_SEGMENT + 8)?;
    let description = read_u16(row, RECEIVER_SEGMENT + 10)?;

    if description != 0 {
        unsupported.push("transport_description");
    }

    let primary = endpoints.first().and_then(Option::as_ref);
    let endpoint_decoding_complete = !unsupported.contains(&"endpoint_address_family");
    let format_hexadecimal = format.map_or_else(String::new, bytes_to_hex);
    let subtype = format.and_then(|bytes| read_u32(bytes, 8));
    let codec = format.and_then(|bytes| read_u32(bytes, 12));
    let channel_count = channels.iter().map(Vec::len).sum::<usize>();
    let mapping_hexadecimal = mapping.first().map_or_else(String::new, |mapping| {
        let start = usize::from(mapping.pointer);
        let end = start + MAPPING_HEADER_BYTES + usize::from(mapping.byte_count);
        bytes_to_hex(&packet[start..end])
    });

    Some(ModernArcReceiverFlowStatus {
        record_pointer: pointer,
        record_length_bytes: ROW_BYTES as u16,
        record_type_code: COMMON_HEADER,
        flow_number: global,
        flow_type: None,
        media_type_code: MEDIA_VIDEO,
        media_local_flow_id: local,
        flow_type_code: None,
        flow_name_pointer: name_pointer,
        flow_name: name,
        format_pointer,
        format_descriptor_hexadecimal: format_hexadecimal,
        sample_rate: None,
        encoding: None,
        latency_nanoseconds: None,
        local_receiver_channel_count: u16::try_from(channel_count).ok()?,
        receiver_mapping_descriptor_pointer: mapping_head,
        receiver_mapping_descriptor_hexadecimal: mapping_hexadecimal,
        status_flags: None,
        status_code: None,
        endpoint_descriptor_hexadecimal: primary.map_or_else(String::new, |endpoint| {
            endpoint.raw_descriptor_hexadecimal.clone()
        }),
        destination_user_datagram_port: primary.and_then(|endpoint| endpoint.udp_port),
        destination_internet_protocol_version_four_address: primary
            .and_then(|endpoint| endpoint.ipv4_address.clone()),
        raw_record_hexadecimal: bytes_to_hex(row),
        segmented: Some(SegmentedReceiverFlowStatus {
            record_layout: "media4_segmented",
            media_type: "video",
            raw_options: read_u32(row, 12)?,
            raw_current_scalar: read_u32(row, 24)?,
            raw_scheme: *row.get(34)?,
            format_subtype: subtype,
            codec_identifier: codec,
            transport_description_selector: selector,
            transport_description_reference: description,
            flow_channel_slot_count: slots,
            channel_map_slices: mapping,
            receiver_channel_numbers_by_flow_channel: channels,
            interface_endpoints: endpoints,
            raw_indexed_endpoint_state: *row.get(ENDPOINT_SEGMENT + 3)?,
            endpoint_decoding_complete,
            unsupported_fields: unsupported,
            referenced_ranges: objects.spans,
        }),
    })
}

pub(super) fn pointed_objects_outside_rows(flows: &[ModernArcReceiverFlowStatus]) -> bool {
    let rows: Vec<_> = flows
        .iter()
        .map(|flow| {
            let start = usize::from(flow.record_pointer);
            start..start + usize::from(flow.record_length_bytes)
        })
        .collect();

    flows
        .iter()
        .filter_map(|flow| flow.segmented.as_ref())
        .flat_map(|flow| &flow.referenced_ranges)
        .all(|object| rows.iter().all(|row| !overlaps(object, row)))
}

fn overlaps(left: &Range<usize>, right: &Range<usize>) -> bool {
    left.start < right.end && right.start < left.end
}
