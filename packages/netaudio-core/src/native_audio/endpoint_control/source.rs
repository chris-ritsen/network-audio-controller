use std::net::{Ipv4Addr, SocketAddrV4};

use crate::protocol::{RESPONSE_HEADER_SIZE, RESULT_CODE_SUCCESS};

use super::{
    double_word, string, word, Endpoint, TransmitFlow, Writer, CHANNEL_COUNT, FRAMES_PER_PACKET,
    MAXIMUM_TRANSMIT_FRAMES_PER_PACKET,
    SAMPLE_BITS, SAMPLE_RATE,
};
use crate::native_audio::source_control::{
    Error, Header, ALLOCATION_OPERATION, REFERENCE_REVISION, STOP_OPERATION,
};

const REMAP_OPERATION: u16 = 0x0102;
const UNSUPPORTED_REVISION: u16 = 0x0100;
const UNKNOWN_FLOW: u16 = 0x0102;
const STALE_GENERATION: u16 = 0x0103;
const SLOT_COUNT_MISMATCH: u16 = 0x0312;
const NO_SELECTED_CHANNEL: u16 = 0x0301;
const RESOURCE_UNAVAILABLE: u16 = 0x03f0;
const INVALID_DESTINATION: u16 = 0x0321;
const UNKNOWN_CHANNEL_WARNING: u16 = 0x8301;

pub enum Action {
    Reply(Vec<u8>),
    Allocate {
        header: Header,
        flow: TransmitFlow,
        result: u16,
    },
    Remap {
        header: Header,
        channels: Vec<u16>,
        result: u16,
    },
    Stop {
        header: Header,
    },
}

fn reply(header: Header, result: u16) -> Result<Action, Error> {
    Ok(Action::Reply(
        Writer::response(header, RESPONSE_HEADER_SIZE, result)?.finish(),
    ))
}

pub fn unavailable(header: Header) -> Result<Vec<u8>, Error> {
    Ok(Writer::response(header, RESPONSE_HEADER_SIZE, RESOURCE_UNAVAILABLE)?.finish())
}

fn checked_region(
    bytes: &[u8],
    offset: usize,
    length: usize,
    minimum: usize,
    alignment: usize,
) -> Result<(), Error> {
    if offset < minimum
        || offset % alignment != 0
        || offset
            .checked_add(length)
            .is_none_or(|end| end > bytes.len())
    {
        return Err(Error(
            "source object is incomplete, overlaps the fixed directory or is misaligned",
        ));
    }

    Ok(())
}

fn selected_channels(bytes: &[u8], offset: usize, count: usize) -> Result<(Vec<u16>, u16), Error> {
    let mut channels = Vec::with_capacity(count);
    let mut result = RESULT_CODE_SUCCESS;
    for index in 0..count {
        let identifier = word(bytes, offset + 2 * index)?;
        if identifier > CHANNEL_COUNT {
            channels.push(0);
            result = UNKNOWN_CHANNEL_WARNING;
        } else {
            channels.push(identifier);
        }
    }

    Ok((channels, result))
}

pub fn admit(endpoint: &Endpoint, request: &[u8]) -> Result<Option<Action>, Error> {
    let header = Header::decode(request)?;
    if header.result != 0 {
        return Ok(None);
    }

    if header.revision != REFERENCE_REVISION {
        return reply(header, UNSUPPORTED_REVISION).map(Some);
    }

    let action = match header.operation {
        ALLOCATION_OPERATION => allocate(endpoint, request, header)?,
        STOP_OPERATION | REMAP_OPERATION => {
            let minimum = if header.operation == STOP_OPERATION {
                16
            } else {
                18
            };

            if request.len() < minimum {
                return Err(Error("owned source request is incomplete"));
            }

            let selector = word(request, 10)?;
            let generation = double_word(request, 12)?;
            let Some(current) = endpoint.transmit_flow.as_ref().filter(|_| selector == 1) else {
                return reply(header, UNKNOWN_FLOW).map(Some);
            };

            if current.generation != generation {
                return reply(header, STALE_GENERATION).map(Some);
            }

            if header.operation == STOP_OPERATION {
                if request.len() != 16 {
                    return Err(Error("owned source STOP has surplus bytes"));
                }
                Action::Stop { header }
            } else {
                let count = usize::from(word(request, 16)?);
                if request.len() != 18 + 2 * count {
                    return Err(Error("source remap has an invalid extent"));
                }

                if count != current.channels.len() {
                    return reply(header, SLOT_COUNT_MISMATCH).map(Some);
                }

                let (channels, result) = selected_channels(request, 18, count)?;
                if channels.iter().all(|identifier| *identifier == 0) {
                    return reply(header, NO_SELECTED_CHANNEL).map(Some);
                }
                Action::Remap {
                    header,
                    channels,
                    result,
                }
            }
        }
        _ => return Ok(None),
    };

    Ok(Some(action))
}

fn allocate(endpoint: &Endpoint, request: &[u8], header: Header) -> Result<Action, Error> {
    if request.len() < 30 {
        return Err(Error("source allocation is incomplete"));
    }

    if endpoint.transmit_flow.is_some() {
        return reply(header, RESOURCE_UNAVAILABLE);
    }

    let interfaces = usize::from(word(request, 20)?);
    let count = usize::from(word(request, 22)?);
    if interfaces != 1 || !(1..=usize::from(CHANNEL_COUNT)).contains(&count) {
        return reply(header, RESOURCE_UNAVAILABLE);
    }

    let fixed_end = 26 + 2 * (interfaces + count);
    if fixed_end > request.len() {
        return Err(Error("source allocation directory is incomplete"));
    }

    if double_word(request, 12)? != SAMPLE_RATE || word(request, 18)? != SAMPLE_BITS {
        return reply(header, RESOURCE_UNAVAILABLE);
    }

    let (channels, result) = selected_channels(request, 24 + 2 * interfaces, count)?;
    if channels.iter().all(|identifier| *identifier == 0) {
        return reply(header, NO_SELECTED_CHANNEL);
    }

    let descriptor = usize::from(word(request, 24)?);
    checked_region(request, descriptor, 8, fixed_end, 4)?;
    if request[descriptor] != 8 || request[descriptor + 1] != 2 {
        return reply(header, INVALID_DESTINATION);
    }

    let address = Ipv4Addr::new(
        request[descriptor + 4],
        request[descriptor + 5],
        request[descriptor + 6],
        request[descriptor + 7],
    );
    let destination = SocketAddrV4::new(address, word(request, descriptor + 2)?);
    if destination.port() == 0
        || address.is_unspecified()
        || address.is_multicast()
        || address.is_broadcast()
    {
        return reply(header, INVALID_DESTINATION);
    }

    let extension = usize::from(word(request, fixed_end - 2)?);
    if extension == 0 {
        return Err(Error(
            "selected source allocation requires its complete reference extension",
        ));
    }
    checked_region(request, extension, 20, fixed_end, 2)?;
    let extension_length = usize::from(request[extension]) * 2;
    checked_region(request, extension, extension_length, fixed_end, 2)?;
    if extension_length < 20 {
        return Err(Error("source allocation reference extension is incomplete"));
    }

    let frames_per_packet = word(request, extension + 4)?;
    if !(FRAMES_PER_PACKET..=MAXIMUM_TRANSMIT_FRAMES_PER_PACKET).contains(&frames_per_packet) {
        return reply(header, RESOURCE_UNAVAILABLE);
    }

    let heap_minimum = extension + extension_length;
    let hostname = string(request, usize::from(word(request, 10)?), heap_minimum)?;
    let flow_name = string(
        request,
        usize::from(word(request, extension + 6)?),
        heap_minimum,
    )?;
    if hostname.is_empty() || flow_name.is_empty() {
        return Err(Error("source allocation receiver names cannot be empty"));
    }

    Ok(Action::Allocate {
        header,
        flow: TransmitFlow {
            frames_per_packet,
            generation: 0,
            destination,
            channels,
            receiver_hostname: hostname,
            receiver_flow_name: flow_name,
            active: false,
        },
        result,
    })
}

pub fn allocation_committed(
    endpoint: &mut Endpoint,
    header: Header,
    mut flow: TransmitFlow,
    generation: u32,
    result: u16,
) -> Result<Vec<u8>, Error> {
    if generation == 0 || endpoint.transmit_flow.is_some() {
        return Err(Error(
            "source allocation commit has no uniquely owned generation or resource",
        ));
    }
    flow.generation = generation;
    let mut response = Writer::response(header, 24, result)?;
    response.word(10, 1);
    response.double_word(12, generation);
    response.word(16, 1);
    response.word(18, 1);
    endpoint.transmit_flow = Some(flow);
    Ok(response.finish())
}

pub fn remap_committed(
    endpoint: &mut Endpoint,
    header: Header,
    channels: Vec<u16>,
    result: u16,
) -> Result<Vec<u8>, Error> {
    let flow = endpoint
        .transmit_flow
        .as_mut()
        .ok_or(Error("source allocation retired before remap commit"))?;
    if flow.channels.len() != channels.len() {
        return Err(Error("source remap cannot resize a flow"));
    }
    flow.active = false;
    flow.channels = channels;
    let mut response = Writer::response(header, 22, result)?;
    response.word(10, 1);
    response.double_word(12, flow.generation);
    Ok(response.finish())
}

pub fn stop_committed(endpoint: &mut Endpoint, header: Header) -> Result<Vec<u8>, Error> {
    endpoint.transmit_flow = None;
    Ok(Writer::response(header, RESPONSE_HEADER_SIZE, RESULT_CODE_SUCCESS)?.finish())
}
