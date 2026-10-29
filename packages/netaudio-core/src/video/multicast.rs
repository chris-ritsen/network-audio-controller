use std::num::NonZeroU16;

use crate::commands::{OPCODE_CREATE_TX_FLOW_2809, OPCODE_DELETE_TX_FLOW_2809};

use super::Error;

pub const MINIMUM_REVISION: u16 = 0x2802;
pub const MAXIMUM_REVISION: u16 = 0x280f;
const HEADER_BYTES: usize = 10;
const CREATE_FIXED_BYTES: usize = 98;
const DELETE_BYTES: usize = 34;
const MEDIA_SELECTOR: u16 = 4;

fn read_word(bytes: &[u8], offset: usize) -> Result<u16, Error> {
    let value = bytes
        .get(
            offset
                ..offset
                    .checked_add(2)
                    .ok_or(Error("video multicast offset overflow"))?,
        )
        .ok_or(Error("video multicast packet is truncated"))?;
    Ok(u16::from_be_bytes(value.try_into().unwrap()))
}

#[derive(Clone, Copy)]
pub struct Context {
    pub revision: u16,
    pub transaction: u16,
}

fn validate_context(context: Context) -> Result<(), Error> {
    if !(MINIMUM_REVISION..=MAXIMUM_REVISION).contains(&context.revision) {
        return Err(Error(
            "video multicast revision is outside the implemented profile",
        ));
    }

    Ok(())
}

fn word(bytes: &mut [u8], offset: usize, value: u16) {
    bytes[offset..offset + size_of::<u16>()].copy_from_slice(&value.to_be_bytes());
}

pub struct Create<'a> {
    pub context: Context,
    pub media_local_flow: NonZeroU16,
    pub media_local_channels: &'a [NonZeroU16],
}

pub struct CreateRequest {
    pub context: Context,
    pub media_local_flow: NonZeroU16,
    pub media_local_channels: Vec<NonZeroU16>,
}

impl CreateRequest {
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() < CREATE_FIXED_BYTES || read_word(bytes, 2)? as usize != bytes.len() {
            return Err(Error(
                "video multicast create has an invalid declared extent",
            ));
        }
        let context = Context {
            revision: read_word(bytes, 0)?,
            transaction: read_word(bytes, 4)?,
        };
        validate_context(context)?;
        if read_word(bytes, 6)? != OPCODE_CREATE_TX_FLOW_2809
            || read_word(bytes, 8)? != 0
            || bytes[10..16] != [0; 6]
            || bytes[16] != 1
            || bytes[17] != 1
            || read_word(bytes, 18)? != HEADER_BYTES as u16 + 10
            || bytes[20] != 22
            || read_word(bytes, 26)? != MEDIA_SELECTOR
            || read_word(bytes, 34)? != 2
            || bytes[64] != 10
            || bytes[84] != 4
        {
            return Err(Error(
                "video multicast create is outside the selected one-slot profile",
            ));
        }
        let count = usize::from(
            bytes[92]
                .checked_sub(3)
                .ok_or(Error("video multicast slot chain is invalid"))?,
        );
        if count != 1 || bytes.len() != CREATE_FIXED_BYTES + count * 2 {
            return Err(Error(
                "video multicast create does not preserve its exact ordinary geometry",
            ));
        }
        let flow = NonZeroU16::new(read_word(bytes, 28)?).ok_or(Error(
            "video multicast create lacks a local flow identifier",
        ))?;
        let channels = (0..count)
            .map(|index| {
                NonZeroU16::new(read_word(bytes, 96 + index * 2)?)
                    .ok_or(Error("video multicast create has a zero local channel"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let request = Self {
            context,
            media_local_flow: flow,
            media_local_channels: channels,
        };
        if (Create {
            context: request.context,
            media_local_flow: request.media_local_flow,
            media_local_channels: &request.media_local_channels,
        })
        .encode()?
            != bytes
        {
            return Err(Error(
                "video multicast create differs from the selected ordinary profile",
            ));
        }
        Ok(request)
    }
}

impl Create<'_> {
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        validate_context(self.context)?;
        let channels = self.media_local_channels;
        if channels.is_empty() || channels.len() > usize::from(u8::MAX) {
            return Err(Error(
                "video multicast creation requires a nonempty byte-sized channel selection",
            ));
        }

        let length = CREATE_FIXED_BYTES
            .checked_add(channels.len() * size_of::<u16>())
            .ok_or(Error("video multicast creation extent overflow"))?;
        let length_u16 = u16::try_from(length)
            .map_err(|_| Error("video multicast creation exceeds ARC extent"))?;
        let count = u8::try_from(channels.len())
            .map_err(|_| Error("video multicast channel selection exceeds byte count"))?;
        let mut bytes = vec![0; length];
        word(&mut bytes, 0, self.context.revision);
        word(&mut bytes, 2, length_u16);
        word(&mut bytes, 4, self.context.transaction);
        word(&mut bytes, 6, OPCODE_CREATE_TX_FLOW_2809);
        bytes[16] = 1;
        bytes[17] = 1;
        word(&mut bytes, 18, HEADER_BYTES as u16 + 10);

        bytes[20] = 22;
        bytes[21] = 39_u8
            .checked_add(count)
            .ok_or(Error("video multicast primary extent overflow"))?;
        word(&mut bytes, 26, MEDIA_SELECTOR);
        word(&mut bytes, 28, self.media_local_flow.get());
        word(&mut bytes, 34, 2);

        bytes[64] = 10;
        bytes[65] = 17_u8
            .checked_add(count)
            .ok_or(Error("video multicast extension extent overflow"))?;

        bytes[84] = 4;
        bytes[85] = 7_u8
            .checked_add(count)
            .ok_or(Error("video multicast slot extent overflow"))?;
        bytes[92] = 3_u8
            .checked_add(count)
            .ok_or(Error("video multicast channel extent overflow"))?;
        word(&mut bytes, 94, 1);
        for (index, channel) in channels.iter().enumerate() {
            word(&mut bytes, 96 + index * size_of::<u16>(), channel.get());
        }

        Ok(bytes)
    }
}

pub struct Delete {
    pub context: Context,
    pub media_local_flow: NonZeroU16,
}

pub struct DeleteRequest {
    pub context: Context,
    pub media_local_flow: NonZeroU16,
}

impl DeleteRequest {
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() != DELETE_BYTES || read_word(bytes, 2)? as usize != DELETE_BYTES {
            return Err(Error(
                "video multicast delete has an invalid declared extent",
            ));
        }
        let context = Context {
            revision: read_word(bytes, 0)?,
            transaction: read_word(bytes, 4)?,
        };
        validate_context(context)?;
        let request = Self {
            context,
            media_local_flow: NonZeroU16::new(read_word(bytes, 22)?).ok_or(Error(
                "video multicast delete lacks a local flow identifier",
            ))?,
        };
        if (Delete {
            context: request.context,
            media_local_flow: request.media_local_flow,
        })
        .encode()?
            != bytes
        {
            return Err(Error(
                "video multicast delete differs from the selected singleton profile",
            ));
        }
        Ok(request)
    }
}

impl Delete {
    pub fn encode(&self) -> Result<[u8; DELETE_BYTES], Error> {
        validate_context(self.context)?;
        let mut bytes = [0; DELETE_BYTES];
        word(&mut bytes, 0, self.context.revision);
        word(&mut bytes, 2, DELETE_BYTES as u16);
        word(&mut bytes, 4, self.context.transaction);
        word(&mut bytes, 6, OPCODE_DELETE_TX_FLOW_2809);
        word(&mut bytes, 16, 1);
        word(&mut bytes, 18, MEDIA_SELECTOR);
        word(&mut bytes, 22, self.media_local_flow.get());
        Ok(bytes)
    }
}
