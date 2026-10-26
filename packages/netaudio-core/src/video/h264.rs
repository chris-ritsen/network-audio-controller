use super::{rtp::Packet, Error};

pub const START_CODE: &[u8; 4] = &[0, 0, 0, 1];
pub const SLICE: u8 = 1;
pub const RANDOM_ACCESS_SLICE: u8 = 5;
pub const SUPPLEMENTAL_INFORMATION: u8 = 6;
pub const SEQUENCE_PARAMETERS: u8 = 7;
pub const PICTURE_PARAMETERS: u8 = 8;
pub const ACCESS_UNIT_DELIMITER: u8 = 9;
pub const FILLER: u8 = 12;
pub const NAL_TYPE_MASK: u8 = 0x1f;
const FORBIDDEN_FLAG: u8 = 0x80;
const AGGREGATION: u8 = 24;
const FRAGMENT: u8 = 28;
const FRAGMENT_START: u8 = 0x80;
const FRAGMENT_END: u8 = 0x40;
const FRAGMENT_RESERVED: u8 = 0x20;
const NAL_IDENTITY_MASK: u8 = 0xe0;

#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub maximum_access_unit_bytes: usize,
    pub maximum_packets: usize,
    pub maximum_age_nanoseconds: i64,
}

#[derive(Debug)]
pub struct AccessUnit {
    pub identifier: u64,
    pub timestamp: u32,
    pub arrival_nanoseconds: i64,
    pub random_access: bool,
    pub bytes: Vec<u8>,
    pub nal_ranges: Vec<std::ops::Range<usize>>,
}

struct Assembly {
    identifier: u64,
    timestamp: u32,
    started_nanoseconds: i64,
    last_arrival_nanoseconds: i64,
    packets: usize,
    bytes: Vec<u8>,
    nal_ranges: Vec<std::ops::Range<usize>>,
    fragment: Option<(u8, usize)>,
    random_access: bool,
    has_slice: bool,
}

pub struct Receiver {
    limits: Limits,
    next_identifier: u64,
    next_sequence: Option<u16>,
    hunting: bool,
    assembly: Option<Assembly>,
}

impl Receiver {
    pub fn new(limits: Limits) -> Result<Self, Error> {
        if limits.maximum_access_unit_bytes == 0
            || limits.maximum_packets == 0
            || limits.maximum_age_nanoseconds <= 0
        {
            return Err(Error("H.264 assembly limits must be positive and finite"));
        }

        Ok(Self {
            limits,
            next_identifier: 1,
            next_sequence: None,
            hunting: true,
            assembly: None,
        })
    }

    pub fn invalidate(&mut self) {
        self.assembly = None;
        self.next_sequence = None;
        self.hunting = true;
    }

    pub fn expiration_nanoseconds(&self) -> Option<i64> {
        self.assembly.as_ref().and_then(|assembly| {
            assembly
                .started_nanoseconds
                .checked_add(self.limits.maximum_age_nanoseconds)
        })
    }

    pub fn push(
        &mut self,
        packet: Packet<'_>,
        arrival_nanoseconds: i64,
    ) -> Result<Option<AccessUnit>, Error> {
        let result = self.append(packet, arrival_nanoseconds);

        if result.is_err() {
            self.invalidate();
        }

        result
    }

    fn append(
        &mut self,
        packet: Packet<'_>,
        arrival_nanoseconds: i64,
    ) -> Result<Option<AccessUnit>, Error> {
        if self
            .next_sequence
            .is_some_and(|expected| expected != packet.sequence)
        {
            return Err(Error("H.264 packet loss, duplication or reordering"));
        }

        if let Some(assembly) = &self.assembly {
            if assembly.timestamp != packet.timestamp {
                return Err(Error("RTP timestamp changed before the access-unit marker"));
            }

            if arrival_nanoseconds < assembly.last_arrival_nanoseconds
                || arrival_nanoseconds
                    .checked_sub(assembly.started_nanoseconds)
                    .is_none_or(|age| age >= self.limits.maximum_age_nanoseconds)
            {
                return Err(Error("H.264 assembly expired or its clock moved backwards"));
            }
        }

        let payload = Payload::parse(packet.payload)?;

        if self.assembly.is_none() {
            if self.hunting && !payload.can_start_recovery() {
                return Ok(None);
            }

            let identifier = self.next_identifier;
            self.next_identifier = identifier
                .checked_add(1)
                .ok_or(Error("access-unit identity exhausted"))?;
            self.assembly = Some(Assembly {
                identifier,
                timestamp: packet.timestamp,
                started_nanoseconds: arrival_nanoseconds,
                last_arrival_nanoseconds: arrival_nanoseconds,
                packets: 0,
                bytes: Vec::new(),
                nal_ranges: Vec::new(),
                fragment: None,
                random_access: false,
                has_slice: false,
            });
        }

        let assembly = self.assembly.as_mut().unwrap();
        assembly.packets += 1;

        if assembly.packets > self.limits.maximum_packets {
            return Err(Error("H.264 access unit exceeds its packet limit"));
        }

        assembly.last_arrival_nanoseconds = arrival_nanoseconds;
        assembly.append(payload, self.limits.maximum_access_unit_bytes, self.hunting)?;
        self.next_sequence = Some(packet.sequence.wrapping_add(1));

        if !packet.marker {
            return Ok(None);
        }

        if assembly.fragment.is_some()
            || !assembly.has_slice
            || (self.hunting && !assembly.random_access)
        {
            return Err(Error(
                "H.264 marker does not complete a valid coded access unit",
            ));
        }

        let assembly = self.assembly.take().unwrap();
        self.hunting = false;

        Ok(Some(AccessUnit {
            identifier: assembly.identifier,
            timestamp: assembly.timestamp,
            arrival_nanoseconds,
            random_access: assembly.random_access,
            bytes: assembly.bytes,
            nal_ranges: assembly.nal_ranges,
        }))
    }
}

impl Assembly {
    fn append(
        &mut self,
        payload: Payload<'_>,
        maximum_bytes: usize,
        hunting: bool,
    ) -> Result<(), Error> {
        match payload {
            Payload::Complete(units) => {
                if self.fragment.is_some() {
                    return Err(Error("complete NAL interrupts an unfinished fragment"));
                }

                for unit in units {
                    self.mark_type(unit[0] & NAL_TYPE_MASK, hunting)?;
                    self.extend(START_CODE, maximum_bytes)?;
                    let start = self.bytes.len();
                    self.extend(unit, maximum_bytes)?;
                    self.nal_ranges.push(start..self.bytes.len());
                }
            }
            Payload::Fragment {
                header,
                start,
                end,
                bytes,
            } => {
                if start {
                    if self.fragment.is_some() {
                        return Err(Error(
                            "new H.264 fragment starts before the prior fragment ends",
                        ));
                    }

                    self.mark_type(header & NAL_TYPE_MASK, hunting)?;
                    self.extend(START_CODE, maximum_bytes)?;
                    let offset = self.bytes.len();
                    self.extend(&[header], maximum_bytes)?;
                    self.fragment = Some((header, offset));
                }

                let (expected, offset) = self
                    .fragment
                    .ok_or(Error("H.264 continuation has no fragment owner"))?;

                if header != expected {
                    return Err(Error("H.264 fragment identity changed"));
                }

                self.extend(bytes, maximum_bytes)?;

                if end {
                    self.fragment = None;
                    self.nal_ranges.push(offset..self.bytes.len());
                }
            }
        }

        Ok(())
    }

    fn mark_type(&mut self, kind: u8, hunting: bool) -> Result<(), Error> {
        if hunting && kind == SLICE {
            return Err(Error(
                "recovery requires an independently decodable random-access picture",
            ));
        }

        self.random_access |= kind == RANDOM_ACCESS_SLICE;
        self.has_slice |= matches!(kind, SLICE | RANDOM_ACCESS_SLICE);
        Ok(())
    }

    fn extend(&mut self, bytes: &[u8], maximum_bytes: usize) -> Result<(), Error> {
        if self
            .bytes
            .len()
            .checked_add(bytes.len())
            .is_none_or(|length| length > maximum_bytes)
        {
            return Err(Error("H.264 access unit exceeds its byte limit"));
        }

        self.bytes.extend_from_slice(bytes);
        Ok(())
    }
}

enum Payload<'a> {
    Complete(Vec<&'a [u8]>),
    Fragment {
        header: u8,
        start: bool,
        end: bool,
        bytes: &'a [u8],
    },
}

impl<'a> Payload<'a> {
    fn parse(bytes: &'a [u8]) -> Result<Self, Error> {
        if bytes.len() < 2 || bytes[0] & FORBIDDEN_FLAG != 0 {
            return Err(Error("invalid H.264 payload header"));
        }

        match bytes[0] & NAL_TYPE_MASK {
            AGGREGATION => {
                let mut units = Vec::new();
                let mut offset = 1;

                while offset < bytes.len() {
                    let size = bytes
                        .get(offset..offset + 2)
                        .ok_or(Error("truncated H.264 aggregate length"))?;
                    let length = usize::from(u16::from_be_bytes([size[0], size[1]]));
                    offset += 2;
                    let unit = bytes
                        .get(offset..offset + length)
                        .ok_or(Error("H.264 aggregate exceeds payload"))?;
                    validate_unit(unit)?;
                    units.push(unit);
                    offset += length;
                }

                if units.is_empty() {
                    return Err(Error("empty H.264 aggregate"));
                }

                Ok(Self::Complete(units))
            }
            FRAGMENT => {
                let start = bytes[1] & FRAGMENT_START != 0;
                let end = bytes[1] & FRAGMENT_END != 0;
                let header = (bytes[0] & NAL_IDENTITY_MASK) | (bytes[1] & NAL_TYPE_MASK);

                if bytes.len() < 3 || bytes[1] & FRAGMENT_RESERVED != 0 || (start && end) {
                    return Err(Error("invalid H.264 fragmentation flags or extent"));
                }

                validate_type(header)?;
                Ok(Self::Fragment {
                    header,
                    start,
                    end,
                    bytes: &bytes[2..],
                })
            }
            _ => {
                validate_unit(bytes)?;
                Ok(Self::Complete(vec![bytes]))
            }
        }
    }

    fn can_start_recovery(&self) -> bool {
        match self {
            Self::Complete(units) => units.iter().all(|unit| unit[0] & NAL_TYPE_MASK != SLICE),
            Self::Fragment { header, start, .. } => *start && header & NAL_TYPE_MASK != SLICE,
        }
    }
}

fn validate_unit(bytes: &[u8]) -> Result<(), Error> {
    if bytes.len() < 2 {
        return Err(Error("H.264 NAL is shorter than the supported minimum"));
    }

    validate_type(bytes[0])
}

fn validate_type(header: u8) -> Result<(), Error> {
    if header & FORBIDDEN_FLAG != 0
        || !matches!(
            header & NAL_TYPE_MASK,
            SLICE
                | RANDOM_ACCESS_SLICE
                | SUPPLEMENTAL_INFORMATION
                | SEQUENCE_PARAMETERS
                | PICTURE_PARAMETERS
                | ACCESS_UNIT_DELIMITER
                | FILLER
        )
    {
        return Err(Error("unsupported or forbidden H.264 NAL type"));
    }

    Ok(())
}
