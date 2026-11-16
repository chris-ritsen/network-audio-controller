//! Bounded protobuf primitives. Unknown fields remain available in the raw record.
#[derive(Debug, Clone)]
pub enum Value<'a> {
    Varint(u64),
    Bytes(&'a [u8]),
    Fixed,
}
#[derive(Debug, Clone)]
pub struct Field<'a> {
    pub number: u32,
    pub value: Value<'a>,
}
pub type Message<'a> = Vec<Field<'a>>;
fn varint(data: &[u8], pos: &mut usize) -> Option<u64> {
    let mut value = 0;
    for shift in (0..70).step_by(7) {
        let byte = *data.get(*pos)?;
        *pos += 1;
        if shift == 63 && byte > 1 {
            return None;
        }
        value |= u64::from(byte & 127) << shift;
        if byte < 128 {
            return Some(value);
        }
    }
    None
}
pub fn parse(data: &[u8]) -> Option<Message<'_>> {
    let mut pos = 0;
    let mut fields = Vec::new();
    while pos < data.len() {
        let key = varint(data, &mut pos)?;
        let number = u32::try_from(key >> 3).ok()?;
        if number == 0 || number > 0x1fff_ffff {
            return None;
        }
        let value = match key & 7 {
            0 => Value::Varint(varint(data, &mut pos)?),
            2 => {
                let len = usize::try_from(varint(data, &mut pos)?).ok()?;
                let end = pos.checked_add(len)?;
                let bytes = data.get(pos..end)?;
                pos = end;
                Value::Bytes(bytes)
            }
            1 | 5 => {
                let end = pos.checked_add(if key & 7 == 1 { 8 } else { 4 })?;
                data.get(pos..end)?;
                pos = end;
                Value::Fixed
            }
            _ => return None,
        };
        fields.push(Field { number, value });
    }
    Some(fields)
}
pub fn uint(fields: &Message<'_>, number: u32) -> Option<u32> {
    let mut result = 0;
    for field in fields.iter().filter(|candidate| candidate.number == number) {
        let Value::Varint(value) = field.value else {
            return None;
        };
        result = u32::try_from(value).ok()?;
    }
    Some(result)
}
pub fn bytes<'a>(fields: &Message<'a>, number: u32) -> Option<Option<&'a [u8]>> {
    let mut result = None;
    for field in fields.iter().filter(|candidate| candidate.number == number) {
        let Value::Bytes(payload) = field.value else {
            return None;
        };
        result = Some(payload);
    }
    Some(result)
}
pub fn nested<'a>(fields: &Message<'a>, number: u32) -> Option<Option<Message<'a>>> {
    bytes(fields, number)?.map(parse).transpose_option()
}
trait Transpose<T> {
    fn transpose_option(self) -> Option<Option<T>>;
}
impl<T> Transpose<T> for Option<Option<T>> {
    fn transpose_option(self) -> Option<Option<T>> {
        match self {
            None => Some(None),
            Some(inner) => inner.map(Some),
        }
    }
}
pub fn string(fields: &Message<'_>, number: u32) -> Option<String> {
    Some(
        std::str::from_utf8(bytes(fields, number)?.unwrap_or_default())
            .ok()?
            .to_owned(),
    )
}
pub fn repeated_uint(fields: &Message<'_>, number: u32) -> Option<Vec<u32>> {
    let mut result = Vec::new();
    for field in fields.iter().filter(|candidate| candidate.number == number) {
        match field.value {
            Value::Varint(value) => result.push(u32::try_from(value).ok()?),
            Value::Bytes(packed) => {
                let mut position = 0;
                while position < packed.len() {
                    result.push(u32::try_from(varint(packed, &mut position)?).ok()?);
                }
            }
            _ => return None,
        }
    }
    Some(result)
}
pub fn put_varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 128 {
        out.push((value as u8 & 127) | 128);
        value >>= 7;
    }
    out.push(value as u8);
}
pub fn put_uint(out: &mut Vec<u8>, number: u32, value: u32) {
    if value == 0 {
        return;
    }
    put_varint(out, u64::from(number) << 3);
    put_varint(out, u64::from(value));
}
pub fn put_bytes(out: &mut Vec<u8>, number: u32, value: &[u8]) {
    put_varint(out, (u64::from(number) << 3) | 2);
    put_varint(out, value.len() as u64);
    out.extend_from_slice(value);
}
pub fn message(number: u32, value: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    put_bytes(&mut out, number, value);
    out
}
pub fn scalar(number: u32, value: u32) -> Vec<u8> {
    let mut out = Vec::new();
    put_uint(&mut out, number, value);
    out
}

pub fn put_uint_explicit(out: &mut Vec<u8>, number: u32, value: u32) {
    put_varint(out, u64::from(number) << 3);
    put_varint(out, u64::from(value));
}
