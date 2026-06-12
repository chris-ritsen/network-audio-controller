pub fn u16_at(data: &[u8], offset: usize) -> u16 {
    u16::from_be_bytes([data[offset], data[offset + 1]])
}

pub fn read_u16(data: &[u8], offset: usize) -> Option<u16> {
    data.get(offset..offset + 2)
        .map(|slice| u16::from_be_bytes([slice[0], slice[1]]))
}

pub fn read_u32(data: &[u8], offset: usize) -> Option<u32> {
    data.get(offset..offset + 4)
        .map(|slice| u32::from_be_bytes([slice[0], slice[1], slice[2], slice[3]]))
}

pub fn null_terminated_slice(data: &[u8], offset: usize) -> &[u8] {
    if offset >= data.len() {
        return &[];
    }
    let end = data[offset..]
        .iter()
        .position(|&byte| byte == 0)
        .map(|relative| offset + relative)
        .unwrap_or(data.len());
    &data[offset..end]
}

pub fn string_at_pointer(data: &[u8], pointer: u16) -> Option<String> {
    if pointer == 0 || pointer as usize >= data.len() {
        return None;
    }
    std::str::from_utf8(null_terminated_slice(data, pointer as usize))
        .ok()
        .map(str::to_owned)
}

pub fn null_terminated_lossy(data: &[u8], offset: usize) -> String {
    String::from_utf8_lossy(null_terminated_slice(data, offset)).into_owned()
}
