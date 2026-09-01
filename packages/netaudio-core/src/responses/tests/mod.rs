use super::*;
use crate::protocol::PROTOCOL_ID;
use crate::test_support::decode_hexadecimal;

fn stamp_arc_response(response: &mut [u8], protocol: u16, opcode: u16, result: u16) {
    let length = response.len() as u16;
    response[0..2].copy_from_slice(&protocol.to_be_bytes());
    response[2..4].copy_from_slice(&length.to_be_bytes());
    response[6..8].copy_from_slice(&opcode.to_be_bytes());
    response[8..10].copy_from_slice(&result.to_be_bytes());
}

fn stamp_conmon_response(response: &mut [u8], opcode: u16) {
    let length = response.len() as u16;
    response[0..2].copy_from_slice(&0xFFFFu16.to_be_bytes());
    response[2..4].copy_from_slice(&length.to_be_bytes());
    response[16..24].copy_from_slice(b"Audinate");
    response[24] = 0x07;
    response[26..28].copy_from_slice(&opcode.to_be_bytes());
}

fn metering_frame(tx_levels: &[u8], rx_levels: &[u8]) -> Vec<u8> {
    let mut data = vec![0u8; METERING_V2_HEADER_SIZE + tx_levels.len() + rx_levels.len()];
    let length = data.len() as u16;
    data[0..2].copy_from_slice(&0xFFFFu16.to_be_bytes());
    data[2..4].copy_from_slice(&length.to_be_bytes());
    data[4..6].copy_from_slice(&0x1F81u16.to_be_bytes());
    data[8..16].copy_from_slice(&[0x00, 0x1D, 0xC1, 0x19, 0x24, 0x5C, 0x00, 0x00]);
    data[16..24].copy_from_slice(b"Audinate");
    data[METERING_FAMILY_OFFSET] = 0x02;
    data[METERING_V2_TX_COUNT_OFFSET] = u8::try_from(tx_levels.len()).unwrap();
    data[METERING_V2_RX_COUNT_OFFSET] = u8::try_from(rx_levels.len()).unwrap();
    data[METERING_V2_SUFFIX_OFFSET] = 0xFE;
    let tx_levels_end = METERING_V2_LEVELS_OFFSET + tx_levels.len();
    data[METERING_V2_LEVELS_OFFSET..tx_levels_end].copy_from_slice(tx_levels);
    data[tx_levels_end..].copy_from_slice(rx_levels);
    data
}

fn metering_frame_v3(tx_levels: &[u8], rx_levels: &[u8]) -> Vec<u8> {
    let mut data = vec![0u8; METERING_V3_HEADER_SIZE + tx_levels.len() + rx_levels.len()];
    let length = u16::try_from(data.len()).unwrap();
    data[0..2].copy_from_slice(&0xFFFFu16.to_be_bytes());
    data[2..4].copy_from_slice(&length.to_be_bytes());
    data[4..6].copy_from_slice(&0xDDFBu16.to_be_bytes());
    data[8..16].copy_from_slice(&[0x00, 0x1D, 0xC1, 0x08, 0x12, 0x58, 0x00, 0x00]);
    data[16..24].copy_from_slice(b"Audinate");
    data[METERING_FAMILY_OFFSET] = 0x03;
    data[METERING_V3_TX_COUNT_OFFSET..METERING_V3_TX_COUNT_OFFSET + 2]
        .copy_from_slice(&u16::try_from(tx_levels.len()).unwrap().to_be_bytes());
    data[METERING_V3_RX_COUNT_OFFSET..METERING_V3_RX_COUNT_OFFSET + 2]
        .copy_from_slice(&u16::try_from(rx_levels.len()).unwrap().to_be_bytes());
    let tx_levels_end = METERING_V3_LEVELS_OFFSET + tx_levels.len();
    data[METERING_V3_LEVELS_OFFSET..tx_levels_end].copy_from_slice(tx_levels);
    data[tx_levels_end..].copy_from_slice(rx_levels);
    data
}

mod conmon;
mod conmon_detail;
mod device;
mod flows;
mod github_issue_reports;
mod modern_arc_capture;
mod robustness;
