use super::transport::{CommandIntent, DeviceTransport, TransportError};
use netaudio_core::commands;
use netaudio_core::parser::{
    ChannelAudioMetadata, ChannelCount, RX_CHANNELS_PER_PAGE, RxChannel, TX_CHANNELS_PER_PAGE,
    TxChannel, parse_channel_audio_metadata, parse_channel_count, parse_rx_page,
    parse_tx_friendly_page, parse_tx_info_page,
};
use netaudio_core::protocol::{RESPONSE_HEADER_SIZE, RESULT_CODE_MORE_PAGES, response_envelope};
use netaudio_core::responses::{
    DeviceSettings, PropertyDirectory, parse_device_name, parse_device_settings,
    parse_property_directory,
};
use netaudio_core::spec::Target;
use serde::Serialize;
use serde_json::{Map, Value};
use std::collections::BTreeSet;

#[derive(Debug, Serialize)]
pub struct ChannelInventory {
    pub receivers: Vec<RxChannel>,
    pub transmitters: Vec<TxChannel>,
    pub audio: Option<ChannelAudioMetadata>,
}

pub async fn paged_inventory(
    transport: &mut DeviceTransport,
    kind: &str,
    protocol_identifier: u16,
    mut partial_observation: impl FnMut(&Map<String, Value>),
) -> Result<Value, TransportError> {
    const MAXIMUM_INVENTORY_PAGES: usize = 256;

    let mut inventory = netaudio_core::inventory::Inventory::new(
        kind,
        protocol_identifier,
        MAXIMUM_INVENTORY_PAGES,
    )
    .map_err(|message| TransportError::InvalidCommand(message.to_owned()))?;

    loop {
        match inventory.state() {
            netaudio_core::inventory::InventoryState::Pending { next_command, .. } => {
                let response = transport
                    .execute(&Value::Object(next_command), CommandIntent::Read)
                    .await?;
                inventory
                    .accept(&response)
                    .map_err(TransportError::MalformedResponse)?;
            }
            netaudio_core::inventory::InventoryState::Partial {
                next_command,
                partial_inventory,
                ..
            } => {
                partial_observation(&partial_inventory);
                let response = transport
                    .execute(&Value::Object(next_command), CommandIntent::Read)
                    .await?;
                inventory
                    .accept(&response)
                    .map_err(TransportError::MalformedResponse)?;
            }
            netaudio_core::inventory::InventoryState::Complete { inventory, .. } => {
                return Ok(Value::Object(inventory));
            }
        }
    }
}

pub async fn device_name(transport: &mut DeviceTransport) -> Result<String, TransportError> {
    let response = transport
        .request(
            commands::build_device_name,
            Target::Arc,
            CommandIntent::Read,
        )
        .await?;
    parse_device_name(&response).ok_or(TransportError::MalformedResponse("device name"))
}

pub async fn channel_count(
    transport: &mut DeviceTransport,
) -> Result<ChannelCount, TransportError> {
    let response = transport
        .request(
            commands::build_channel_count,
            Target::Arc,
            CommandIntent::Read,
        )
        .await?;
    parse_channel_count(&response).ok_or(TransportError::MalformedResponse("channel count"))
}

pub async fn settings(transport: &mut DeviceTransport) -> Result<DeviceSettings, TransportError> {
    let response = transport
        .request(
            commands::build_device_settings,
            Target::Arc,
            CommandIntent::Read,
        )
        .await?;
    parse_device_settings(&response).ok_or(TransportError::MalformedResponse("device settings"))
}

pub async fn properties(
    transport: &mut DeviceTransport,
) -> Result<PropertyDirectory, TransportError> {
    let response = transport
        .request(
            commands::build_property_directory,
            Target::Arc,
            CommandIntent::Read,
        )
        .await?;
    parse_property_directory(&response)
        .ok_or(TransportError::MalformedResponse("property directory"))
}

pub async fn legacy_channels(
    transport: &mut DeviceTransport,
    counts: &ChannelCount,
) -> Result<ChannelInventory, TransportError> {
    let mut inventory = ChannelInventory {
        receivers: Vec::new(),
        transmitters: Vec::new(),
        audio: None,
    };
    let mut starting_channel = 1;
    let mut seen = BTreeSet::new();

    while inventory.receivers.len() < usize::from(counts.rx_count) {
        let response = transport
            .request(
                |identifier| commands::build_receivers_from(starting_channel, identifier),
                Target::Arc,
                CommandIntent::Read,
            )
            .await?;
        inventory.audio = inventory
            .audio
            .or_else(|| parse_channel_audio_metadata(&response));
        let page = parse_rx_page(&response, starting_channel)
            .ok_or(TransportError::MalformedResponse("receiver channels"))?;
        let continues = page.len() == usize::from(RX_CHANNELS_PER_PAGE) || more_pages(&response);
        let last = page.last().map(|channel| channel.number);

        for channel in &page {
            if channel.number < starting_channel || !seen.insert(channel.number) {
                return Err(TransportError::MalformedResponse(
                    "receiver channel page did not advance",
                ));
            }
        }

        inventory.receivers.extend(page);

        if !continues || last.is_none() || inventory.receivers.len() >= usize::from(counts.rx_count)
        {
            break;
        }

        starting_channel = next_channel(last, "receiver channel range")?;
    }

    if inventory.receivers.len() != usize::from(counts.rx_count) {
        return Err(TransportError::MalformedResponse(
            "receiver channel count disagrees with inventory",
        ));
    }

    if counts.tx_count == 0 {
        return Ok(inventory);
    }

    let response = transport
        .request(
            |identifier| commands::build_transmitter_names(counts.tx_count, identifier),
            Target::Arc,
            CommandIntent::Read,
        )
        .await?;
    let names = parse_tx_friendly_page(&response, 1).ok_or(TransportError::MalformedResponse(
        "transmitter friendly names",
    ))?;
    starting_channel = 1;
    seen.clear();

    while inventory.transmitters.len() < usize::from(counts.tx_count) {
        let response = transport
            .request(
                |identifier| commands::build_transmitters_from(starting_channel, identifier),
                Target::Arc,
                CommandIntent::Read,
            )
            .await?;
        inventory.audio = inventory
            .audio
            .or_else(|| parse_channel_audio_metadata(&response));
        let page = parse_tx_info_page(&response, starting_channel)
            .ok_or(TransportError::MalformedResponse("transmitter channels"))?;
        let declared = response
            .get(RESPONSE_HEADER_SIZE + 1)
            .copied()
            .map(usize::from);
        let continues = page.len() == usize::from(TX_CHANNELS_PER_PAGE)
            || (more_pages(&response) && Some(page.len()) == declared);
        let last = page.last().map(|channel| channel.number);

        for channel in &page {
            if channel.number < starting_channel || !seen.insert(channel.number) {
                return Err(TransportError::MalformedResponse(
                    "transmitter channel page did not advance",
                ));
            }
        }

        inventory.transmitters.extend(page);

        if !continues || last.is_none() {
            break;
        }

        starting_channel = next_channel(last, "transmitter channel range")?;
    }

    if inventory.transmitters.is_empty()
        || inventory.transmitters.len() > usize::from(counts.tx_count)
    {
        return Err(TransportError::MalformedResponse(
            "transmitter channel count disagrees with inventory",
        ));
    }

    for channel in &mut inventory.transmitters {
        if let Some((_, name)) = names.iter().find(|(number, _)| *number == channel.number) {
            channel.friendly_name = Some(name.clone());
        }
    }

    Ok(inventory)
}

fn more_pages(response: &[u8]) -> bool {
    response_envelope(response)
        .is_some_and(|envelope| envelope.result_code == RESULT_CODE_MORE_PAGES)
}

fn next_channel(last: Option<u16>, operation: &'static str) -> Result<u16, TransportError> {
    last.and_then(|number| number.checked_add(1))
        .ok_or(TransportError::MalformedResponse(operation))
}
