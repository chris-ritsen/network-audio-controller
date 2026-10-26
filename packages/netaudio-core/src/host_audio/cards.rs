use super::snapshot::JackPort;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Card {
    pub index: u32,
    pub id: String,
    pub driver: String,
    pub name: String,
    pub long_name: Option<String>,
    pub usb_id: Option<String>,
    pub playback_devices: Vec<u32>,
    pub capture_devices: Vec<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usb_vendor: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usb_product: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usb_serial: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usb_path: Option<String>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    Capture,
    Playback,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Bridge {
    pub card: String,
    pub direction: Direction,
    pub program: String,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Channel {
    pub card: String,
    pub pcm: u32,
    pub direction: Direction,
    pub channel: u32,
}

pub fn parse_listing(text: &str) -> Vec<Card> {
    let mut lines = text.lines().peekable();
    let mut cards = Vec::new();
    while let Some(line) = lines.next() {
        let Some((index, remainder)) = line.trim_start().split_once('[') else {
            continue;
        };
        let Ok(index) = index.trim().parse() else {
            continue;
        };
        let Some((identifier, remainder)) = remainder.split_once("]:") else {
            continue;
        };
        let Some((driver, name)) = remainder.trim().split_once(" - ") else {
            continue;
        };
        let long_name = lines
            .peek()
            .filter(|line| line.starts_with(char::is_whitespace))
            .map(|line| line.trim().to_owned())
            .filter(|line| !line.is_empty());
        cards.push(Card {
            index,
            id: identifier.trim().to_owned(),
            driver: driver.trim().to_owned(),
            name: name.trim().to_owned(),
            long_name,
            usb_id: None,
            playback_devices: Vec::new(),
            capture_devices: Vec::new(),
            usb_vendor: None,
            usb_product: None,
            usb_serial: None,
            usb_path: None,
        });
    }
    cards
}

pub fn reference<'a>(value: &str, cards: &'a [Card]) -> Option<&'a Card> {
    let value = value.trim();
    let value = value
        .split_once("CARD=")
        .or_else(|| value.split_once(':'))
        .map_or(value, |(_, value)| value);
    let value = value.split(',').next().unwrap_or(value);
    cards.iter().find(|card| {
        value == card.id || value.parse::<u32>().is_ok_and(|index| index == card.index)
    })
}

pub fn is_dante(card: &Card) -> bool {
    let text = format!(
        "{} {} {} {} {}",
        card.id,
        card.driver,
        card.name,
        card.long_name.as_deref().unwrap_or_default(),
        card.usb_vendor.as_deref().unwrap_or_default()
    );
    caseless::default_case_fold_str(&text).contains("dante")
        || card
            .usb_id
            .as_deref()
            .is_some_and(|value| value.starts_with("3018:"))
}

pub fn hardware_channel(port: &JackPort, cards: &[Card]) -> Option<Channel> {
    hardware_aliases(&port.aliases, cards)
}

pub fn hardware_aliases(aliases: &[String], cards: &[Card]) -> Option<Channel> {
    aliases.iter().find_map(|alias| {
        let alias = alias.strip_prefix("alsa_pcm:hw:")?;
        let (device, channel) = alias.split_once(':')?;
        let (identifier, pcm) = device.split_once(',')?;
        let card = reference(identifier, cards)?;
        let (direction, channel) = if let Some(channel) = channel.strip_prefix("out") {
            (Direction::Capture, channel)
        } else {
            (Direction::Playback, channel.strip_prefix("in")?)
        };
        Some(Channel {
            card: card.id.clone(),
            pcm: pcm.parse().ok()?,
            direction,
            channel: channel.parse().ok()?,
        })
    })
}

pub fn bridge(arguments: &[String], cards: &[Card]) -> Option<(String, Bridge)> {
    let program = arguments.first()?.rsplit('/').next()?;
    let direction = match program {
        "alsa_in" | "zita-a2j" => Direction::Capture,
        "alsa_out" | "zita-j2a" => Direction::Playback,
        _ => return None,
    };
    let option = |name| {
        arguments
            .windows(2)
            .find(|pair| pair[0] == name)
            .map(|pair| pair[1].clone())
    };
    let client = option("-j")?;
    let device = option("-d")?;
    let card = reference(&device, cards)?;
    Some((
        client,
        Bridge {
            card: card.id.clone(),
            direction,
            program: program.to_owned(),
        },
    ))
}

pub fn bridge_channel(name: &str, bridges: &BTreeMap<String, Bridge>) -> Option<Channel> {
    let (client, name) = name.split_once(':')?;
    let bridge = bridges.get(client)?;
    let channel = name
        .strip_prefix("capture_")
        .or_else(|| name.strip_prefix("playback_"))?;
    Some(Channel {
        card: bridge.card.clone(),
        pcm: 0,
        direction: bridge.direction,
        channel: channel.parse().ok()?,
    })
}
