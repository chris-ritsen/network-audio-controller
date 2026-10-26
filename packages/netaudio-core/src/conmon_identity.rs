use serde::Serialize;
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Identity {
    pub cmc_process: Option<u16>,
    pub cmc_process_state: &'static str,
    pub cmc_source_identifier: Option<String>,
    pub cmc_txt_id_raw: Option<String>,
    pub cmc_txt_process_raw: Option<String>,
}

impl Default for Identity {
    fn default() -> Self {
        Self {
            cmc_process: None,
            cmc_process_state: "none",
            cmc_source_identifier: None,
            cmc_txt_id_raw: None,
            cmc_txt_process_raw: None,
        }
    }
}

pub fn source_identifier(value: &str) -> Option<String> {
    (value.len() == 16 && value.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .then(|| value.to_ascii_lowercase())
}

impl Identity {
    pub fn advertised(&self) -> Option<(&str, u16)> {
        (self.cmc_process_state == "advertised")
            .then_some((self.cmc_source_identifier.as_deref()?, self.cmc_process?))
    }

    pub fn from_properties(properties: &BTreeMap<String, Option<String>>) -> Self {
        let identifier = properties.get("id").cloned().flatten();
        let raw_process = properties.get("process").cloned().flatten();
        let process = raw_process.as_deref().and_then(|value| {
            (!value.is_empty()
                && value.len() <= 5
                && value.bytes().all(|byte| byte.is_ascii_digit()))
            .then(|| value.parse::<u16>().ok())
            .flatten()
        });
        Self {
            cmc_process: process,
            cmc_process_state: if !properties.contains_key("process") {
                "missing"
            } else if process.is_some() {
                "advertised"
            } else {
                "invalid"
            },
            cmc_source_identifier: identifier.as_deref().and_then(source_identifier),
            cmc_txt_id_raw: identifier,
            cmc_txt_process_raw: raw_process,
        }
    }

    pub fn from_memberships<'a>(
        memberships: impl IntoIterator<Item = &'a BTreeMap<String, Option<String>>>,
    ) -> Self {
        let mut identities = memberships.into_iter().map(Self::from_properties);
        let Some(first) = identities.next() else {
            return Self::default();
        };
        let mut identifier = first.cmc_source_identifier.clone();
        let mut contradictory = false;
        for identity in identities {
            contradictory |= identity.cmc_source_identifier != first.cmc_source_identifier
                || identity.cmc_process != first.cmc_process
                || identity.cmc_process_state != first.cmc_process_state;
            if identity.cmc_source_identifier != identifier {
                identifier = None;
            }
        }
        if contradictory {
            Self {
                cmc_process_state: "contradictory",
                cmc_source_identifier: identifier,
                ..Self::default()
            }
        } else {
            first
        }
    }
}
