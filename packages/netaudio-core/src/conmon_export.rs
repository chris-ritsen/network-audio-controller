use std::collections::BTreeMap;
use std::num::NonZeroUsize;

use serde::{Deserialize, Serialize};

use crate::bytes::hexadecimal;
use crate::responses::ConmonExportFragment;

#[derive(Clone, Copy, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum ExportKind {
    DiagnosticLogs,
    CapabilityPartition,
}

impl ExportKind {
    pub(crate) fn identity(self) -> ([u8; 4], u16) {
        match self {
            Self::DiagnosticLogs => (*b"LOGS", 1),
            Self::CapabilityPartition => (*b"CAP1", 2),
        }
    }
}

#[derive(Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ExportConfiguration {
    pub kind: ExportKind,
    pub maximum_encoded_size: NonZeroUsize,
}

#[derive(Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ExportResult {
    kind: ExportKind,
    echoed_tag_hexadecimal: String,
    selector_value: u16,
    record_protocol_identifier: u16,
    encoded_payload_hexadecimal: String,
    fragment_count: u16,
}

#[derive(Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ExportProgress<'a> {
    pub matched: bool,
    pub result: Option<&'a ExportResult>,
}

pub struct ExportCollector {
    configuration: ExportConfiguration,
    tag: String,
    total_size: Option<usize>,
    record_protocol: Option<u16>,
    terminal: Option<u16>,
    received_size: usize,
    fragments: BTreeMap<u16, (String, bool)>,
    result: Option<ExportResult>,
}

impl ExportCollector {
    pub fn new(configuration: ExportConfiguration) -> Self {
        Self {
            tag: hexadecimal(&configuration.kind.identity().0),
            configuration,
            total_size: None,
            record_protocol: None,
            terminal: None,
            received_size: 0,
            fragments: BTreeMap::new(),
            result: None,
        }
    }

    /// Rejected fragments leave assembly unchanged. Unrelated exports are ignored.
    pub fn accept(
        &mut self,
        fragment: ConmonExportFragment,
    ) -> Result<ExportProgress<'_>, &'static str> {
        let matched = fragment
            .echoed_tag_hexadecimal
            .eq_ignore_ascii_case(&self.tag)
            && fragment.selector_value == self.configuration.kind.identity().1;

        if !matched || self.result.is_some() {
            return Ok(ExportProgress {
                matched,
                result: matched.then_some(self.result.as_ref()).flatten(),
            });
        }

        let total = fragment.total_encoded_size as usize;
        let size = usize::from(fragment.fragment_size);
        let identifier = fragment.fragment_identifier;
        let more = fragment.has_more_fragments;

        if total == 0
            || total > self.configuration.maximum_encoded_size.get()
            || identifier == 0
            || (identifier == u16::MAX && more)
            || size == 0
            || size > total
        {
            return Err("ConMon export fragment fields are invalid");
        }

        if fragment.data_hexadecimal.len() != size * 2
            || !fragment
                .data_hexadecimal
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
        {
            return Err("ConMon export fragment data or size is invalid");
        }

        if self.total_size.is_some_and(|value| value != total) {
            return Err("ConMon export total encoded size changed");
        }

        if self
            .record_protocol
            .is_some_and(|value| value != fragment.record_protocol_identifier)
        {
            return Err("ConMon export record protocol identifier changed");
        }

        let current = (fragment.data_hexadecimal.to_ascii_lowercase(), more);

        if let Some(existing) = self.fragments.get(&identifier) {
            return if existing == &current {
                Ok(ExportProgress {
                    matched: true,
                    result: None,
                })
            } else {
                Err("ConMon export fragment identifier conflicts")
            };
        }

        if self.terminal.is_some_and(|terminal| identifier > terminal)
            || (!more && self.fragments.keys().any(|existing| *existing > identifier))
        {
            return Err("ConMon export fragment follows the terminal fragment");
        }

        if !more && self.terminal.is_some_and(|terminal| terminal != identifier) {
            return Err("ConMon export has multiple terminal fragments");
        }

        let received = self
            .received_size
            .checked_add(size)
            .filter(|received| *received <= total)
            .ok_or("ConMon export fragments exceed the declared encoded size")?;
        let terminal = if more {
            self.terminal
        } else {
            Some(identifier)
        };
        // Identifiers are unique, positive, and bounded by the terminal, so this
        // count also proves there are no gaps in the completed sequence.
        let complete = terminal.is_some_and(|last| self.fragments.len() + 1 == usize::from(last));

        if complete && received != total {
            return Err("ConMon export fragments do not cover the declared encoded size");
        }

        self.total_size = Some(total);
        self.record_protocol = Some(fragment.record_protocol_identifier);
        self.terminal = terminal;
        self.received_size = received;
        self.fragments.insert(identifier, current);

        if complete {
            self.result = Some(ExportResult {
                kind: self.configuration.kind,
                echoed_tag_hexadecimal: self.tag.clone(),
                selector_value: self.configuration.kind.identity().1,
                record_protocol_identifier: fragment.record_protocol_identifier,
                encoded_payload_hexadecimal: std::mem::take(&mut self.fragments)
                    .into_values()
                    .map(|(payload, _)| payload)
                    .collect(),
                fragment_count: terminal.expect("completed export has a terminal"),
            });
        }

        Ok(ExportProgress {
            matched: true,
            result: self.result.as_ref(),
        })
    }
}
