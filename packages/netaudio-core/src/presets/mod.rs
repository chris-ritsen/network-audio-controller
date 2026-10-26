mod format;
mod json;
mod normalize;
mod parsing;
pub mod planning;
mod xml;

use std::collections::BTreeMap;

use serde_json::{Map, Value};

pub use normalize::normalize_configuration;

pub const MAXIMUM_BYTES: usize = 4 * 1024 * 1024;
pub const MAXIMUM_DEVICES: usize = 128;
pub const SCHEMA_VERSION: u32 = 3;

const EXTENSION: &str = "netaudio_configuration";

#[derive(Clone, Debug)]
pub struct Document {
    pub name: String,
    devices: BTreeMap<String, Device>,
    root: xml::Element,
}

#[derive(Clone, Debug)]
pub struct Device {
    configuration: Map<String, Value>,
    element: xml::Element,
}

#[derive(Clone, Debug, Default)]
pub struct XmlIdentity {
    pub server_name: Option<String>,
    pub mac_address: Option<String>,
    pub manufacturer: Option<String>,
    pub model_identifier: Option<String>,
    pub model_name: Option<String>,
    pub model_version: Option<String>,
}

impl Document {
    pub fn new(name: impl Into<String>) -> Self {
        let name = name.into();
        let mut root = xml::Element::new("preset");
        root.attributes.insert("version".into(), "2.1.0".into());
        root.set_text("name", &name);
        root.set_text(
            "description",
            "Dante Controller preset with NetAudio schema-v3 extension",
        );

        Self {
            name,
            devices: BTreeMap::new(),
            root,
        }
    }

    pub fn parse(content: &str) -> Result<Self, String> {
        parsing::parse(content)
    }

    pub fn devices(&self) -> &BTreeMap<String, Device> {
        &self.devices
    }

    pub fn insert(&mut self, configuration: Value, identity: XmlIdentity) -> Result<(), String> {
        let configuration = normalize_configuration(configuration)?;
        let name = configuration["name"].as_str().unwrap().to_owned();

        if self.devices.contains_key(&name) {
            return Err(format!("duplicate preset device name: {name:?}"));
        }

        if self.devices.len() == MAXIMUM_DEVICES {
            return Err("a preset must contain at most 128 devices".into());
        }

        let element = format::identity_element(&name, identity)?;
        self.devices.insert(
            name,
            Device {
                configuration,
                element,
            },
        );
        Ok(())
    }

    pub fn to_xml(&self) -> Result<String, String> {
        format::serialize(self)
    }
}

impl Device {
    pub fn configuration(&self) -> &Map<String, Value> {
        &self.configuration
    }
}
