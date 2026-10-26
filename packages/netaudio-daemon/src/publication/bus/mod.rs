mod interfaces;
mod views;

use crate::state::ReadModel;
use anyhow::{Context, Result, ensure};
use blake2::digest::{Update, VariableOutput};
use futures_util::StreamExt;
use interfaces::{DanteChannel, DanteDevice, Manager, ShureChannel, ShureDevice};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use zbus::object_server::{Interface, SignalEmitter};

const ROOT: &str = "/com/netaudio";

#[derive(Default)]
struct Inventory {
    dante: BTreeMap<String, Value>,
    wireless: BTreeMap<String, Value>,
    children: BTreeMap<String, BTreeSet<String>>,
}

pub async fn run(model: Arc<ReadModel>, cancellation: CancellationToken) -> Result<()> {
    let mut delay = Duration::from_millis(250);
    loop {
        let result = tokio::select! {
            _ = cancellation.cancelled() => return Ok(()),
            result = connected(&model, &mut delay) => result,
        };
        let error = result.expect_err("D-Bus service runs until disconnected");
        tracing::warn!(%error, "D-Bus publication disconnected");
        status(&model, "disconnected", Some(error.to_string()))?;
        tokio::select! {
            _ = cancellation.cancelled() => return Ok(()),
            _ = tokio::time::sleep(delay) => {},
        }
        delay = (delay * 2).min(Duration::from_secs(30));
    }
}

async fn connected(model: &ReadModel, delay: &mut Duration) -> Result<()> {
    let builder = zbus::connection::Builder::session()?
        .name("com.netaudio.Daemon")?
        .allow_name_replacements(false)
        .replace_existing_names(false)
        .serve_at(ROOT, zbus::fdo::ObjectManager)?;
    let connection = tokio::time::timeout(Duration::from_secs(5), builder.build())
        .await
        .context("D-Bus connection exceeded its deadline")??;
    let mut messages = zbus::MessageStream::from(&connection);
    let mut subscription = model.subscribe(None)?;
    let mut inventory = Inventory::default();
    inventory.manager(&connection).await?;
    loop {
        tokio::select! {
            message = messages.next() => {
                message.context("D-Bus connection closed")??;
            }
            event = subscription.next() => {
                let event = event.context("D-Bus publication lost its event subscription")?;
                match event.value["event"].as_str().unwrap_or("") {
                    crate::events::SNAPSHOT => {
                        for (name, record) in event.value["devices"].as_object().into_iter().flatten() {
                            inventory.dante(&connection, name, record.clone()).await?;
                        }
                        for (mac, record) in event.value["shure_devices"].as_object().into_iter().flatten() {
                            inventory.wireless(&connection, mac, record.clone()).await?;
                        }
                        *delay = Duration::from_millis(250);
                        status(model, "ready", None)?;
                    }
                    crate::events::DEVICE_DISCOVERED | crate::events::DEVICE_UPDATED | crate::events::TELEMETRY_UPDATED => {
                        let name = event.value["server_name"].as_str().context("D-Bus device identity missing")?;
                        inventory.dante(&connection, name, event.value["device"].clone()).await?;
                    }
                    crate::events::DEVICE_REMOVED => {
                        let name = event.value["server_name"].as_str().context("D-Bus removed device identity missing")?;
                        inventory.dante(&connection, name, Value::Null).await?;
                    }
                    "shure_device_discovered" | "shure_device_updated" => {
                        let mac = event.value["mac"].as_str().context("D-Bus wireless identity missing")?;
                        inventory.wireless(&connection, mac, event.value["device"].clone()).await?;
                    }
                    "shure_device_removed" => {
                        let mac = event.value["mac"].as_str().context("D-Bus removed wireless identity missing")?;
                        inventory.wireless(&connection, mac, Value::Null).await?;
                    }
                    crate::events::SHURE_METER_VALUES => {
                        let mac = event.value["mac"].as_str().context("D-Bus meter identity missing")?;
                        let snapshot = model.snapshot()?;
                        if let Some(record) = snapshot.value["shure_devices"].get(mac) {
                            inventory.wireless(&connection, mac, record.clone()).await?;
                        }
                    }
                    _ => {},
                }
            }
        }
    }
}

impl Inventory {
    async fn manager(&self, connection: &zbus::Connection) -> Result<()> {
        install(connection, ROOT, Manager { record: json!({
            "DanteDevices":self.dante.keys().collect::<Vec<_>>(),
            "ShureDevices":self.wireless.keys().collect::<Vec<_>>(),
            "DanteDeviceCount":self.dante.len(),
            "ShureDeviceCount":self.wireless.values().filter(|record| record["online"] == true).count(),
        }) }).await
    }

    async fn dante(
        &mut self,
        connection: &zbus::Connection,
        name: &str,
        record: Value,
    ) -> Result<()> {
        let path = format!("{ROOT}/dante/devices/{}", safe_name(name)?);
        let exists = self.dante.contains_key(name);
        if record["online"] != true {
            self.children::<DanteChannel>(connection, &path, BTreeSet::new())
                .await?;
            if exists {
                connection
                    .object_server()
                    .remove::<DanteDevice, _>(path.as_str())
                    .await?;
                self.dante.remove(name);
                self.manager(connection).await?;
                Manager::dante_device_removed(&SignalEmitter::new(connection, ROOT)?, name).await?;
            }
            return Ok(());
        }
        install(
            connection,
            &path,
            DanteDevice {
                record: views::dante_device(&record),
            },
        )
        .await?;
        let mut children = BTreeSet::new();
        for (direction, field) in [("tx", "transmitters"), ("rx", "receivers")] {
            for (number, channel) in record["channels"][field].as_object().into_iter().flatten() {
                let number = number
                    .parse::<u32>()
                    .context("D-Bus channel number is invalid")?;
                let child = format!("{path}/{direction}/{number}");
                let mut channel = channel.clone();
                channel["number"] = json!(number);
                install(
                    connection,
                    &child,
                    DanteChannel {
                        record: views::dante_channel(&channel),
                    },
                )
                .await?;
                children.insert(child);
            }
        }
        self.children::<DanteChannel>(connection, &path, children)
            .await?;
        self.dante.insert(name.to_owned(), record);
        self.manager(connection).await?;
        if !exists {
            Manager::dante_device_added(&SignalEmitter::new(connection, ROOT)?, name).await?;
        }
        Ok(())
    }

    async fn wireless(
        &mut self,
        connection: &zbus::Connection,
        mac: &str,
        record: Value,
    ) -> Result<()> {
        let identity = crate::shure::correlations::normalize(mac)
            .map(Ok)
            .unwrap_or_else(|| safe_name(&mac.to_ascii_lowercase()))?;
        let path = format!("{ROOT}/shure/devices/{identity}");
        let exists = self.wireless.contains_key(mac);
        if record.is_null() {
            self.children::<ShureChannel>(connection, &path, BTreeSet::new())
                .await?;
            if exists {
                connection
                    .object_server()
                    .remove::<ShureDevice, _>(path.as_str())
                    .await?;
                self.wireless.remove(mac);
                self.manager(connection).await?;
                Manager::shure_device_removed(&SignalEmitter::new(connection, ROOT)?, mac).await?;
            }
            return Ok(());
        }
        install(
            connection,
            &path,
            ShureDevice {
                record: views::shure_device(&record),
            },
        )
        .await?;
        let mut children = BTreeSet::new();
        for (number, channel) in record["channels"].as_object().into_iter().flatten() {
            let number = number
                .parse::<u32>()
                .context("D-Bus wireless channel number is invalid")?;
            let child = format!("{path}/channels/{number}");
            let mut channel = channel.clone();
            channel["number"] = json!(number);
            install(
                connection,
                &child,
                ShureChannel {
                    record: views::shure_channel(
                        &channel,
                        record["device_type"].as_str().unwrap_or(""),
                    ),
                },
            )
            .await?;
            children.insert(child);
        }
        self.children::<ShureChannel>(connection, &path, children)
            .await?;
        self.wireless.insert(mac.to_owned(), record);
        self.manager(connection).await?;
        if !exists {
            Manager::shure_device_added(&SignalEmitter::new(connection, ROOT)?, mac).await?;
        }
        Ok(())
    }

    async fn children<T: Interface>(
        &mut self,
        connection: &zbus::Connection,
        path: &str,
        current: BTreeSet<String>,
    ) -> Result<()> {
        for old in self.children.get(path).into_iter().flatten() {
            if !current.contains(old) {
                connection
                    .object_server()
                    .remove::<T, _>(old.as_str())
                    .await?;
            }
        }
        if current.is_empty() {
            self.children.remove(path);
        } else {
            self.children.insert(path.to_owned(), current);
        }
        Ok(())
    }
}

async fn install<T: Interface>(
    connection: &zbus::Connection,
    path: &str,
    current: T,
) -> Result<()> {
    let server = connection.object_server();
    let emitter = SignalEmitter::new(connection, path)?;
    if let Ok(interface) = server.interface::<_, T>(path).await {
        let mut previous = interface.get_mut().await;
        let before = previous.get_all(server, connection, None, &emitter).await?;
        let mut after = current.get_all(server, connection, None, &emitter).await?;
        let invalidated: Vec<_> = before
            .keys()
            .filter(|key| !after.contains_key(*key))
            .cloned()
            .collect();
        after.retain(|key, value| before.get(key) != Some(value));
        *previous = current;
        drop(previous);
        if !after.is_empty() || !invalidated.is_empty() {
            emitter
                .emit(
                    "org.freedesktop.DBus.Properties",
                    "PropertiesChanged",
                    &(T::name(), after, invalidated),
                )
                .await?;
        }
    } else {
        ensure!(
            server.at(path, current).await?,
            "D-Bus interface registration conflicted"
        );
    }
    Ok(())
}

fn safe_name(name: &str) -> Result<String> {
    let readable: String = name
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '_' {
                character
            } else {
                '_'
            }
        })
        .collect();
    let readable = readable.trim_matches('_');
    let readable = if readable.is_empty() {
        "device"
    } else {
        readable
    };
    let mut digest = blake2::Blake2sVar::new(6).context("Initialize D-Bus identity digest")?;
    digest.update(name.as_bytes());
    let mut bytes = [0; 6];
    digest
        .finalize_variable(&mut bytes)
        .context("Finalize D-Bus identity digest")?;
    Ok(format!("{readable}_{}", hex::encode(bytes)))
}

fn status(model: &ReadModel, state: &str, error: Option<String>) -> Result<()> {
    let value = json!({"state":state,"error":error});
    model.observe(
        "dbus_publication",
        value.clone(),
        json!({"event":"dbus_publication_status","dbus_publication":value}),
    )
}
