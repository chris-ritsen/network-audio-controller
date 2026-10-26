use anyhow::{Context, Result, ensure};
use redis::{ConnectionAddr, ConnectionInfo, IntoConnectionInfo, RedisConnectionInfo};
use toml::Value;

pub(super) fn connection(
    configuration: &crate::configuration::Configuration,
) -> Result<Option<ConnectionInfo>> {
    let configured = configuration.profile.get("redis");
    let mut settings = configured
        .map(|value| value.as_table().cloned().context("redis must be a table"))
        .transpose()?
        .unwrap_or_default();
    for (field, variable) in [
        ("socket", "REDIS_SOCKET"),
        ("host", "REDIS_HOST"),
        ("port", "REDIS_PORT"),
        ("db", "REDIS_DB"),
        ("password", "REDIS_PASSWORD"),
    ] {
        if let Some(value) = std::env::var_os(variable).filter(|value| !value.is_empty()) {
            settings.insert(
                field.into(),
                Value::String(
                    value
                        .into_string()
                        .map_err(|_| anyhow::anyhow!("{variable} must be UTF-8"))?,
                ),
            );
        }
    }
    if configured.is_none()
        && !["socket", "host", "port"]
            .iter()
            .any(|field| settings.contains_key(*field))
    {
        return Ok(None);
    }
    let text = |field| -> Result<Option<String>> {
        settings
            .get(field)
            .map(|value| {
                value
                    .as_str()
                    .map(str::to_owned)
                    .context(format!("redis.{field} must be a string"))
            })
            .transpose()
    };
    let number = |field, default| -> Result<i64> {
        match settings.get(field) {
            None => Ok(default),
            Some(Value::Integer(value)) => Ok(*value),
            Some(Value::String(value)) => value
                .parse()
                .with_context(|| format!("redis.{field} must be an integer")),
            _ => anyhow::bail!("redis.{field} must be an integer"),
        }
    };
    let database = number("db", 0)?;
    ensure!(database >= 0, "redis.db must not be negative");
    let mut authentication = RedisConnectionInfo::default()
        .set_db(database)
        .set_protocol(redis::ProtocolVersion::RESP3);
    if let Some(password) = text("password")? {
        authentication = authentication.set_password(password);
    }
    let socket = ["socket", "socket_path", "unix_socket"]
        .into_iter()
        .map(text)
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .find(|value| !value.is_empty());
    let address = if let Some(socket) = socket {
        ConnectionAddr::Unix(std::path::PathBuf::from(socket))
    } else {
        let port =
            u16::try_from(number("port", 6379)?).context("redis.port must be a valid port")?;
        ensure!(port != 0, "redis.port must be nonzero");
        ConnectionAddr::Tcp(
            text("host")?
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| "localhost".into()),
            port,
        )
    };
    Ok(Some(
        address
            .into_connection_info()?
            .set_redis_settings(authentication),
    ))
}
