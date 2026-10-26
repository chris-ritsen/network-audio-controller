use serde::de::{Error, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::{Map, Number, Value};

struct UniqueValue(Value);

impl<'de> Deserialize<'de> for UniqueValue {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct UniqueVisitor;

        impl<'de> Visitor<'de> for UniqueVisitor {
            type Value = UniqueValue;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("JSON with distinct object keys")
            }

            fn visit_bool<E: Error>(self, value: bool) -> Result<Self::Value, E> {
                Ok(UniqueValue(Value::Bool(value)))
            }

            fn visit_i64<E: Error>(self, value: i64) -> Result<Self::Value, E> {
                Ok(UniqueValue(Value::Number(value.into())))
            }

            fn visit_u64<E: Error>(self, value: u64) -> Result<Self::Value, E> {
                Ok(UniqueValue(Value::Number(value.into())))
            }

            fn visit_f64<E: Error>(self, value: f64) -> Result<Self::Value, E> {
                Number::from_f64(value)
                    .map(|value| UniqueValue(Value::Number(value)))
                    .ok_or_else(|| E::custom("JSON numbers must be finite"))
            }

            fn visit_str<E: Error>(self, value: &str) -> Result<Self::Value, E> {
                Ok(UniqueValue(Value::String(value.into())))
            }

            fn visit_string<E: Error>(self, value: String) -> Result<Self::Value, E> {
                Ok(UniqueValue(Value::String(value)))
            }

            fn visit_unit<E: Error>(self) -> Result<Self::Value, E> {
                Ok(UniqueValue(Value::Null))
            }

            fn visit_seq<A: SeqAccess<'de>>(
                self,
                mut sequence: A,
            ) -> Result<Self::Value, A::Error> {
                let mut values = Vec::new();

                while let Some(UniqueValue(value)) = sequence.next_element()? {
                    values.push(value);
                }

                Ok(UniqueValue(Value::Array(values)))
            }

            fn visit_map<A: MapAccess<'de>>(self, mut mapping: A) -> Result<Self::Value, A::Error> {
                let mut values = Map::new();

                while let Some((name, UniqueValue(value))) =
                    mapping.next_entry::<String, UniqueValue>()?
                {
                    if values.insert(name.clone(), value).is_some() {
                        return Err(A::Error::custom(format!(
                            "duplicate JSON object key: {name}"
                        )));
                    }
                }

                Ok(UniqueValue(Value::Object(values)))
            }
        }

        deserializer.deserialize_any(UniqueVisitor)
    }
}

pub(super) fn parse(content: &str) -> Result<Value, serde_json::Error> {
    serde_json::from_str::<UniqueValue>(content).map(|value| value.0)
}
