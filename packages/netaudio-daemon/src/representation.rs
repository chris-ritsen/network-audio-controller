use serde_json::Value;

pub fn browser_values(value: &mut Value) {
    match value {
        Value::Object(fields) => {
            for (name, value) in fields {
                if matches!(name.as_str(), "session_id" | "session_version")
                    && let Value::Number(number) = value
                    && (number.is_i64() || number.is_u64())
                {
                    *value = Value::String(number.to_string());
                    continue;
                }

                browser_values(value);
            }
        }
        Value::Array(values) => {
            for value in values {
                browser_values(value);
            }
        }
        _ => {}
    }
}
