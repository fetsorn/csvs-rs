use serde_json::Value;

pub trait IntoValue {
    fn into_value(self) -> Value;
}
