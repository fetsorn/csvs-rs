use super::into_value::IntoValue;
use super::Grain;
use serde_json::{json, Value};

impl IntoValue for Grain {
    fn into_value(self) -> Value {
        match self.leaf_value {
            Some(leaf_value) => json!({
                "_": self.base,
                self.base: self.base_value,
                self.leaf: leaf_value
            }),
            None => json!({
                "_": self.base,
                self.base: self.base_value
            }),
        }
    }
}
