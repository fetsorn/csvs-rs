use assert_json_diff::assert_json_eq;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::convert::Into;
use std::convert::TryFrom;
use std::fs;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Grain {
    pub base: String,
    pub base_value: Option<String>,
    pub leaf: String,
    pub leaf_value: Option<String>,
}

impl TryFrom<Value> for Grain {
    type Error = ();

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        Ok(Grain {
            base: "".to_owned(),
            base_value: Some("".to_owned()),
            leaf: "".to_owned(),
            leaf_value: Some("".to_owned())
        })
    }
}

impl Into<Value> for Grain {
    fn into(self) -> Value {
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
