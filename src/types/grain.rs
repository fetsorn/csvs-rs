use super::into_value::IntoValue;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::convert::Into;
use std::convert::TryFrom;

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
        match value {
            Value::Null => panic!(""),
            Value::Bool(_) => panic!(""),
            Value::Number(_) => panic!(""),
            Value::String(_) => panic!(""),
            Value::Array(_) => panic!(""),
            Value::Object(v) => {
                let base: String = match &v["_"] {
                    Value::Null => panic!(""),
                    Value::Bool(_) => panic!(""),
                    Value::Number(_) => panic!(""),
                    Value::String(s) => s.to_owned(),
                    Value::Array(_) => panic!(""),
                    Value::Object(_) => panic!(""),
                };

                let base_value: Option<String> = if v.contains_key(&base) {
                    match &v[&base] {
                        Value::Null => panic!(""),
                        Value::Bool(_) => panic!(""),
                        Value::Number(_) => panic!(""),
                        Value::String(s) => Some(s.to_owned()),
                        Value::Array(_) => panic!(""),
                        Value::Object(_) => panic!(""),
                    }
                } else {
                    None
                };

                let leaves: HashMap<String, Value> = v
                    .iter()
                    .filter(|&(key, _)| (*key != "_") && (**key != base))
                    .map(|(key, val)| (key.to_owned(), val.clone()))
                    .collect();

                if leaves.is_empty() {
                    return Ok(Grain {
                        base: base.to_owned(),
                        base_value: base_value.to_owned(),
                        leaf: "".to_owned(),
                        leaf_value: None,
                    });
                }

                if leaves.len() > 1 {
                    panic!()
                };

                let leaf = leaves.keys().nth(0).unwrap();

                let leaf_value: Option<String> = match &v[leaf] {
                    Value::Null => panic!(""),
                    Value::Bool(_) => panic!(""),
                    Value::Number(_) => panic!(""),
                    Value::String(s) => Some(s.to_owned()),
                    Value::Array(_) => panic!(""),
                    Value::Object(_) => panic!(""),
                };

                Ok(Grain {
                    base: base.to_owned(),
                    base_value: base_value.clone(),
                    leaf: leaf.to_owned(),
                    leaf_value,
                })
            }
        }
    }
}

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
