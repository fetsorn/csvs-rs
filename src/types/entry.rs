use super::into_value::IntoValue;
use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::convert::TryInto;
use std::fmt;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Entry {
    pub base: String,
    pub base_value: Option<String>,
    pub leader_value: Option<String>,
    pub leaves: HashMap<String, Vec<Entry>>,
}

impl fmt::Display for Entry {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.clone().into_value())
    }
}

impl TryFrom<Value> for Entry {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self> {
        // validate that value is object
        match value {
            Value::Null => Err(Error::from_message("")),
            Value::Bool(_) => Err(Error::from_message("")),
            Value::Number(_) => Err(Error::from_message("")),
            Value::String(_) => Err(Error::from_message("")),
            Value::Array(_) => Err(Error::from_message("")),
            Value::Object(v) => {
                let base = match &v["_"] {
                    Value::Null => return Err(Error::from_message("")),
                    Value::Bool(_) => return Err(Error::from_message("")),
                    Value::Number(_) => return Err(Error::from_message("")),
                    Value::String(s) => s,
                    Value::Array(_) => return Err(Error::from_message("")),
                    Value::Object(_) => return Err(Error::from_message("")),
                };

                let base_value = if v.contains_key(base) {
                    match &v[base] {
                        Value::Null => return Err(Error::from_message("")),
                        Value::Bool(_) => return Err(Error::from_message("")),
                        Value::Number(_) => return Err(Error::from_message("")),
                        Value::String(s) => Some(s),
                        Value::Array(_) => return Err(Error::from_message("")),
                        Value::Object(_) => return Err(Error::from_message("")),
                    }
                } else {
                    None
                };

                let leader_value = if v.contains_key("__") {
                    match &v["__"] {
                        Value::Null => return Err(Error::from_message("")),
                        Value::Bool(_) => return Err(Error::from_message("")),
                        Value::Number(_) => return Err(Error::from_message("")),
                        Value::String(s) => Some(s),
                        Value::Array(_) => return Err(Error::from_message("")),
                        Value::Object(_) => return Err(Error::from_message("")),
                    }
                } else {
                    None
                };

                let leaves = v
                    .iter()
                    .filter(|(key, _)| (*key != "_") && (*key != base) && (*key != "__"))
                    .map(|(key, val)| {
                        let values: Vec<Entry> = match val {
                            Value::Null => return Err(Error::from_message("")),
                            Value::Bool(_) => return Err(Error::from_message("")),
                            Value::Number(_) => return Err(Error::from_message("")),
                            Value::String(s) => {
                                vec![Entry {
                                    base: key.to_owned(),
                                    base_value: Some(s.to_owned()),
                                    leader_value: None,
                                    leaves: HashMap::new(),
                                }]
                            }
                            Value::Array(vs) => vs
                                .iter()
                                .map(|v| match v {
                                    Value::Null => return Err(Error::from_message("")),
                                    Value::Bool(_) => return Err(Error::from_message("")),
                                    Value::Number(_) => return Err(Error::from_message("")),
                                    Value::String(s) => Ok(Entry {
                                        base: key.to_owned(),
                                        base_value: Some(s.to_owned()),
                                        leader_value: None,
                                        leaves: HashMap::new(),
                                    }),
                                    Value::Array(_) => return Err(Error::from_message("")),
                                    Value::Object(_) => {
                                        let e: Entry = v.clone().try_into()?;

                                        Ok(e)
                                    }
                                })
                                .collect::<Result<Vec<Entry>>>()?,
                            Value::Object(_) => {
                                let e: Entry = val.clone().try_into()?;

                                vec![e]
                            }
                        };

                        Ok((key.to_owned(), values))
                    })
                    .collect::<Result<HashMap<String, Vec<Entry>>>>()?;

                Ok(Entry {
                    base: base.to_owned(),
                    base_value: base_value.cloned(),
                    leader_value: leader_value.cloned(),
                    leaves,
                })
            }
        }
    }
}

impl TryFrom<String> for Entry {
    type Error = Error;

    fn try_from(value: String) -> Result<Self> {
        let value_json: Value = serde_json::from_str(&value)?;

        value_json.try_into()
    }
}

impl TryFrom<&str> for Entry {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self> {
        let value_json: Value = serde_json::from_str(value)?;

        value_json.try_into()
    }
}

impl IntoValue for Entry {
    fn into_value(self) -> Value {
        let mut value: Value = json!({
            "_": self.base,
        });

        match self.base_value {
            None => (),
            Some(s) => value[self.base] = s.into(),
        }

        match self.leader_value {
            None => (),
            Some(s) => value["__"] = s.into(),
        }

        for (leaf, items) in self.leaves.iter() {
            for entry in items {
                // condense entry to a string if it has no leaves
                let leaf_value: Value = match entry.leaves.is_empty() {
                    true => match &entry.base_value {
                        None => continue,
                        Some(s) => s.to_owned().into(),
                    },
                    false => entry.clone().into_value(),
                };

                value[&leaf] = match value.get(leaf) {
                    None => leaf_value,
                    Some(i) => match i {
                        Value::Null => panic!("unreachable"),
                        Value::Bool(_) => panic!("unreachable"),
                        Value::Number(_) => panic!("unreachable"),
                        Value::String(s) => vec![s.to_owned().into(), leaf_value].into(),
                        Value::Object(o) => vec![o.clone().into(), leaf_value].into(),
                        Value::Array(vs) => [&vs[..], &[leaf_value]].concat().into(),
                    },
                };
            }
        }

        value
    }
}
