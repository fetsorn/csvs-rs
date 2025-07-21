use super::Entry;
use crate::{Error, Result};
use serde_json::Value;
use std::collections::HashMap;

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
