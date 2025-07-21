use super::Grain;
use crate::{Error, Result};
use serde_json::Value;
use std::convert::TryFrom;

impl TryFrom<Value> for Grain {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self> {
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

                let leaf: Option<(String, Value)> = v
                    .iter()
                    .filter(|(key, _)| (*key != "_") && (*key != base))
                    .try_fold(None, |with_pair, ((key, val))| {
                        if with_pair.is_some() {
                            Err(Error::from_message("more than one key in grain"))
                        } else {
                            Ok(Some((key.to_owned(), val.to_owned())))
                        }
                    })?;

                match leaf {
                    None => Ok(Grain {
                        base: base.to_owned(),
                        base_value: base_value.cloned(),
                        leaf: "".to_owned(),
                        leaf_value: None,
                    }),
                    Some((key, val)) => {
                        let leaf_value: Option<String> = match val {
                            Value::Null => return Err(Error::from_message("")),
                            Value::Bool(_) => return Err(Error::from_message("")),
                            Value::Number(_) => return Err(Error::from_message("")),
                            Value::String(s) => Some(s.to_owned()),
                            Value::Array(_) => return Err(Error::from_message("")),
                            Value::Object(_) => return Err(Error::from_message("")),
                        };

                        Ok(Grain {
                            base: base.to_owned(),
                            base_value: base_value.cloned(),
                            leaf: key,
                            leaf_value,
                        })
                    }
                }
            }
        }
    }
}
