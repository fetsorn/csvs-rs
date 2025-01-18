use super::into_value::IntoValue;
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
    type Error = ();

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        // validate that value is object
        let r = match value {
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
                    Value::String(s) => s.clone(),
                    Value::Array(_) => panic!(""),
                    Value::Object(_) => panic!(""),
                };

                let base_value: Option<String> = if v.contains_key(&base) {
                    match &v[&base] {
                        Value::Null => panic!(""),
                        Value::Bool(_) => panic!(""),
                        Value::Number(_) => panic!(""),
                        Value::String(s) => Some(s.clone()),
                        Value::Array(_) => panic!(""),
                        Value::Object(_) => panic!(""),
                    }
                } else {
                    None
                };

                let leader_value: Option<String> = if v.contains_key("__") {
                    match &v["__"] {
                        Value::Null => panic!(""),
                        Value::Bool(_) => panic!(""),
                        Value::Number(_) => panic!(""),
                        Value::String(s) => Some(s.clone()),
                        Value::Array(_) => panic!(""),
                        Value::Object(_) => panic!(""),
                    }
                } else {
                    None
                };

                let leaves: HashMap<String, Vec<Entry>> = v
                    .iter()
                    .filter(|(key, _)| (*key != "_") && (**key != base) && (**key != "__"))
                    .map(|(key, val)| {
                        let leaf = key;
                        let values = match val {
                            Value::Null => panic!(""),
                            Value::Bool(_) => panic!(""),
                            Value::Number(_) => panic!(""),
                            Value::String(s) => {
                                vec![Entry {
                                    base: leaf.to_string(),
                                    base_value: Some(s.to_string()),
                                    leader_value: None,
                                    leaves: HashMap::new(),
                                }]
                            }
                            Value::Array(ss) => ss
                                .iter()
                                .map(|s| match s {
                                    Value::Null => panic!(""),
                                    Value::Bool(_) => panic!(""),
                                    Value::Number(_) => panic!(""),
                                    Value::String(s) => Entry {
                                        base: leaf.to_string(),
                                        base_value: Some(s.to_string()),
                                        leader_value: None,
                                        leaves: HashMap::new(),
                                    },
                                    Value::Array(_) => panic!(""),
                                    Value::Object(_) => {
                                        let e: Entry = s.clone().try_into().unwrap();

                                        e
                                    }
                                })
                                .collect(),
                            Value::Object(_) => {
                                let e: Entry = val.clone().try_into().unwrap();
                                vec![e]
                            }
                        };
                        (leaf.clone(), values)
                    })
                    .collect();

                Entry {
                    base,
                    base_value,
                    leader_value,
                    leaves,
                }
            }
        };

        Ok(r)
    }
}

impl TryFrom<String> for Entry {
    type Error = ();

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let value_json: Value = match serde_json::from_str(&value) {
            Ok(v) => v,
            Err(_) => return Err(()),
        };

        value_json.try_into()
    }
}

impl TryFrom<&str> for Entry {
    type Error = ();

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let value_json: Value = match serde_json::from_str(value) {
            Ok(v) => v,
            Err(_) => return Err(()),
        };

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
                let leaf_value: Value = match entry.leaves.is_empty() {
                    true => entry.base_value.clone().unwrap().to_string().into(),
                    false => entry.clone().into_value(),
                };

                match value.get(leaf) {
                    None => value[&leaf] = leaf_value,
                    Some(i) => match i {
                        Value::Null => panic!(""),
                        Value::Bool(_) => panic!(""),
                        Value::Number(_) => panic!(""),
                        Value::String(s) => {
                            value[&leaf] = vec![s.to_string().into(), leaf_value].into()
                        }
                        Value::Object(o) => {
                            value[&leaf] = vec![o.clone().into(), leaf_value].into()
                        }
                        Value::Array(vs) => {
                            value[&leaf] = [vs.clone(), vec![leaf_value]].concat().into()
                        }
                    },
                }
            }
        }

        value
    }
}
