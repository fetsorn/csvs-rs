use assert_json_diff::assert_json_eq;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::convert::Into;
use std::convert::TryFrom;
use std::fs;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Entry {
    pub base: String,
    pub base_value: Option<String>,
    pub leaves: HashMap<String, Vec<Entry>>,
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

                // TODO handle if no value found for key
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

                let leaves: HashMap<String, Vec<Entry>> = v
                    .iter()
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
                    base: base,
                    base_value: base_value,
                    leaves: leaves,
                }
            }
        };

        Ok(r)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct ValueTest {
    initial: Value,
    expected: Value,
}

#[test]
fn entry_try_into_test1() {
    let file =
        fs::File::open("./src/test/entry_try_from1.json").expect("file should open read only");

    let test: ValueTest = serde_json::from_reader(file).expect("file should be proper JSON");

    let result: Entry = test.initial.clone().try_into().unwrap();

    let result_json: Value = serde_json::from_str(&serde_json::to_string(&result).unwrap()).unwrap();

    assert_json_eq!(result_json, test.expected);
}

impl Into<Value> for Entry {
    fn into(self) -> Value {
        let mut value: Value = json!({
            "_": self.base,
            self.base: self.base_value,
        });

        for (leaf, items) in self.leaves.iter() {
            for entry in items {
                let leaf_value: Value = entry.clone().into();

                value[&leaf] = leaf_value;
            }
        }

        value
    }
}
