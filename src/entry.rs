use assert_json_diff::assert_json_eq;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::convert::TryInto;
use std::convert::TryFrom;
use std::fs;
use super::into_value::IntoValue;

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
                    .filter(|(key, _)| (*key != "_") && (**key != base))
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
struct EntryTest {
    value: Value,
    entry: Value,
}

#[test]
fn entry_try_from_test() {
    let file =
        fs::File::open("./src/test/entry.json").expect("file should open read only");

    let tests: Vec<EntryTest> = serde_json::from_reader(file).expect("file should be proper JSON");

    for test in tests.iter() {
        let result: Entry = test.value.clone().try_into().unwrap();

        let result_string = serde_json::to_string(&result).unwrap();

        let result_json: Value = serde_json::from_str(&result_string).unwrap();

        assert_json_eq!(result_json, test.entry);
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

        for (leaf, items) in self.leaves.iter() {
            for entry in items {
                let leaf_value: Value = match entry.leaves.is_empty() {
                    true => entry.base_value.clone().unwrap().to_string().into(),
                    false => entry.clone().into_value()
                };

                match value.get(&leaf) {
                    None => value[&leaf] = leaf_value,
                    Some(i) => match i {
                        Value::Null => panic!(""),
                        Value::Bool(_) => panic!(""),
                        Value::Number(_) => panic!(""),
                        Value::String(s) => value[&leaf] = vec![s.to_string().into(), leaf_value].into(),
                        Value::Object(o) => value[&leaf] = vec![o.clone().into(), leaf_value].into(),
                        Value::Array(vs) => value[&leaf] = vec![vs.clone(), vec![leaf_value]].concat().into(),
                    }
                }
            }
        }

        value
    }
}

#[test]
fn entry_into_test() {
    let file =
        fs::File::open("./src/test/entry.json").expect("file should open read only");

    let tests: Vec<EntryTest> = serde_json::from_reader(file).expect("file should be proper JSON");

    for test in tests.iter() {
        let entry_string = serde_json::to_string(&test.entry).unwrap();

        let entry: Entry = serde_json::from_str(&entry_string).unwrap();

        let result: Value = entry.clone().into_value();

        assert_json_eq!(result, test.value);
    }
}
