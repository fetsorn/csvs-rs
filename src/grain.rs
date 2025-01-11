use assert_json_diff::assert_json_eq;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::convert::Into;
use std::convert::TryFrom;
use std::fs;
use super::into_value::IntoValue;

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

                let leaves: HashMap<String, Value> = v
                    .iter()
                    .filter(|&(key, _)| (*key != "_") && (**key != base))
                    .map(|(key, val)| (key.clone(), val.clone()))
                    .collect();

                if leaves.len() == 0 {return Ok(Grain {
                    base: base.to_owned(),
                    base_value: base_value.to_owned(),
                    leaf: "".to_string(),
                    leaf_value: None
                })}

                if leaves.len() > 1 {panic!()};

                let keys: Vec<String> = leaves.keys().cloned().collect();

                let leaf: String = keys[0].clone();

                let leaf_value: Option<String> = match &v[&leaf] {
                    Value::Null => panic!(""),
                    Value::Bool(_) => panic!(""),
                    Value::Number(_) => panic!(""),
                    Value::String(s) => Some(s.clone()),
                    Value::Array(_) => panic!(""),
                    Value::Object(_) => panic!(""),
                };

                Ok(Grain {
                    base: base.to_owned(),
                    base_value: base_value.to_owned(),
                    leaf: leaf,
                    leaf_value: leaf_value
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

#[derive(Debug, Serialize, Deserialize, Clone)]
struct GrainTest {
    value: Value,
    grain: Value,
}

#[test]
fn grain_try_from_test() {
    let file =
        fs::File::open("./src/test/grain.json").expect("file should open read only");

    let tests: Vec<GrainTest> = serde_json::from_reader(file).expect("file should be proper JSON");

    for test in tests.iter() {
        let result: Grain = test.value.clone().try_into().unwrap();

        let result_string = serde_json::to_string(&result).unwrap();

        let result_json: Value = serde_json::from_str(&result_string).unwrap();

        assert_json_eq!(result_json, test.grain);
    }
}

#[test]
fn grain_into_test() {
    let file =
        fs::File::open("./src/test/grain.json").expect("file should open read only");

    let tests: Vec<GrainTest> = serde_json::from_reader(file).expect("file should be proper JSON");

    for test in tests.iter() {
        let grain_string = serde_json::to_string(&test.grain).unwrap();

        let grain: Grain = serde_json::from_str(&grain_string).unwrap();

        let result: Value = grain.clone().into_value();

        assert_json_eq!(result, test.value);
    }
}
