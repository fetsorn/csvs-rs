use super::read_record;
use crate::types::grain::Grain;
use crate::types::into_value::IntoValue;
use assert_json_diff::assert_json_eq;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::convert::Into;
use std::convert::TryFrom;
use std::fs;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct GrainTest {
    value: Value,
    grain: Value,
}

#[test]
fn grain_try_from_test() {
    let file = fs::File::open("./src/test/cases/grain.json").expect("file should open read only");

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
    let file = fs::File::open("./src/test/cases/grain.json").expect("file should open read only");

    let tests: Vec<GrainTest> = serde_json::from_reader(file).expect("file should be proper JSON");

    for test in tests.iter() {
        let grain_string = serde_json::to_string(&test.grain).unwrap();

        let grain: Grain = serde_json::from_str(&grain_string).unwrap();

        let result: Value = grain.clone().into_value();

        assert_json_eq!(result, test.value);
    }
}
