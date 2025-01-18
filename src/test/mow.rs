use crate::{
    record::mow::mow,
    test::read_record,
    types::{entry::Entry, grain::Grain, into_value::IntoValue},
};
use assert_json_diff::assert_json_eq;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct MowTest {
    initial: String,
    trait_: String,
    thing: String,
    expected: Vec<String>,
}

#[test]
fn mow_test() {
    let file = fs::File::open("./src/test/cases/mow.json").expect("file should open read only");

    let tests: Vec<MowTest> = serde_json::from_reader(file).expect("file should be proper JSON");

    for test in tests.iter() {
        let entry: Entry = read_record(&test.initial).clone().try_into().unwrap();

        let result: Vec<Grain> = mow(entry.clone(), &test.trait_, &test.thing);

        let result_json: Vec<Value> = result.iter().map(|i| i.clone().into_value()).collect();

        let expected_json: Vec<Value> = test
            .expected
            .iter()
            .map(|grain| read_record(grain))
            .collect();

        assert_json_eq!(result_json, expected_json);
    }
}
