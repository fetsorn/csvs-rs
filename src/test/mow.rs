use crate::{
    error::{Error, Result},
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
fn mow_test() -> Result<()> {
    let file = fs::File::open("./src/test/cases/mow.json").expect("file should open read only");

    let tests: Vec<MowTest> = serde_json::from_reader(file).expect("file should be proper JSON");

    for test in tests.iter() {
        let entry: Entry = read_record(&test.initial).try_into()?;

        let result: Vec<Grain> = mow(&entry, &test.trait_, &test.thing);

        let result_json: Vec<Value> = result.into_iter().map(|i| i.into_value()).collect();

        let expected_json: Vec<Value> = test
            .expected
            .iter()
            .map(|grain| read_record(grain))
            .collect();

        assert_json_eq!(result_json, expected_json);
    }

    Ok(())
}
