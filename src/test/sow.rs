use crate::{
    error::{Error, Result},
    record::sow::sow,
    test::read_record,
    Entry, Grain, IntoValue,
};
use assert_json_diff::assert_json_eq;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct SowTest {
    initial: String,
    grain: String,
    trait_: String,
    thing: String,
    expected: String,
}

#[test]
fn sow_test() -> Result<()> {
    let file = fs::File::open("./src/test/cases/sow.json").expect("file should open read only");

    let tests: Vec<SowTest> = serde_json::from_reader(file).expect("file should be proper JSON");

    for test in tests.iter() {
        let entry: Entry = read_record(&test.initial).try_into()?;

        let grain: Grain = read_record(&test.grain).try_into()?;

        let result: Entry = sow(&entry, &grain, &test.trait_, &test.thing);

        let result_json: Value = result.into_value();

        let expected_json: Value = read_record(&test.expected);

        assert_json_eq!(result_json, expected_json);
    }

    Ok(())
}
