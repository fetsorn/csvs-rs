use crate::error::{Error, Result};
use crate::types::{entry::Entry, into_value::IntoValue};
use assert_json_diff::assert_json_eq;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct EntryTest {
    value: Value,
    entry: Value,
}

#[test]
fn entry_try_from_test() -> Result<()> {
    let file = fs::File::open("./src/test/cases/entry.json").expect("file should open read only");

    let tests: Vec<EntryTest> = serde_json::from_reader(file).expect("file should be proper JSON");

    for test in tests.iter() {
        let result: Entry = test.value.clone().try_into()?;

        let result_string = serde_json::to_string(&result)?;

        let result_json: Value = serde_json::from_str(&result_string)?;

        assert_json_eq!(result_json, test.entry);
    }

    Ok(())
}

#[test]
fn entry_into_test() -> Result<()> {
    let file = fs::File::open("./src/test/cases/entry.json").expect("file should open read only");

    let tests: Vec<EntryTest> = serde_json::from_reader(file).expect("file should be proper JSON");

    for test in tests.iter() {
        let entry_string = serde_json::to_string(&test.entry)?;

        let entry: Entry = serde_json::from_str(&entry_string)?;

        let result: Value = entry.into_value();

        assert_json_eq!(result, test.value);
    }

    Ok(())
}
