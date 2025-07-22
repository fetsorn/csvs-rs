use super::read_record;
use assert_json_diff::assert_json_eq;
use csvs::{Entry, Result, Schema};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct LevelTest {
    schema: String,
    initial: String,
    expected: i32,
}

#[test]
fn level_test() -> Result<()> {
    let file = fs::File::open("./src/test/cases/get_nesting_level.json")
        .expect("file should open read only");

    let tests: Vec<LevelTest> = serde_json::from_reader(file).expect("file should be proper JSON");

    for test in tests.iter() {
        let schema_record = read_record(&test.schema);

        let schema: Schema = schema_record.try_into()?;

        let level = schema.get_nesting_level(&test.initial);

        assert_eq!(level, test.expected);
    }

    Ok(())
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct SortTest {
    schema: String,
    initial: Vec<String>,
    expected: Vec<String>,
}

#[test]
fn sort_descending_test() -> Result<()> {
    let file = fs::File::open("./src/test/cases/sort_descending.json")
        .expect("file should open read only");

    let tests: Vec<SortTest> = serde_json::from_reader(file).expect("file should be proper JSON");

    for test in tests.iter() {
        let schema_record = read_record(&test.schema);

        let schema: Schema = schema_record.try_into()?;

        let mut sorted = test.initial.clone();

        sorted.sort_by(schema.sort_nesting_descending());

        assert_eq!(sorted, test.expected);
    }

    Ok(())
}

#[test]
fn sort_ascending_test() -> Result<()> {
    let file =
        fs::File::open("./src/test/cases/sort_ascending.json").expect("file should open read only");

    let tests: Vec<SortTest> = serde_json::from_reader(file).expect("file should be proper JSON");

    for test in tests.iter() {
        let schema_record = read_record(&test.schema);

        let schema: Schema = schema_record.try_into()?;

        let mut sorted = test.initial.clone();

        sorted.sort_by(schema.sort_nesting_ascending());

        assert_eq!(sorted, test.expected);
    }

    Ok(())
}
