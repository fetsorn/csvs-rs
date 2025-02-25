use crate::{
    schema::{get_nesting_level, sort_nesting_ascending, sort_nesting_descending},
    test::read_record,
    types::{entry::Entry, schema::Schema},
};
use assert_json_diff::assert_json_eq;
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
fn level_test() {
    let file = fs::File::open("./src/test/cases/get_nesting_level.json")
        .expect("file should open read only");

    let tests: Vec<LevelTest> = serde_json::from_reader(file).expect("file should be proper JSON");

    for test in tests.iter() {
        let schema_record = read_record(&test.schema);

        let schema = schema_record.try_into().unwrap();

        let level = get_nesting_level(&schema, &test.initial);

        assert_eq!(level, test.expected);
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct SortTest {
    schema: String,
    initial: Vec<String>,
    expected: Vec<String>,
}

#[test]
fn sort_descending_test() {
    let file = fs::File::open("./src/test/cases/sort_descending.json")
        .expect("file should open read only");

    let tests: Vec<SortTest> = serde_json::from_reader(file).expect("file should be proper JSON");

    for test in tests.iter() {
        let schema_record = read_record(&test.schema);

        let schema = schema_record.try_into().unwrap();

        let mut sorted = test.initial.clone();

        sorted.sort_by(sort_nesting_descending(schema));

        assert_eq!(sorted, test.expected);
    }
}

#[test]
fn sort_ascending_test() {
    let file =
        fs::File::open("./src/test/cases/sort_ascending.json").expect("file should open read only");

    let tests: Vec<SortTest> = serde_json::from_reader(file).expect("file should be proper JSON");

    for test in tests.iter() {
        let schema_record = read_record(&test.schema);

        let schema = schema_record.try_into().unwrap();

        let mut sorted = test.initial.clone();

        sorted.sort_by(sort_nesting_ascending(schema));

        assert_eq!(sorted, test.expected);
    }
}
