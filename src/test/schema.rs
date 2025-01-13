use crate::entry::Entry;
use crate::schema::{Schema, Leaves, Trunks, find_crown};
use assert_json_diff::assert_json_eq;
use super::read_record;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::convert::Into;
use std::convert::TryFrom;
use std::fs;


#[derive(Debug, Serialize, Deserialize, Clone)]
struct SchemaTest {
    entry: Value,
    schema: Value,
}

#[test]
fn entry_into_test() {
    let file = fs::File::open("./src/test/cases/schema.json").expect("file should open read only");

    let tests: Vec<SchemaTest> = serde_json::from_reader(file).expect("file should be proper JSON");

    for test in tests.iter() {
        let entry_string = serde_json::to_string(&test.entry).unwrap();

        let entry: Entry = serde_json::from_str(&entry_string).unwrap();

        let result: Schema = entry.clone().try_into().unwrap();

        let result_string: String = serde_json::to_string(&result).unwrap();

        assert_json_eq!(result, test.schema);
    }
}

#[test]
fn find_crown_test() {
    let schema = Schema(HashMap::from([
        (
            "datum".to_string(),
            (
                Trunks(vec![]),
                Leaves(vec!["date".to_string(), "name".to_string()]),
            ),
        ),
        (
            "date".to_string(),
            (Trunks(vec!["datum".to_string()]), Leaves(vec![])),
        ),
        (
            "name".to_string(),
            (Trunks(vec!["datum".to_string()]), Leaves(vec![])),
        ),
    ]));

    let crown = find_crown(&schema, "datum");

    assert_eq!(
        crown.clone().sort(),
        vec!["datum", "name", "date"].clone().sort()
    );
}
