use crate::{
    error::{Error, Result},
    schema::find_crown,
    Branch, Entry, Leaves, Schema, Trunks,
};
use assert_json_diff::assert_json_eq;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::fs;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct SchemaTest {
    entry: Value,
    schema: Value,
}

#[test]
fn entry_into_test() -> Result<()> {
    let file = fs::File::open("./src/test/cases/schema.json").expect("file should open read only");

    let tests: Vec<SchemaTest> = serde_json::from_reader(file).expect("file should be proper JSON");

    for test in tests.iter() {
        let entry_string = serde_json::to_string(&test.entry)?;

        let entry: Entry = serde_json::from_str(&entry_string)?;

        let result: Schema = entry.try_into()?;

        let result_string: String = serde_json::to_string(&result)?;

        assert_json_eq!(result, test.schema);
    }

    Ok(())
}

#[test]
fn find_crown_test() {
    let schema = Schema(HashMap::from([
        (
            "datum".to_owned(),
            Branch {
                trunks: Trunks(vec![]),
                leaves: Leaves(vec!["date".to_owned(), "name".to_owned()]),
            },
        ),
        (
            "date".to_owned(),
            Branch {
                trunks: Trunks(vec!["datum".to_owned()]),
                leaves: Leaves(vec![]),
            },
        ),
        (
            "name".to_owned(),
            Branch {
                trunks: Trunks(vec!["datum".to_owned()]),
                leaves: Leaves(vec![]),
            },
        ),
    ]));

    let mut crown = find_crown(&schema, "datum");

    crown.sort();

    assert_eq!(crown, vec!["date", "datum", "name"]);
}
