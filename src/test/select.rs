use assert_json_diff::assert_json_eq;
use serde_json::Value;
extern crate dir_diff;
use crate::{
    error::{Error, Result},
    select::select_record,
    test::read_record,
    types::{entry::Entry, into_value::IntoValue},
};
use serde::{Deserialize, Serialize};
use std::fs;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct SelectTest {
    initial: String,
    query: Vec<Value>,
    expected: Vec<String>,
}

#[tokio::test]
async fn select_test() -> Result<()> {
    let file = fs::File::open("./src/test/cases/select.json").expect("file should open read only");

    let tests: Vec<SelectTest> = serde_json::from_reader(file).expect("file should be proper JSON");

    for test in tests.iter() {
        let initial_path = format!("./src/test/datasets/{}", test.initial);

        let initial_path = std::path::Path::new(&initial_path);

        // parse query to Entry
        let queries: Vec<Entry> = test.query.iter().map(|query| query.clone().try_into()).collect::<Result<Vec<Entry>>>()?;

        println!("ask: {:#?}", queries.clone().into_iter().map(|query| query.into_value()));

        let entries = select_record(initial_path.to_owned(), queries).await?;

        let entries_json: Vec<Value> = entries.iter().map(|i| i.clone().into_value()).collect();

        let expected_json: Vec<Value> = test
            .expected
            .iter()
            .map(|grain| read_record(grain))
            .collect();

        println!("want: {:#?}", expected_json);
        println!("got: {:#?}", entries_json);

        assert_json_eq!(entries_json, expected_json);
    }

    Ok(())
}
