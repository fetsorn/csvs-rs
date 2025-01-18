use super::read_record;
use crate::types::entry::Entry;
use crate::types::into_value::IntoValue;
use assert_json_diff::assert_json_eq;
use serde_json::Value;
use temp_dir::TempDir;
extern crate dir_diff;
use crate::schema::{Leaves, Schema, Trunks};
use crate::select::select_record;
use async_stream::stream;
use futures_core::stream::Stream;
use futures_util::pin_mut;
use futures_util::stream::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::fs;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct SelectTest {
    initial: String,
    query: Value,
    expected: Vec<String>,
}

#[tokio::test]
async fn select_test() {
    let file = fs::File::open("./src/test/cases/select.json").expect("file should open read only");

    let tests: Vec<SelectTest> = serde_json::from_reader(file).expect("file should be proper JSON");

    for test in tests.iter() {
        let initial_path = format!("./src/test/datasets/{}", test.initial);

        let initial_path = std::path::Path::new(&initial_path);

        // parse query to Entry
        let query: Entry = test.query.clone().try_into().unwrap();

        let entries = select_record(initial_path.to_owned(), vec![query.clone()]).await;

        let entries_json: Vec<Value> = entries.iter().map(|i| i.clone().into_value()).collect();

        let expected_json: Vec<Value> = test
            .expected
            .iter()
            .map(|grain| read_record(grain))
            .collect();

        println!("ask: {:?}", query.into_value());
        println!("want: {:?}", expected_json);
        println!("got: {:?}", entries_json);

        assert_json_eq!(entries_json, expected_json);
    }
}
