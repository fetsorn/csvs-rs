use crate::entry::Entry;
use assert_json_diff::assert_json_eq;
use serde_json::Value;
use super::read_record;
use crate::schema::{Schema, Trunks, Leaves};
use crate::insert::insert_record;
use std::collections::HashMap;
use std::fs;
use serde::{Deserialize, Serialize};
use async_stream::stream;
use futures_core::stream::Stream;
use futures_util::pin_mut;
use futures_util::stream::StreamExt;


#[derive(Debug, Serialize, Deserialize, Clone)]
struct InsertTest {
    path: String,
    query: Value,
    expected: Value,
}

#[tokio::test]
async fn insert_test() {
    let file = fs::File::open("./src/test/cases/insert.json").expect("file should open read only");

    let tests: Vec<InsertTest> = serde_json::from_reader(file).expect("file should be proper JSON");

    for test in tests.iter() {
        // TODO setup temporary path
        let temp_path = test.path.clone();

        // parse query to Entry
        let query: Entry = test.query.clone().try_into().unwrap();

        let result = insert_record(&temp_path, query).await;

        assert_json_eq!(result, test.expected);
    }
}
