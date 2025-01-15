use super::read_record;
use crate::types::entry::Entry;
use assert_json_diff::assert_json_eq;
use serde_json::Value;
use temp_dir::TempDir;
extern crate dir_diff;
use crate::insert::insert_record;
use crate::schema::{Leaves, Schema, Trunks};
use async_stream::stream;
use futures_core::stream::Stream;
use futures_util::pin_mut;
use futures_util::stream::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::fs;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct InsertTest {
    initial: String,
    query: String,
    expected: String,
}

#[tokio::test]
async fn insert_test() {
    let file = fs::File::open("./src/test/cases/insert.json").expect("file should open read only");

    let tests: Vec<InsertTest> = serde_json::from_reader(file).expect("file should be proper JSON");

    for test in tests.iter() {
        let temp_path = TempDir::new().unwrap();

        let initial_path = format!("./src/test/datasets/{}", test.initial);

        for entry in fs::read_dir(initial_path.clone()).unwrap() {
            let entry = entry.unwrap();

            let filetype = entry.file_type().unwrap();

            if filetype.is_dir() {
            } else {
                fs::copy(entry.path(), temp_path.as_ref().join(entry.file_name())).unwrap();
            }
        }

        let expected_str = format!("./src/test/datasets/{}", test.expected);

        let expected_path = std::path::Path::new(&expected_str);

        // parse query to Entry
        let query: Entry = read_record(&test.query).clone().try_into().unwrap();

        insert_record(temp_path.path().to_owned(), vec![query]).await;

        if dir_diff::is_different(temp_path.path(), expected_path).unwrap() {
            for entry in fs::read_dir(temp_path.path()).unwrap() {
                let entry = entry.unwrap();

                let filetype = entry.file_type().unwrap();

                if filetype.is_dir() {
                } else {
                    let received = fs::read_to_string(entry.path()).unwrap();

                    let expected =
                        fs::read_to_string(expected_path.join(entry.file_name())).unwrap();

                    assert_eq!(received, expected);
                }
            }
        }

        assert!(!dir_diff::is_different(temp_path.path(), expected_path).unwrap());
    }
}
