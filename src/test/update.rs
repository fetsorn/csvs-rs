extern crate dir_diff;
use crate::{
    error::{Error, Result},
    test::read_record,
    types::{entry::Entry, grain::Grain},
    update::update_record,
};
use serde::{Deserialize, Serialize};
use std::fs;
use temp_dir::TempDir;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct UpdateTest {
    initial: String,
    query: Vec<String>,
    expected: String,
}

#[tokio::test]
async fn update_test() -> Result<()> {
    let file = fs::File::open("./src/test/cases/update.json").expect("file should open read only");

    let tests: Vec<UpdateTest> = serde_json::from_reader(file).expect("file should be proper JSON");

    for test in tests.iter() {
        let temp_path = TempDir::new()?;

        let initial_path = format!("./src/test/datasets/{}", test.initial);

        for file_entry in fs::read_dir(&initial_path)? {
            let file_entry = file_entry?;

            let file_type = file_entry.file_type()?;

            if file_type.is_dir() {
            } else {
                fs::copy(
                    file_entry.path(),
                    temp_path.as_ref().join(file_entry.file_name()),
                )?;
            }
        }

        let expected_str = format!("./src/test/datasets/{}", test.expected);

        let expected_path = std::path::Path::new(&expected_str);

        // parse query to Entry
        let queries: Vec<Entry> = test
            .query
            .iter()
            .map(|query| read_record(&query).try_into())
            .collect::<Result<Vec<Entry>>>()?;

        update_record(temp_path.path().to_owned(), queries).await;

        if dir_diff::is_different(temp_path.path(), expected_path)? {
            for file_entry in fs::read_dir(temp_path.path())? {
                let file_entry = file_entry?;

                let file_type = file_entry.file_type()?;

                if file_type.is_dir() {
                } else {
                    let received = fs::read_to_string(file_entry.path())?;

                    let expected = fs::read_to_string(expected_path.join(file_entry.file_name()))?;

                    assert_eq!(received, expected);
                }
            }
        }

        assert!(!dir_diff::is_different(temp_path.path(), expected_path)?);
    }

    Ok(())
}
