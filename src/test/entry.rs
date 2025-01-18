use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct EntryTest {
    value: Value,
    entry: Value,
}

#[test]
fn entry_try_from_test() {
    let file = fs::File::open("./src/test/cases/entry.json").expect("file should open read only");

    let tests: Vec<EntryTest> = serde_json::from_reader(file).expect("file should be proper JSON");

    for test in tests.iter() {
        let result: Entry = test.value.clone().try_into().unwrap();

        let result_string = serde_json::to_string(&result).unwrap();

        let result_json: Value = serde_json::from_str(&result_string).unwrap();

        assert_json_eq!(result_json, test.entry);
    }
}

#[test]
fn entry_into_test() {
    let file = fs::File::open("./src/test/cases/entry.json").expect("file should open read only");

    let tests: Vec<EntryTest> = serde_json::from_reader(file).expect("file should be proper JSON");

    for test in tests.iter() {
        let entry_string = serde_json::to_string(&test.entry).unwrap();

        let entry: Entry = serde_json::from_str(&entry_string).unwrap();

        let result: Value = entry.clone().into_value();

        assert_json_eq!(result, test.value);
    }
}
