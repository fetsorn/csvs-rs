mod entry;
mod grain;
mod insert;
mod delete;
mod mow;
mod schema;
mod sow;
use serde_json::Value;
use std::fs;

pub fn read_record(path: &str) -> Value {
    let entry_path = format!("./src/test/records/{}.json", path);

    let entry_file = fs::File::open(entry_path).expect("file should open read only");

    let entry_json: Value =
        serde_json::from_reader(entry_file).expect("file should be proper JSON");

    entry_json
}
