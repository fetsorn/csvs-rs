use assert_json::assert_json;
use clap::Parser;
use serde_json::{json, Result, Value};

#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    #[arg(short = 'i', long)]
    source_path: Option<String>,

    #[arg(short = 'o', long)]
    target_path: Option<String>,

    #[arg(short = 't', long, default_value = "json")]
    target_type: String,

    #[arg(short, long)]
    query: String,
}

fn main() {
    let args = Args::parse();

    println!("Hello {}!", args.query);
}

#[derive(Debug)]
struct Tablet {
    thing_branch: String,
    trait_branch: String,
    thing_is_first: bool,
    trait_is_first: bool,
    filename: String,
    trait_is_regex: bool,
    querying: bool,
    eager: bool,
    accumulating: bool,
    passthrough: bool,
}

#[derive(Debug)]
struct Step {
    is_match: bool,
    record: Value,
}

fn step(tablet: Tablet, record: Value, trait_value: &str, thing_value: &str) -> Step {
    let mut record = match record {
        Value::Object(m) => {
            let mut m = m.clone();

            let existing_leaf = m.get(trait_value);

            let leaves = match existing_leaf {
                None => {
                    let mut value_new: Value = Value::String(thing_value.to_string());
                    value_new
                }
                Some(Value::Array(vs)) => {
                    let mut value_new: Value = Value::String(thing_value.to_string());
                    let mut vs_new: Vec<Value> = vs.clone();
                    vs_new.extend(vec![value_new]);
                    Value::Array(vs_new)
                }
                Some(v) => {
                    let mut value_new: Value = Value::String(thing_value.to_string());
                    let mut vs_new: Value = Value::Array(vec![v.clone(), value_new]);
                    vs_new
                }
            };

            m.insert("event".to_string(), leaves);
            Value::Object(m)
        }
        v => v.clone(),
    };

    Step {
        record,
        is_match: true,
    }
}

#[test]
fn step_schema_test() {
    let tablet = Tablet {
        thing_branch: "_".to_string(),
        trait_branch: "_".to_string(),
        thing_is_first: false,
        trait_is_first: true,
        filename: "_-_.csv".to_string(),
        trait_is_regex: false,
        querying: false,
        eager: false,
        accumulating: false,
        passthrough: false,
    };

    let record = json!({ "_": "_" });

    let result = step(tablet, record, "event", "actname");

    assert_json!(result.record, { "_": "_", "event": "actname" });
}
