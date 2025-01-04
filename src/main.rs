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

fn sow(entry: Value, grain: Value, trait_: &str, thing: &str) -> Value {
    // let base = entry;

    entry.clone()
    // if base equals thing
    //   append grain.thing to record.thing
    // if base equals trait
    //   append grain.thing to record.thing
    // if record has trait
    //   for each item of record.trait
    //     if item.trait equals grain.trait
    //       append grain.thing to item.thing
    // otherwise
    //   for each field of record
    //     for each item of record.field
    //       sow grain to item
}

#[test]
fn sow_test() {
    let entry = serde_json::from_str(r#"
        {
            "_": "datum",
            "datum": "value1",
            "filepath": {
                "_": "filepath",
                "filepath": "path/to/1",
                "moddate": "2001-01-01"
            },
            "saydate": "2001-01-01",
            "sayname": "name1",
            "actdate": "2001-01-01",
            "actname": "name1"
        }
    "#).unwrap();

    let grain = serde_json::from_str(r#"
        {
            "_": "datum",
            "datum": "value1",
            "saydate": "2001-01-01"
        }
    "#).unwrap();

    let result = sow(entry, grain, "datum", "saydate");

    assert_json!(result, {
        "_": "datum",
        "datum": "value1",
        "filepath": {
            "_": "filepath",
            "filepath": "path/to/1",
            "moddate": "2001-01-01"
        },
        "saydate": [ "2001-01-01", "2001-01-01" ],
        "sayname": "name1",
        "actdate": "2001-01-01",
        "actname": "name1",
    });
}
