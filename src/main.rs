use assert_json::assert_json;
use clap::Parser;
use std::collections::HashMap;
use serde::{Serialize, Deserialize};
use serde_json::{Value};

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

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Entry {
    base: String,
    base_value: String,
    leaves: HashMap<String, Vec<Entry>>
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Grain {
    base: String,
    base_value: String,
    leaf: String,
    leaf_value: String,
}

fn mow(entry: Entry, trait_: &str, thing: &str) -> Vec<Grain> {
    if entry.base == thing {
        let items = &entry.leaves[trait_];

        let grains: Vec<Grain> = items.iter().map(|item|
            Grain {
                base: entry.base.clone(),
                base_value: entry.base_value.clone(),
                leaf: trait_.to_string(),
                leaf_value: item.base_value.clone(),
            }
        ).collect();

        return grains
    }

    if entry.base == trait_ {
        let items = &entry.leaves[thing];

        let grains: Vec<Grain> = items.iter().map(|item|
            Grain {
                base: entry.base.clone(),
                base_value: entry.base_value.clone(),
                leaf: thing.to_string(),
                leaf_value: item.base_value.clone(),
            }
        ).collect();

        return grains
    }

    vec![]
}

#[test]
fn mow_test() {
    // TODO convert Value to Entry
    //let entry_json: Value = serde_json::from_str(r#"
    //    {
    //        "_": "datum",
    //        "datum": "value1",
    //        "filepath": {
    //            "_": "filepath",
    //            "filepath": "path/to/1",
    //            "moddate": "2001-01-01"
    //        },
    //        "saydate": "2001-01-01",
    //        "sayname": "name1",
    //        "actdate": "2001-01-01",
    //        "actname": "name1"
    //    }
    //"#).unwrap();

    let entry = Entry {
        base: "datum".to_string(),
        base_value: "value1".to_string(),
        leaves: HashMap::from([
            ("filepath".to_string(), vec![Entry {
                base: "filepath".to_string(),
                base_value: "path/to/1".to_string(),
                leaves: HashMap::from([
                    ("moddate".to_string(), vec![Entry {
                        base: "moddate".to_string(),
                        base_value: "2001-01-01".to_string(),
                        leaves: HashMap::new(),
                    }])
                ])
            }]),
            ("saydate".to_string(), vec![Entry {
                base: "saydate".to_string(),
                base_value: "2001-01-01".to_string(),
                leaves: HashMap::new(),
            }]),
            ("sayname".to_string(), vec![Entry {
                base: "sayname".to_string(),
                base_value: "name1".to_string(),
                leaves: HashMap::new(),
            }]),
            ("actdate".to_string(), vec![Entry {
                base: "actdate".to_string(),
                base_value: "2001-01-01".to_string(),
                leaves: HashMap::new(),
            }]),
            ("actname".to_string(), vec![Entry {
                base: "sayname".to_string(),
                base_value: "name1".to_string(),
                leaves: HashMap::new(),
            }]),
        ]),
    };

    let result = mow(entry.clone(), "datum", "actdate")[0].clone();

    let result_str = serde_json::to_string(&result).unwrap();

    let result_json: Value = serde_json::from_str(&result_str).unwrap();

    // TODO turn result grains to Value here

    assert_json!(result_json, {
            "_": "datum",
            "datum": "value1",
            "actdate": "2001-01-01"
        });
}

//fn sow(entry: Value, grain: Value, trait_: &str, thing: &str) -> Value {
//    // let base = entry;
//
//    entry.clone()
//    // if base equals thing
//    //   append grain.thing to record.thing
//    // if base equals trait
//    //   append grain.thing to record.thing
//    // if record has trait
//    //   for each item of record.trait
//    //     if item.trait equals grain.trait
//    //       append grain.thing to item.thing
//    // otherwise
//    //   for each field of record
//    //     for each item of record.field
//    //       sow grain to item
//}

//#[test]
//fn sow_test() {
//    let entry = serde_json::from_str(r#"
//        {
//            "_": "datum",
//            "datum": "value1",
//            "filepath": {
//                "_": "filepath",
//                "filepath": "path/to/1",
//                "moddate": "2001-01-01"
//            },
//            "saydate": "2001-01-01",
//            "sayname": "name1",
//            "actdate": "2001-01-01",
//            "actname": "name1"
//        }
//    "#).unwrap();
//
//    let grain = serde_json::from_str(r#"
//        {
//            "_": "datum",
//            "datum": "value1",
//            "saydate": "2001-01-01"
//        }
//    "#).unwrap();
//
//    let result = sow(entry, grain, "datum", "saydate");
//
//    assert_json!(result, {
//        "_": "datum",
//        "datum": "value1",
//        "filepath": {
//            "_": "filepath",
//            "filepath": "path/to/1",
//            "moddate": "2001-01-01"
//        },
//        "saydate": [ "2001-01-01", "2001-01-01" ],
//        "sayname": "name1",
//        "actdate": "2001-01-01",
//        "actname": "name1",
//    });
//}
