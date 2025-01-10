use assert_json_diff::assert_json_eq;
use std::fs;
use clap::Parser;
use std::collections::HashMap;
use serde::{Serialize, Deserialize};
use serde_json::{json, Value};
use std::convert::From;
use std::convert::Into;
use std::convert::TryFrom;
use std::convert::TryInto;

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

impl TryFrom<Value> for Entry {
    type Error = ();

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        // validate that value is object
        let r = match value {
            Value::Null => panic!(""),
            Value::Bool(_) => panic!(""),
            Value::Number(_) => panic!(""),
            Value::String(_) => panic!(""),
            Value::Array(_) => panic!(""),
            Value::Object(v) => {
                let base: String = match &v["_"] {
                    Value::Null => panic!(""),
                    Value::Bool(_) => panic!(""),
                    Value::Number(_) => panic!(""),
                    Value::String(s) => s.clone(),
                    Value::Array(_) => panic!(""),
                    Value::Object(_) => panic!("")
                };

                let base_value: String = match &v[&base] {
                    Value::Null => panic!(""),
                    Value::Bool(_) => panic!(""),
                    Value::Number(_) => panic!(""),
                    Value::String(s) => s.clone(),
                    Value::Array(_) => panic!(""),
                    Value::Object(_) => panic!("")
                };

                let leaves: HashMap<String, Vec<Entry>> = v.iter().map(|(key, val)| {
                    let leaf = key;
                    let values = match val {
                        Value::Null => panic!(""),
                        Value::Bool(_) => panic!(""),
                        Value::Number(_) => panic!(""),
                        Value::String(s) => {
                            vec![
                                Entry {
                                    base: leaf.to_string(),
                                    base_value: s.to_string(),
                                    leaves: HashMap::new(),
                                }
                            ]
                        },
                        Value::Array(ss) => {
                            ss.iter().map(|s| {
                                match s {
                                    Value::Null => panic!(""),
                                    Value::Bool(_) => panic!(""),
                                    Value::Number(_) => panic!(""),
                                    Value::String(s) => Entry {
                                        base: leaf.to_string(),
                                        base_value: s.to_string(),
                                        leaves: HashMap::new(),
                                    },
                                    Value::Array(_) => panic!(""),
                                    Value::Object(_) => {
                                        let e: Entry = s.clone().try_into().unwrap();

                                        e
                                    }
                                }
                            }).collect()
                        },
                        Value::Object(_) => {
                            let e: Entry = val.clone().try_into().unwrap();
                            vec![
                                e
                            ]
                        }
                    };
                    (leaf.clone(), values)
                }).collect();

                Entry {
                    base: base,
                    base_value: base_value,
                    leaves: leaves,
                }
            }
        };

        Ok(r)
    }
}

impl Into<Value> for Entry {
    fn into(self) -> Value {
        let mut value: Value = json!({
            "_": self.base,
            self.base: self.base_value,
        });

        for (leaf, items) in self.leaves.iter() {
            for entry in items {
                let leaf_value: Value = entry.clone().into();

                value[&leaf] = leaf_value;
            }
        }

        value
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Grain {
    base: String,
    base_value: String,
    leaf: String,
    leaf_value: String,
}

impl Into<Value> for Grain {
    fn into(self) -> Value {
        json!({
            "_": self.base,
            self.base: self.base_value,
            self.leaf: self.leaf_value
        })
    }
}


fn mow(entry: Entry, trait_: &str, thing: &str) -> Vec<Grain> {
    println!("{}{}", trait_, thing);

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

#[derive(Debug, Serialize, Deserialize, Clone)]
struct MowTest {
    initial: Value,
    trait_: String,
    thing: String,
    expected: Value,
}

#[test]
fn mow_test() {
    let file = fs::File::open("./src/test.json")
        .expect("file should open read only");

    let test: MowTest = serde_json::from_reader(file)
        .expect("file should be proper JSON");

    let entry: Entry = test.initial.try_into().unwrap();

    let result = mow(
        entry.clone(),
        &test.trait_,
        &test.thing,
    )[0].clone();

    let result_json: Value = result.into();

    assert_json_eq!(result_json, test.expected);
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
