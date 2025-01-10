use assert_json_diff::assert_json_eq;
use clap::Parser;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::convert::From;
use std::convert::Into;
use std::convert::TryFrom;
use std::convert::TryInto;
use std::fs;

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
    base_value: Option<String>,
    leaves: HashMap<String, Vec<Entry>>,
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
                    Value::Object(_) => panic!(""),
                };

                // TODO handle if no value found for key
                let base_value: Option<String> = if v.contains_key(&base) {
                    match &v[&base] {
                    Value::Null => panic!(""),
                    Value::Bool(_) => panic!(""),
                    Value::Number(_) => panic!(""),
                    Value::String(s) => Some(s.clone()),
                    Value::Array(_) => panic!(""),
                    Value::Object(_) => panic!(""),
                    }
                } else {
                    None
                };

                let leaves: HashMap<String, Vec<Entry>> = v
                    .iter()
                    .map(|(key, val)| {
                        let leaf = key;
                        let values = match val {
                            Value::Null => panic!(""),
                            Value::Bool(_) => panic!(""),
                            Value::Number(_) => panic!(""),
                            Value::String(s) => {
                                vec![Entry {
                                    base: leaf.to_string(),
                                    base_value: Some(s.to_string()),
                                    leaves: HashMap::new(),
                                }]
                            }
                            Value::Array(ss) => ss
                                .iter()
                                .map(|s| match s {
                                    Value::Null => panic!(""),
                                    Value::Bool(_) => panic!(""),
                                    Value::Number(_) => panic!(""),
                                    Value::String(s) => Entry {
                                        base: leaf.to_string(),
                                        base_value: Some(s.to_string()),
                                        leaves: HashMap::new(),
                                    },
                                    Value::Array(_) => panic!(""),
                                    Value::Object(_) => {
                                        let e: Entry = s.clone().try_into().unwrap();

                                        e
                                    }
                                })
                                .collect(),
                            Value::Object(_) => {
                                let e: Entry = val.clone().try_into().unwrap();
                                vec![e]
                            }
                        };
                        (leaf.clone(), values)
                    })
                    .collect();

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
    leaf_value: Option<String>,
}

impl Into<Value> for Grain {
    fn into(self) -> Value {
        match self.leaf_value {
            Some(leaf_value) => json!({
            "_": self.base,
            self.base: self.base_value,
            self.leaf: leaf_value
        }),
            None => json!({
            "_": self.base,
            self.base: self.base_value
        })
        }

    }
}

fn mow(entry: Entry, trait_: &str, thing: &str) -> Vec<Grain> {
    if entry.base == thing {
        let items = &entry.leaves[trait_];

        let grains: Vec<Grain> = items
            .iter()
            .map(|item| Grain {
                base: entry.base.clone(),
                base_value: entry.base_value.clone().unwrap(),
                leaf: trait_.to_string(),
                leaf_value: Some(item.base_value.clone().unwrap()),
            })
            .collect();

        return grains;
    }

    if entry.base == trait_ {
        let items = &entry.leaves[thing];

        let grains: Vec<Grain> = items
            .iter()
            .map(|item| Grain {
                base: entry.base.clone(),
                base_value: entry.base_value.clone().unwrap(),
                leaf: thing.to_string(),
                leaf_value: Some(item.base_value.clone().unwrap()),
            })
            .collect();

        return grains;
    }

    // TODO if record has trait
    let record_has_trait = entry.leaves.keys().any(|v| v == trait_);

    if record_has_trait {
       let trunk_items: Vec<Entry> = entry.leaves[trait_].clone();

        let grains: Vec<Grain> = trunk_items.iter().fold(
            vec![],
            |with_trunk_item, trunk_item| {
                if trunk_item.leaves.contains_key(thing) {
                    let branch_items: Vec<Entry> = trunk_item.leaves[thing].clone();

                    let trunk_item_grains = branch_items.iter().map(|branch_item| {
                        Grain {
                            base: trait_.to_string(),
                            base_value: trunk_item.base_value.clone().unwrap(),
                            leaf: thing.to_string(),
                            leaf_value: Some(branch_item.base_value.clone().unwrap()),
                        }
                    }).collect();

                    vec![with_trunk_item, trunk_item_grains].concat()
                } else {
                    // TODO somewhere here return { _: trait, [trait]: trunkValue }
                    //      if branch item does not have base value
                    let grain = Grain {
                        base: trait_.to_string(),
                        base_value: trunk_item.base_value.clone().unwrap(),
                        leaf: thing.to_string(),
                        leaf_value: None,
                    };

                    vec![with_trunk_item, vec![grain]].concat()
                }
            }
        );

        return grains;
    }

    // go into objects
    entry.leaves.iter().fold(
        vec![],
        |with_entry, (leaf, leaf_items)| {
            let leaf_grains = leaf_items.iter().fold(
                vec![],
                |with_leaf_item, leaf_item| {
                    let leaf_item_grains = mow(leaf_item.clone(), trait_, thing);

                    return vec![with_leaf_item, leaf_item_grains].concat();
                }
            );

            return vec![with_entry, leaf_grains].concat();
        }
    )
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
    let file = fs::File::open("./src/test.json").expect("file should open read only");

    let test: MowTest = serde_json::from_reader(file).expect("file should be proper JSON");

    let entry: Entry = test.initial.try_into().unwrap();

    let result: Vec<Grain> = mow(entry.clone(), &test.trait_, &test.thing);

    let result_json: Vec<Value> = result.iter().map(|i| i.clone().into()).collect();

    assert_json_eq!(result_json, test.expected);
}

#[test]
fn mow_test1() {
    let file = fs::File::open("./src/test1.json").expect("file should open read only");

    let test: MowTest = serde_json::from_reader(file).expect("file should be proper JSON");

    let entry: Entry = test.initial.try_into().unwrap();

    let result: Vec<Grain> = mow(entry.clone(), &test.trait_, &test.thing);

    let result_json: Vec<Value> = result.iter().map(|i| i.clone().into()).collect();

    assert_json_eq!(result_json, test.expected);
}

#[test]
fn mow_test2() {
    let file = fs::File::open("./src/test2.json").expect("file should open read only");

    let test: MowTest = serde_json::from_reader(file).expect("file should be proper JSON");

    let entry: Entry = test.initial.try_into().unwrap();

    let result: Vec<Grain> = mow(entry.clone(), &test.trait_, &test.thing);

    let result_json: Vec<Value> = result.iter().map(|i| i.clone().into()).collect();

    println!("{:#?}", result_json);
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
