use assert_json_diff::assert_json_eq;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::fs;
use super::grain::Grain;
use super::entry::Entry;

pub fn sow(entry: Entry, grain: Grain, trait_: &str, thing: &str) -> Entry {
    // let base = entry;

    // if base equals thing
    if entry.base == thing {
        return match grain.base_value {
            None => entry,
            Some(grain_base_value) => match entry.base_value {
                Some(_) => panic!(),
                None => Entry {
                    base: thing.to_string(),
                    base_value: Some(grain_base_value),
                    leaves: entry.leaves.clone(),
                },
            },
        }
    }

    //   append grain.thing to record.thing
    // if base equals trait
    if entry.base == trait_ {
        if entry.leaves.contains_key(thing) {
            let mut leaves = entry.leaves.clone();

            let thing_item = Entry {
                base: thing.to_string(),
                base_value: Some(grain.leaf_value.unwrap().to_string()),
                leaves: HashMap::new(),
            };

            leaves.insert(thing.to_string(), vec![leaves[thing].clone(), vec![thing_item]].concat());

            return Entry {
                base: entry.base.to_string(),
                base_value: Some(entry.base_value.unwrap().to_string()),
                leaves: leaves,
            };
        } else {
            return entry;
        }
    }

    //   append grain.thing to record.thing
    // if record has trait
    // let record_has_trait = entry.leaves.keys().any(|v| v == trait_);

    // if record_has_trait {}
    //   for each item of record.trait
    //     if item.trait equals grain.trait
    //       append grain.thing to item.thing
    // otherwise
    //   for each field of record
    //     for each item of record.field
    //       sow grain to item
    // go into objects
    //entry
    //    .leaves
    //    .iter()
    //    .fold(vec![], |with_entry, (leaf, leaf_items)| {
    //        let leaf_grains = leaf_items.iter().fold(vec![], |with_leaf_item, leaf_item| {
    //            let leaf_item_grains = mow(leaf_item.clone(), trait_, thing);

    //            return vec![with_leaf_item, leaf_item_grains].concat();
    //        });

    //        return vec![with_entry, leaf_grains].concat();
    //    })
    entry
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct SowTest {
    initial: Value,
    grain: Value,
    trait_: String,
    thing: String,
    expected: Value,
}

#[test]
fn sow_test1() {
    let file = fs::File::open("./src/test/sow1.json").expect("file should open read only");

    let test: SowTest = serde_json::from_reader(file).expect("file should be proper JSON");

    let entry: Entry = test.initial.try_into().unwrap();

    let grain: Grain = test.grain.clone().try_into().unwrap();

    let result: Entry = sow(entry.clone(), grain.clone(), &test.trait_, &test.thing);

    let result_json: Value = result.into();

    assert_json_eq!(result_json, test.expected);
}
