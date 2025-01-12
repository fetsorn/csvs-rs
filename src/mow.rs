use assert_json_diff::assert_json_eq;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs;
use super::grain::Grain;
use super::entry::Entry;
use crate::into_value::IntoValue;

// TODO replace trait and thing with grain
//      but not clear how to then specify base_is_thing
//      since grain's thing is leaf but can't make leaf same as base
pub fn mow(entry: Entry, trait_: &str, thing: &str) -> Vec<Grain> {
    if entry.base == thing {
        let items = &entry.leaves[trait_];

        let grains: Vec<Grain> = items
            .iter()
            .map(|item| Grain {
                base: entry.base.clone(),
                base_value: Some(entry.base_value.clone().unwrap()),
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
                base_value: Some(entry.base_value.clone().unwrap()),
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

        let grains: Vec<Grain> = trunk_items
            .iter()
            .fold(vec![], |with_trunk_item, trunk_item| {
                if trunk_item.leaves.contains_key(thing) {
                    let branch_items: Vec<Entry> = trunk_item.leaves[thing].clone();

                    let trunk_item_grains = branch_items
                        .iter()
                        .map(|branch_item| Grain {
                            base: trait_.to_string(),
                            base_value: Some(trunk_item.base_value.clone().unwrap()),
                            leaf: thing.to_string(),
                            leaf_value: Some(branch_item.base_value.clone().unwrap()),
                        })
                        .collect();

                    vec![with_trunk_item, trunk_item_grains].concat()
                } else {
                    // TODO somewhere here return { _: trait, [trait]: trunkValue }
                    //      if branch item does not have base value
                    let grain = Grain {
                        base: trait_.to_string(),
                        base_value: Some(trunk_item.base_value.clone().unwrap()),
                        leaf: thing.to_string(),
                        leaf_value: None,
                    };

                    vec![with_trunk_item, vec![grain]].concat()
                }
            });

        return grains;
    }

    // go into objects
    entry
        .leaves
        .iter()
        .fold(vec![], |with_entry, (leaf, leaf_items)| {
            let leaf_grains = leaf_items.iter().fold(vec![], |with_leaf_item, leaf_item| {
                let leaf_item_grains = mow(leaf_item.clone(), trait_, thing);

                return vec![with_leaf_item, leaf_item_grains].concat();
            });

            return vec![with_entry, leaf_grains].concat();
        })
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct MowTest {
    initial: Value,
    trait_: String,
    thing: String,
    expected: Value,
}

#[test]
fn mow_test1() {
    let file = fs::File::open("./src/test/mow1.json").expect("file should open read only");

    let test: MowTest = serde_json::from_reader(file).expect("file should be proper JSON");

    let entry: Entry = test.initial.try_into().unwrap();

    let result: Vec<Grain> = mow(entry.clone(), &test.trait_, &test.thing);

    let result_json: Vec<Value> = result.iter().map(|i| i.clone().into_value()).collect();

    assert_json_eq!(result_json, test.expected);
}

#[test]
fn mow_test2() {
    let file = fs::File::open("./src/test/mow2.json").expect("file should open read only");

    let test: MowTest = serde_json::from_reader(file).expect("file should be proper JSON");

    let entry: Entry = test.initial.try_into().unwrap();

    let result: Vec<Grain> = mow(entry.clone(), &test.trait_, &test.thing);

    let result_json: Vec<Value> = result.iter().map(|i| i.clone().into_value()).collect();

    assert_json_eq!(result_json, test.expected);
}

#[test]
fn mow_test3() {
    let file = fs::File::open("./src/test/mow3.json").expect("file should open read only");

    let test: MowTest = serde_json::from_reader(file).expect("file should be proper JSON");

    let entry: Entry = test.initial.try_into().unwrap();

    let result: Vec<Grain> = mow(entry.clone(), &test.trait_, &test.thing);

    let result_json: Vec<Value> = result.iter().map(|i| i.clone().into_value()).collect();

    assert_json_eq!(result_json, test.expected);
}
