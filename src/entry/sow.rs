use super::Entry;
use crate::Grain;
use std::collections::HashMap;

// TODO remove trait, thing and use grain.base, grain.leaf
pub fn sow(entry: &Entry, grain: &Grain, trait_: &str, thing: &str) -> Entry {
    // if trait_ == "datum" && thing == "filepath" {println!("{} {} {} {}", serde_json::to_string_pretty(&entry).expect(""), serde_json::to_string_pretty(&grain).expect(""), trait_, thing)};

    // let base = entry;

    // if base equals thing
    if entry.base == thing {
        // TODO here match
        return match &grain.leaf_value {
            None => entry.clone(),
            Some(grain_leaf_value) => match &entry.base_value {
                None => Entry {
                    base: thing.to_owned(),
                    base_value: Some(grain_leaf_value.to_owned()),
                    leader_value: None,
                    leaves: entry.leaves.clone(),
                },
                Some(e) => Entry {
                    base: thing.to_owned(),
                    base_value: Some(grain_leaf_value.to_owned()),
                    leader_value: None,
                    leaves: entry.leaves.clone(),
                },
            },
        };
    }

    //   append grain.thing to record.thing
    // if base equals trait
    if entry.base == trait_ {
        let thing_item = Entry {
            base: thing.to_owned(),
            base_value: grain.leaf_value.clone(),
            leader_value: None,
            leaves: HashMap::new(),
        };

        let mut leaves = entry.leaves.clone();

        let items_new = if entry.leaves.contains_key(thing) {
            [&leaves[thing][..], &[thing_item]].concat()
        } else {
            vec![thing_item]
        };

        leaves.insert(thing.to_owned(), items_new);

        return Entry {
            base: entry.base.to_owned(),
            base_value: entry.base_value.clone(),
            leader_value: None,
            leaves,
        };
    }

    let record_has_trait = entry.leaves.keys().any(|v| v == trait_);

    if record_has_trait {
        let trunk_items = match entry.leaves.get(trait_) {
            None => vec![],
            Some(vs) => vs.to_owned(),
        };

        let trait_items: Vec<Entry> = trunk_items
            .iter()
            .map(|trunk_item| {
                let is_match = trunk_item.base_value == grain.base_value;

                let mut leaves = trunk_item.leaves.clone();

                let thing_item = Entry {
                    base: grain.leaf.to_owned(),
                    base_value: grain.leaf_value.clone(),
                    leader_value: None,
                    leaves: HashMap::new(),
                };

                leaves.insert(
                    grain.leaf.to_owned(),
                    [
                        match leaves.get(&grain.leaf) {
                            None => vec![],
                            Some(ls) => ls.clone(),
                        },
                        vec![thing_item],
                    ]
                    .concat(),
                );

                if is_match {
                    Entry {
                        base: trunk_item.base.to_owned(),
                        base_value: trunk_item.base_value.clone(),
                        leader_value: None,
                        leaves,
                    }
                } else {
                    trunk_item.clone()
                }
            })
            .collect();

        let mut entry_new = entry.clone();

        entry_new.leaves.insert(grain.base.to_owned(), trait_items);

        return entry_new;
    }

    // go into objects
    let leaves_new: HashMap<String, Vec<Entry>> = entry
        .leaves
        .iter()
        .map(|(leaf, leaf_items)| {
            let leaf_items_new: Vec<Entry> = leaf_items
                .iter()
                .map(|leaf_item| sow(leaf_item, grain, trait_, thing))
                .collect();

            (leaf.to_owned(), leaf_items_new)
        })
        .collect();

    let foo = Entry {
        base: entry.base.to_owned(),
        base_value: entry.base_value.clone(),
        leader_value: None,
        leaves: leaves_new,
    };

    // if trait_ == "datum" && thing == "filepath" {println!("{}", serde_json::to_string_pretty(&foo).expect(""))};

    foo
}
