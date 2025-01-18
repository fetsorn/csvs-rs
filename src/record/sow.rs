use crate::types::entry::Entry;
use crate::types::grain::Grain;
use std::collections::HashMap;

// TODO remove trait, thing and use grain.base, grain.leaf
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
                    leader_value: None,
                    leaves: entry.leaves.clone(),
                },
            },
        };
    }

    //   append grain.thing to record.thing
    // if base equals trait
    if entry.base == trait_ {
        if entry.leaves.contains_key(thing) {
            let mut leaves = entry.leaves.clone();

            let thing_item = Entry {
                base: thing.to_string(),
                base_value: Some(grain.leaf_value.unwrap().to_string()),
                leader_value: None,
                leaves: HashMap::new(),
            };

            leaves.insert(
                thing.to_string(),
                [leaves[thing].clone(), vec![thing_item]].concat(),
            );

            return Entry {
                base: entry.base.to_string(),
                base_value: Some(entry.base_value.unwrap().to_string()),
                leader_value: None,
                leaves,
            };
        } else {
            return entry;
        }
    }

    let record_has_trait = entry.leaves.keys().any(|v| v == trait_);

    if record_has_trait {
        let trunk_items: Vec<Entry> = entry.leaves.get(trait_).unwrap().clone();

        let trait_items: Vec<Entry> = trunk_items
            .iter()
            .map(|trunk_item| {
                let is_match = trunk_item.base_value == grain.base_value;

                let mut leaves = trunk_item.leaves.clone();

                let thing_item = Entry {
                    base: grain.leaf.clone(),
                    base_value: grain.leaf_value.clone(),
                    leader_value: None,
                    leaves: HashMap::new(),
                };

                leaves.insert(
                    grain.leaf.to_string(),
                    [
                        leaves.get(&grain.leaf).unwrap_or(&vec![]).clone(),
                        vec![thing_item],
                    ]
                    .concat(),
                );

                if is_match {
                    Entry {
                        base: trunk_item.base.clone(),
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

        entry_new.leaves.insert(grain.base, trait_items);

        return entry_new;
    }

    // go into objects
    let leaves_new = entry
        .leaves
        .iter()
        .map(|(leaf, leaf_items)| {
            let leaf_items_new: Vec<Entry> = leaf_items
                .iter()
                .map(|leaf_item| sow(leaf_item.clone(), grain.clone(), trait_, thing))
                .collect();

            (leaf.clone(), leaf_items_new.clone())
        })
        .collect();

    Entry {
        base: entry.base.clone(),
        base_value: entry.base_value.clone(),
        leader_value: None,
        leaves: leaves_new,
    }
}
