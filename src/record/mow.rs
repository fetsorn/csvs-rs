use crate::types::entry::Entry;
use crate::types::grain::Grain;

// TODO replace trait and thing with grain
//      but not clear how to then specify base_is_thing
//      since grain's thing is leaf but can't make leaf same as base
pub fn mow(entry: &Entry, trait_: &str, thing: &str) -> Vec<Grain> {
    if entry.base == thing {
        if trait_ == entry.base {
            return vec![Grain {
                base: entry.base.to_owned(),
                base_value: entry.base_value.clone(),
                leaf: trait_.to_owned(),
                leaf_value: entry.base_value.clone(),
            }];
        }

        let items = match entry.leaves.get(trait_) {
            None => &vec![],
            Some(vs) => vs,
        };

        let grains: Vec<Grain> = items
            .iter()
            .map(|item| Grain {
                base: entry.base.to_owned(),
                base_value: entry.base_value.clone(),
                leaf: trait_.to_owned(),
                leaf_value: item.base_value.clone(),
            })
            .collect();

        return grains;
    }

    if entry.base == trait_ {
        return match entry.leaves.get(thing) {
            None => vec![Grain {
                base: trait_.to_owned(),
                base_value: entry.base_value.clone(),
                leaf: thing.to_owned(),
                leaf_value: None,
            }],
            Some(items) => items
                .iter()
                .map(|item| Grain {
                    base: entry.base.to_owned(),
                    base_value: entry.base_value.clone(),
                    leaf: thing.to_owned(),
                    leaf_value: item.base_value.clone(),
                })
                .collect(),
        };
    }

    // TODO if record has trait
    let record_has_trait = entry.leaves.keys().any(|v| v == trait_);

    if record_has_trait {
        let trunk_items: &Vec<Entry> = &entry.leaves[trait_];

        let grains: Vec<Grain> = trunk_items
            .iter()
            .fold(vec![], |with_trunk_item, trunk_item| {
                if trunk_item.leaves.contains_key(thing) {
                    let branch_items: &Vec<Entry> = &trunk_item.leaves[thing];

                    let trunk_item_grains = branch_items
                        .iter()
                        .map(|branch_item| Grain {
                            base: trait_.to_owned(),
                            base_value: Some(trunk_item.base_value.as_ref().unwrap().to_owned()),
                            leaf: thing.to_owned(),
                            leaf_value: Some(branch_item.base_value.as_ref().unwrap().to_owned()),
                        })
                        .collect();

                    [with_trunk_item, trunk_item_grains].concat()
                } else {
                    // TODO somewhere here return { _: trait, [trait]: trunkValue }
                    //      if branch item does not have base value
                    let grain = Grain {
                        base: trait_.to_owned(),
                        base_value: Some(trunk_item.base_value.as_ref().unwrap().to_owned()),
                        leaf: thing.to_owned(),
                        leaf_value: None,
                    };

                    [with_trunk_item, vec![grain]].concat()
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
                let leaf_item_grains = mow(leaf_item, trait_, thing);

                [with_leaf_item, leaf_item_grains].concat()
            });

            [with_entry, leaf_grains].concat()
        })
}
