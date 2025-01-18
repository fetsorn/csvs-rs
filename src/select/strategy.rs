use crate::schema::{find_crown, Leaves, Schema, Trunks};
use crate::types::entry::Entry;
use async_stream::stream;
use futures_core::stream::Stream;
use futures_util::pin_mut;
use futures_util::stream::StreamExt;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Tablet {
    pub filename: String,
    pub thing: String,
    pub trait_: String,
    pub thing_is_first: bool,
    pub trait_is_first: bool,
    pub base: String,
    pub trait_is_regex: bool,
    pub passthrough: bool,
    pub querying: bool,
    pub eager: bool,
    pub accumulating: bool,
}

pub fn plan_select_schema(query: Entry) -> Vec<Tablet> {
    vec![Tablet {
        filename: "_-_.csv".to_string(),
        thing: "_".to_string(),
        trait_: "_".to_string(),
        thing_is_first: false,
        trait_is_first: true,
        passthrough: false,
        base: "_".to_string(),
        trait_is_regex: false,
        querying: false,
        eager: false,
        accumulating: false,
    }]
}

fn count_leaves(schema: Schema, branch: &str) -> usize {
    let (_, Leaves(leaves)) = schema.0.get(branch).unwrap();

    leaves.len()
}

fn sort_nesting_ascending(schema: Schema) -> impl FnMut(&String, &String) -> Ordering {
    move |a, b| {
        let schema = schema.clone();

        let (Trunks(trunks_a), _) = schema.0.get(a).unwrap();

        let (Trunks(trunks_b), _) = schema.0.get(b).unwrap();

        if (trunks_a.contains(b)) {
            return Ordering::Less;
        }

        if (trunks_b.contains(a)) {
            return Ordering::Greater;
        }

        if (count_leaves(schema.clone(), a) < count_leaves(schema.clone(), b)) {
            return Ordering::Less;
        }

        if (count_leaves(schema.clone(), a) > count_leaves(schema.clone(), b)) {
            return Ordering::Greater;
        }

        return Ordering::Equal;
    }
}

fn sort_nesting_descending(schema: Schema) -> impl FnMut(&String, &String) -> Ordering {
    move |a, b| {
        let schema = schema.clone();

        let (Trunks(trunks_a), _) = schema.0.get(a).unwrap();

        let (Trunks(trunks_b), _) = schema.0.get(b).unwrap();

        if (trunks_b.contains(a)) {
            return Ordering::Less;
        }

        if (trunks_a.contains(b)) {
            return Ordering::Greater;
        }

        if (count_leaves(schema.clone(), a) > count_leaves(schema.clone(), b)) {
            return Ordering::Less;
        }

        if (count_leaves(schema.clone(), a) < count_leaves(schema.clone(), b)) {
            return Ordering::Greater;
        }

        return Ordering::Equal;
    }
}

fn gather_keys(query: Entry) -> Vec<String> {
    let leaves = query
        .leaves
        .keys()
        .filter(|key| query.base_value.is_none() || **key != query.clone().base_value.unwrap());

    leaves.fold(vec![], |with_leaf, leaf| {
        let leaf_values = query.leaves.get(leaf).unwrap();

        let leaf_keys = leaf_values.iter().fold(vec![], |with_key, item| {
            let has_leaves = item.leaves.keys().len() > 0;

            let keys_item_new = if has_leaves {
                gather_keys(item.clone())
            } else {
                vec![]
            };

            vec![with_key, keys_item_new].concat()
        });

        vec![with_leaf, vec![leaf.to_string()], leaf_keys].concat()
    })
}

pub fn plan_query(schema: Schema, query: Entry) -> Vec<Tablet> {
    let mut queried_branches = gather_keys(query);

    queried_branches.sort_by(sort_nesting_ascending(schema.clone()));

    let queried_tablets = queried_branches.iter().fold(vec![], |with_branch, branch| {
        let empty = (Trunks(vec![]), Leaves(vec![]));

        let (Trunks(trunks), _) = schema.0.get(branch).unwrap_or(&empty);

        let tablets_new: Vec<Tablet> = trunks
            .iter()
            .map(|trunk| Tablet {
                thing: trunk.to_string(),
                trait_: branch.to_string(),
                thing_is_first: true,
                trait_is_first: false,
                base: trunk.to_string(),
                filename: format!("{}-{}.csv", trunk, branch),
                trait_is_regex: true,
                passthrough: false,
                querying: true,
                eager: true,
                accumulating: false,
            })
            .collect();

        return vec![with_branch, tablets_new].concat();
    });

    queried_tablets
}

pub fn plan_options(schema: Schema, base: String) -> Vec<Tablet> {
    let empty = (Trunks(vec![]), Leaves(vec![]));

    let (Trunks(trunks), Leaves(leaves)) = schema.0.get(&base).unwrap_or(&empty);

    let trunk_tablets: Vec<Tablet> = trunks
        .iter()
        .map(|trunk| Tablet {
            thing: base.to_string(),
            trait_: trunk.to_string(),
            thing_is_first: false,
            trait_is_first: false,
            base: trunk.to_string(),
            filename: format!("{}-{}.csv", trunk, base),
            trait_is_regex: true,
            passthrough: false,
            querying: false,
            eager: true,
            accumulating: true,
        })
        .collect();

    let leaf_tablets = leaves
        .iter()
        .map(|leaf| Tablet {
            thing: base.to_string(),
            trait_: base.to_string(),
            thing_is_first: true,
            trait_is_first: true,
            base: base.to_string(),
            filename: format!("{}-{}.csv", base, leaf),
            trait_is_regex: true,
            accumulating: true,
            passthrough: false,
            querying: false,
            eager: true,
        })
        .collect();

    vec![leaf_tablets, trunk_tablets].concat()
}

pub fn plan_values(schema: Schema, query: Entry) -> Vec<Tablet> {
    let mut crown: Vec<String> = find_crown(&schema, &query.base)
        .into_iter()
        .filter(|b| *b != query.base)
        .collect();

    crown.sort_by(sort_nesting_descending(schema.clone()));

    let value_tablets = crown.iter().fold(vec![], |with_branch, branch| {
        let (Trunks(trunks), _) = schema.0.get(branch).unwrap();

        let tablets_new = trunks
            .iter()
            .map(|trunk| Tablet {
                thing: branch.to_string(),
                trait_: trunk.to_string(),
                thing_is_first: false,
                trait_is_first: true,
                base: trunk.to_string(),
                filename: format!("{}-{}.csv", trunk, branch),
                trait_is_regex: false,
                accumulating: false,
                passthrough: true,
                querying: false,
                eager: *trunk == query.base,
            })
            .collect();

        vec![with_branch, tablets_new].concat()
    });

    value_tablets
}

pub fn plan_select(schema: Schema, query: Entry) -> Vec<Tablet> {
    let strategy_query = plan_query(schema.clone(), query.clone());

    let strategy_base = if strategy_query.len() > 0 {
        strategy_query
    } else {
        plan_options(schema.clone(), query.clone().base)
    };

    let strategy_value = plan_values(schema, query);

    let strategy = vec![strategy_base, strategy_value].concat();

    strategy
}
