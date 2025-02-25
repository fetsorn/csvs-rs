use crate::types::{
    entry::Entry,
    schema::{Leaves, Schema, Trunks},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::convert::TryFrom;

pub fn is_connected(schema: &Schema, base: &str, branch: &str) -> bool {
    if branch == base {
        // if branch is base, it is connected
        return true;
    }

    let (Trunks(trunks), Leaves(leaves)) = match schema.0.get(branch) {
        None => (Trunks(vec![]), Leaves(vec![])),
        Some(vs) => vs.clone(),
    };

    for trunk in trunks.iter() {
        if trunk == base {
            // if trunk is base, leaf is connected to base
            return true;
        }

        if is_connected(schema, base, trunk) {
            // if trunk is connected to base, leaf is also connected to base
            return true;
        }
    }

    // if trunk is not connected to base, leaf is also not connected to base
    false
}

pub fn find_crown(schema: &Schema, base: &str) -> Vec<String> {
    schema
        .0
        .keys()
        .filter(|branch| is_connected(schema, base, branch))
        .cloned()
        .collect()
}

pub fn count_leaves(schema: Schema, branch: &str) -> usize {
    let (_, Leaves(leaves)) = schema.0.get(branch).unwrap();

    leaves.len()
}

pub fn get_nesting_level(schema: &Schema, branch: &str) -> i32 {
    let (Trunks(trunks), _) = schema.0.get(branch).unwrap();

    let trunk_levels: Vec<i32> = trunks
        .iter()
        .map(|trunk| get_nesting_level(schema, trunk))
        .collect();

    let level: i32 = *trunk_levels.iter().max().unwrap_or(&-1);

    level + 1
}

pub fn sort_nesting_descending(schema: Schema) -> impl FnMut(&String, &String) -> Ordering {
    move |a, b| {
        let schema = schema.clone();

        let level_a = get_nesting_level(&schema, a);

        let level_b = get_nesting_level(&schema, b);

        if level_a < level_b {
            return Ordering::Less;
        }

        if level_a > level_b {
            return Ordering::Greater;
        }

        return a.cmp(b);
    }
}

pub fn sort_nesting_ascending(schema: Schema) -> impl FnMut(&String, &String) -> Ordering {
    move |a, b| {
        let schema = schema.clone();

        let level_a = get_nesting_level(&schema, a);

        let level_b = get_nesting_level(&schema, b);

        if level_a > level_b {
            return Ordering::Less;
        }

        if level_a < level_b {
            return Ordering::Greater;
        }

        return b.cmp(a);
    }
}
