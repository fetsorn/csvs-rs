use super::types::tablet::Tablet;
use crate::schema::{find_crown, sort_nesting_ascending, sort_nesting_descending};
use crate::types::{
    entry::Entry,
    schema::{Branch, Leaves, Schema, Trunks},
};
use futures_util::stream::StreamExt;
use serde::{Deserialize, Serialize};

pub fn plan_select_schema(query: &Entry) -> Vec<Tablet> {
    vec![Tablet {
        filename: "_-_.csv".to_owned(),
        thing: "_".to_owned(),
        trait_: "_".to_owned(),
        thing_is_first: false,
        trait_is_first: true,
        passthrough: false,
        base: "_".to_owned(),
        trait_is_regex: false,
        querying: false,
        eager: false,
        accumulating: false,
    }]
}

fn gather_keys(query: &Entry) -> Vec<String> {
    let leaves = query.leaves.keys().filter(|key| match &query.base_value {
        None => true,
        Some(s) => key != &s,
    });

    leaves.fold(vec![], |with_leaf, leaf| {
        let leaf_keys = match &query.leaves.get(leaf) {
            None => vec![],
            Some(vs) => vs.iter().fold(vec![], |with_key, item| {
                let has_leaves = item.leaves.keys().len() > 0;

                let keys_item_new = if has_leaves {
                    gather_keys(item)
                } else {
                    vec![]
                };

                [with_key, keys_item_new].concat()
            }),
        };

        [&with_leaf[..], &[leaf.to_owned()], &leaf_keys[..]].concat()
    })
}

pub fn plan_query(schema: &Schema, query: &Entry) -> Vec<Tablet> {
    let mut queried_branches = gather_keys(query);

    queried_branches.sort_by(sort_nesting_ascending(schema.clone()));

    let queried_tablets = queried_branches.iter().fold(vec![], |with_branch, branch| {
        let trunks = match schema.0.get(branch) {
            None => vec![],
            Some(Branch {
                trunks: Trunks(ts), ..
            }) => ts.to_vec(),
        };

        let tablets_new: Vec<Tablet> = trunks
            .iter()
            .map(|trunk| Tablet {
                thing: trunk.to_owned(),
                trait_: branch.to_owned(),
                thing_is_first: true,
                trait_is_first: false,
                base: trunk.to_owned(),
                filename: format!("{}-{}.csv", trunk, branch),
                trait_is_regex: true,
                passthrough: false,
                querying: true,
                eager: true,
                accumulating: false,
            })
            .collect();

        [with_branch, tablets_new].concat()
    });

    queried_tablets
}

pub fn plan_options(schema: &Schema, base: &str) -> Vec<Tablet> {
    let empty = (Trunks(vec![]), Leaves(vec![]));

    let (trunks, leaves) = match schema.0.get(base) {
        None => (vec![], vec![]),
        Some(Branch {
            trunks: Trunks(ts),
            leaves: Leaves(ls),
        }) => (ts.to_vec(), ls.to_vec()),
    };

    let trunk_tablets: Vec<Tablet> = trunks
        .iter()
        .map(|trunk| Tablet {
            thing: base.to_owned(),
            trait_: trunk.to_owned(),
            thing_is_first: false,
            trait_is_first: false,
            base: trunk.to_owned(),
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
            thing: base.to_owned(),
            trait_: base.to_owned(),
            thing_is_first: true,
            trait_is_first: true,
            base: base.to_owned(),
            filename: format!("{}-{}.csv", base, leaf),
            trait_is_regex: true,
            accumulating: true,
            passthrough: false,
            querying: false,
            eager: true,
        })
        .collect();

    [leaf_tablets, trunk_tablets].concat()
}

pub fn plan_values(schema: &Schema, query: &Entry) -> Vec<Tablet> {
    let mut crown: Vec<String> = find_crown(&schema, &query.base)
        .into_iter()
        .filter(|b| *b != query.base)
        .collect();

    crown.sort_by(sort_nesting_descending(schema.clone()));

    // println!("{:#?}", crown);

    let value_tablets = crown.iter().fold(vec![], |with_branch, branch| {
        let trunks = match schema.0.get(branch) {
            None => vec![],
            Some(Branch {
                trunks: Trunks(ts), ..
            }) => ts.to_vec(),
        };

        let tablets_new = trunks
            .iter()
            .map(|trunk| Tablet {
                thing: branch.to_owned(),
                trait_: trunk.to_owned(),
                thing_is_first: false,
                trait_is_first: true,
                base: trunk.to_owned(),
                filename: format!("{}-{}.csv", trunk, branch),
                trait_is_regex: false,
                accumulating: false,
                passthrough: true,
                querying: false,
                eager: *trunk == query.base,
            })
            .collect();

        [with_branch, tablets_new].concat()
    });

    value_tablets
}

pub fn plan_select(schema: &Schema, query: &Entry) -> Vec<Tablet> {
    let strategy_query = plan_query(schema, query);

    let strategy_base = if !strategy_query.is_empty() {
        strategy_query
    } else {
        plan_options(schema, &query.base)
    };

    let strategy_value = plan_values(schema, query);

    [strategy_base, strategy_value].concat()
}
