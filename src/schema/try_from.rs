use super::{Branch, Leaves, Schema, Trunks};
use crate::{Entry, Error, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::convert::TryFrom;

impl TryFrom<Entry> for Schema {
    type Error = Error;

    fn try_from(entry: Entry) -> Result<Self> {
        if entry.base != "_" {
            return Err(Error::from_message("base is not _"));
        }

        let node_map: HashMap<String, Branch> =
            entry
                .leaves
                .iter()
                .fold(HashMap::new(), |with_trunk, (trunk, leaves)| {
                    leaves.iter().filter_map(|e| e.base_value.as_ref()).fold(
                        with_trunk,
                        |mut with_leaf, leaf| {
                            let trunk_branch = match with_leaf.get(trunk) {
                                None => Branch {
                                    trunks: Trunks(vec![]),
                                    leaves: Leaves(vec![]),
                                },
                                Some(vs) => vs.clone(),
                            };

                            let trunk_trunks = trunk_branch.trunks.clone();

                            let trunk_leaves =
                                Leaves([&trunk_branch.leaves.0[..], &[leaf.clone()]].concat());

                            with_leaf.insert(
                                trunk.to_owned(),
                                Branch {
                                    trunks: trunk_trunks,
                                    leaves: trunk_leaves,
                                },
                            );

                            let leaf_branch = match with_leaf.get(leaf) {
                                None => Branch {
                                    trunks: Trunks(vec![]),
                                    leaves: Leaves(vec![]),
                                },
                                Some(vs) => vs.clone(),
                            };

                            let leaf_trunks =
                                Trunks([&leaf_branch.trunks.0[..], &[trunk.to_owned()]].concat());

                            let leaf_leaves = leaf_branch.leaves;

                            with_leaf.insert(
                                leaf.to_owned(),
                                Branch {
                                    trunks: leaf_trunks,
                                    leaves: leaf_leaves,
                                },
                            );

                            with_leaf
                        },
                    )
                });

        Ok(Schema(node_map))
    }
}

impl TryFrom<Value> for Schema {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self> {
        let entry: Entry = value.try_into()?;

        entry.try_into()
    }
}
