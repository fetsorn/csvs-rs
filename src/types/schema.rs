use crate::types::entry::Entry;
use serde_json::Value;
use std::cmp::Ordering;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::convert::TryFrom;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Leaves(pub Vec<String>);

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Trunks(pub Vec<String>);

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Schema(pub HashMap<String, (Trunks, Leaves)>);

impl TryFrom<Entry> for Schema {
    type Error = ();

    fn try_from(entry: Entry) -> Result<Self, Self::Error> {
        if entry.base != "_" {
            return Err(());
        }

        let node_map: HashMap<String, (Trunks, Leaves)> =
            entry
                .leaves
                .iter()
                .fold(HashMap::new(), |with_trunk, (trunk, leaves)| {
                    leaves.iter().map(|e| e.base_value.clone().unwrap()).fold(
                        with_trunk,
                        |with_leaf, leaf| {
                            let mut node_map_new = with_leaf.clone();

                            let empty_node = (Trunks(vec![]), Leaves(vec![]));

                            let (Trunks(trunk_trunks_old), Leaves(trunk_leaves_old)) =
                                with_leaf.get(trunk).unwrap_or(&empty_node.clone()).clone();

                            let trunk_trunks = Trunks(trunk_trunks_old);

                            let trunk_leaves =
                                Leaves([trunk_leaves_old, vec![leaf.clone()]].concat());

                            node_map_new.insert(trunk.clone(), (trunk_trunks, trunk_leaves));

                            let (Trunks(leaf_trunks_old), Leaves(leaf_leaves_old)) =
                                with_leaf.get(&leaf).unwrap_or(&empty_node.clone()).clone();

                            let leaf_trunks =
                                Trunks([leaf_trunks_old, vec![trunk.to_string()]].concat());

                            let leaf_leaves = Leaves(leaf_leaves_old);

                            node_map_new.insert(leaf.clone(), (leaf_trunks, leaf_leaves));

                            node_map_new
                        },
                    )
                });

        Ok(Schema(node_map))
    }
}

impl TryFrom<Value> for Schema {
    type Error = ();

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        let entry: Entry = value.try_into().unwrap();

        entry.try_into()
    }
}
