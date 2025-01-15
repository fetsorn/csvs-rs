use crate::types::entry::Entry;
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
                                Leaves(vec![trunk_leaves_old, vec![leaf.clone()]].concat());

                            node_map_new.insert(trunk.clone(), (trunk_trunks, trunk_leaves));

                            let (Trunks(leaf_trunks_old), Leaves(leaf_leaves_old)) =
                                with_leaf.get(&leaf).unwrap_or(&empty_node.clone()).clone();

                            let leaf_trunks =
                                Trunks(vec![leaf_trunks_old, vec![trunk.to_string()]].concat());

                            let leaf_leaves = Leaves(leaf_leaves_old);

                            node_map_new.insert(leaf.clone(), (leaf_trunks, leaf_leaves));

                            node_map_new
                        },
                    )
                });

        Ok(Schema(node_map))
    }
}

pub fn is_connected(schema: &Schema, base: &str, branch: &str) -> bool {
    if branch == base {
        // if branch is base, it is connected
        return true;
    }

    let empty_node = (Trunks(vec![]), Leaves(vec![]));

    let (Trunks(trunks), Leaves(leaves)) =
        schema.0.get(branch).unwrap_or(&empty_node.clone()).clone();

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
    return false;
}

pub fn find_crown(schema: &Schema, base: &str) -> Vec<String> {
    schema
        .0
        .clone()
        .keys()
        .filter(|branch| is_connected(schema, base, branch))
        .cloned()
        .collect()
}
