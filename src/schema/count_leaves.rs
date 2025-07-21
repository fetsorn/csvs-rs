use super::{Branch, Leaves, Schema};

pub fn count_leaves(schema: &Schema, branch: &str) -> usize {
    let leaves = match schema.0.get(branch) {
        None => vec![],
        Some(Branch {
            leaves: Leaves(ls), ..
        }) => ls.to_vec(),
    };

    leaves.len()
}
