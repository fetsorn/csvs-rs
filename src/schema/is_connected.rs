use super::{Branch, Leaves, Schema, Trunks};

pub fn is_connected(schema: &Schema, base: &str, branch: &str) -> bool {
    if branch == base {
        // if branch is base, it is connected
        return true;
    }

    let Branch {
        trunks: Trunks(trunks),
        leaves: Leaves(leaves),
    } = match schema.0.get(branch) {
        None => Branch {
            trunks: Trunks(vec![]),
            leaves: Leaves(vec![]),
        },
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
