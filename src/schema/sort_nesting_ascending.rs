use super::{Branch, Leaves, Schema, Trunks};
use std::cmp::Ordering;

pub fn sort_nesting_ascending(schema: Schema) -> impl FnMut(&String, &String) -> Ordering {
    move |a, b| {
        let schema = schema.clone();

        let level_a = schema.get_nesting_level(a);

        let level_b = schema.get_nesting_level(b);

        if level_a > level_b {
            return Ordering::Less;
        }

        if level_a < level_b {
            return Ordering::Greater;
        }

        return b.cmp(a);
    }
}
