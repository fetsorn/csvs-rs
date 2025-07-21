use super::Schema;

pub fn find_crown(schema: &Schema, base: &str) -> Vec<String> {
    schema
        .0
        .keys()
        .filter(|branch| schema.is_connected(base, branch))
        .cloned()
        .collect()
}
