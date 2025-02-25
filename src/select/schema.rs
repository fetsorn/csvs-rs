use super::line::select_line_stream;
use super::types::state::State;
use super::types::tablet::Tablet;
use crate::types::entry::Entry;
use crate::types::line::Line;
use async_stream::stream;
use futures_core::stream::Stream;
use futures_util::pin_mut;
use futures_util::stream::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::path::PathBuf;

pub fn select_schema_line_stream<S: Stream<Item = Line>>(
    input: S,
    entry: Entry,
) -> impl Stream<Item = State> {
    stream! {
        let mut schema_entry = entry.clone();

        for await line in input {
            let trunk = line.key;

            let leaf = line.value;

            let leaves = match schema_entry.leaves.get(&trunk) { None => vec![], Some(ls) => ls.to_vec() };

            // append leaf
            let leaves_new = [leaves.clone(), vec![Entry {
                base: trunk.to_owned(),
                base_value: Some(leaf.to_owned()),
                leader_value: None,
                leaves: HashMap::new()
            }]].concat();

            // set leaves of trunk
            schema_entry.leaves.insert(trunk.to_owned(), leaves_new);
        }

        yield State {
            entry: Some(schema_entry),
            query: None,
            fst: None,
            match_map: None,
            has_match: false,
            is_match: false,
            thing_querying: None
        }
    }
}
