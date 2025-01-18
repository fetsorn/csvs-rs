use super::line::select_line_stream;
use super::strategy::Tablet;
use crate::schema::{Leaves, Schema, Trunks};
use crate::types::entry::Entry;
use crate::types::line::Line;
use async_stream::stream;
use futures_core::stream::Stream;
use futures_util::pin_mut;
use futures_util::stream::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, prelude::*, BufReader};
use std::path::PathBuf;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct State {
    pub entry: Option<Entry>,
    pub query: Option<Entry>,
    pub fst: Option<String>,
    pub is_match: bool,
    pub match_map: Option<HashMap<String, bool>>,
    pub has_match: bool,
    pub thing_querying: Option<String>,
}

fn select_schema_line_stream<S: Stream<Item = Line>>(
    input: S,
    entry: Entry,
) -> impl Stream<Item = State> {
    stream! {
        let mut schema_entry = entry.clone();

        for await line in input {
            let trunk = line.key;

            let leaf = line.value;

            let empty_leaves = vec![];

            let leaves = schema_entry.leaves.get(&trunk).unwrap_or(&empty_leaves);

            // append leaf
            let leaves_new = vec![leaves.clone(), vec![Entry {
                base: trunk.clone(),
                base_value: Some(leaf.clone()),
                leader_value: None,
                leaves: HashMap::new()
            }]].concat();

            // set leaves of trunk
            schema_entry.leaves.insert(trunk.clone(), leaves_new);
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

pub fn select_tablet<S: Stream<Item = State>>(
    input: S,
    path: PathBuf,
    tablet: Tablet,
) -> impl Stream<Item = State> {
    stream! {
        for await state in input {

            let filepath = path.join(&tablet.filename);

            // for every line from tablet.filename
            let line_stream = stream! {
                if std::fs::metadata(filepath.clone()).is_err() {
                    return;
                }

                let file = File::open(filepath).unwrap();

                let mut rdr = csv::ReaderBuilder::new()
                        .has_headers(false)
                        .from_reader(file);

                for result in rdr.deserialize() {
                    let record: Line = result.unwrap();

                    yield record;
                }
            };

            let is_schema = tablet.filename == "_-_.csv";

            if is_schema {
                // do select_schema_line_stream
                let s = select_schema_line_stream(line_stream, state.entry.unwrap());

                pin_mut!(s); // needed for iteration

                // yield output
                while let Some(state) = s.next().await {
                    yield state;
                }
            } else {

                // value tablets receive a matchMap from accumulating tablets
                // but don't need to do anything with it or with the accompanying entry
                let drop_match_map = tablet.passthrough && state.match_map.is_some();

                if drop_match_map {
                    // do nothing
                    return;
                }

                // accumulating tablets find all values
                // matched at least once across the dataset
                // to do this they track matches in a shared match map
                // when a new entry is found, it is sent forward without a matchMap
                // and each accumulating tablet forwards the entry as is
                // until the entry reaches non-accumulating value tablets
                // assume the entry is new
                // because it has been checked against the match map upstream
                let forward_accumulating = tablet.accumulating && state.match_map.is_none();

                if forward_accumulating {
                    yield State {
                        entry: state.entry,
                        query: state.query,
                        fst: None,
                        is_match: false,
                        match_map: None,
                        has_match: false,
                        thing_querying: None
                    };

                    return;
                }

                let s = select_line_stream(line_stream, state, tablet.clone());

                pin_mut!(s); // needed for iteration

                // yield output
                while let Some(state) = s.next().await {
                    yield state;
                }
            }
        }
    }
}
