use super::line::select_line_stream;
use super::schema::select_schema_line_stream;
use super::types::state::State;
use super::types::tablet::Tablet;
use crate::error::{Error, Result};
use crate::types::entry::Entry;
use crate::types::line::Line;
use async_stream::{stream, try_stream};
use futures_core::stream::Stream;
use futures_util::pin_mut;
use futures_util::stream::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::path::PathBuf;

fn line_stream(filepath: PathBuf) -> impl Stream<Item = Result<Line>> {
    try_stream! {
        if std::fs::metadata(&filepath).is_err() {
            return;
        }

        let file = File::open(filepath)?;

        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .flexible(true)
            .from_reader(file);

        for result in rdr.records() {
            let record = result?;

            let line = Line {
                key: match record.get(0) { None => String::from(""), Some(s) => s.to_owned() },
                value: match record.get(1) { None => String::from(""), Some(s) => s.to_owned() }
            };

            yield line;
        }
    }
}

pub fn select_tablet<S: Stream<Item = Result<State>>>(
    input: S,
    path: PathBuf,
    tablet: Tablet,
) -> impl Stream<Item = Result<State>> {
    // println!("{}", serde_json::to_string_pretty(&tablet).expect(""));
    // println!("{}", tablet.filename);

    try_stream! {
        for await state in input {
            let state = state?;

            let filepath = path.join(&tablet.filename);

            let is_schema = tablet.filename == "_-_.csv";

            if is_schema {
                // wrap in result here for try_stream! proc to pick up error from ?
                let query = match state.query {
                    None => Err(Error::from_message("unexpected missing query")),
                    Some(q) => Ok(q)
                }?;

                // do select_schema_line_stream
                let s = select_schema_line_stream(line_stream(filepath), query);

                pin_mut!(s); // needed for iteration

                // yield output
                while let Some(state) = s.next().await {
                    let state = state?;

                    yield state;
                }
            } else {
                // value tablets receive a matchMap from accumulating tablets
                // but don't need to do anything with it or with the accompanying entry
                let drop_match_map = tablet.passthrough && state.match_map.is_some();

                if drop_match_map {
                    // do nothing
                    continue;
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
                    // println!("wtf {} {}", tablet.filename, state);
                    yield State {
                        entry: state.entry,
                        query: state.query,
                        fst: None,
                        is_match: false,
                        match_map: None,
                        has_match: false,
                        thing_querying: None
                    };

                    continue;
                }

                let s = select_line_stream(line_stream(filepath), state, tablet.clone());

                pin_mut!(s); // needed for iteration

                // yield output
                while let Some(state) = s.next().await {
                    let state = state?;

                    yield state;
                }
            }
        }
    }
}
