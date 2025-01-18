use crate::schema::{Leaves, Schema, Trunks};
use crate::types::entry::Entry;
mod line;
mod strategy;
use strategy::{plan_select, plan_select_schema};
mod tablet;
use async_stream::stream;
use futures_core::stream::{BoxStream, Stream};
use futures_util::pin_mut;
use futures_util::stream::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use tablet::select_tablet;
use tablet::State;

fn select_schema_stream<S: Stream<Item = Entry>>(
    input: S,
    path: PathBuf,
) -> impl Stream<Item = Entry> {
    stream! {
        for await query in input {
            let is_schema = query.base == "_".to_string();

            if is_schema {
                let strategy = plan_select_schema(query.clone());

                let query_stream = stream! {
                    yield State {
                        entry: Some(query),
                        query: None,
                        match_map: None,
                        has_match: false,
                        is_match: false,
                        fst: None,
                        thing_querying: None
                    };
                };

                let mut stream: BoxStream<'static, State> = Box::pin(query_stream);

                for tablet in strategy.clone() {
                    stream = Box::pin(select_tablet(stream, path.clone(), tablet));
                }

                for await state in stream {
                    yield state.entry.unwrap();
                }
            } else {
            };
        }
    }
}

fn select_record_stream<S: Stream<Item = Entry>>(
    input: S,
    path: PathBuf,
) -> impl Stream<Item = Entry> {
    stream! {
        for await query in input {
            let is_schema = query.base == "_".to_string();

            if is_schema {
            } else {
                let schema = select_schema(path.clone()).await;

                let strategy = plan_select(schema.clone(), query.clone());

                let query_stream = stream! {
                    yield State {
                        entry: Some(query),
                        query: None,
                        is_match: false,
                        has_match: false,
                        thing_querying: None,
                        fst: None,
                        match_map: None
                    };
                };


                let mut stream: BoxStream<'static, State> = Box::pin(query_stream);

                for tablet in strategy.clone() {
                    stream = Box::pin(select_tablet(stream, path.clone(), tablet));
                }

                for await state in stream {
                    yield state.entry.unwrap();
                }
            };

        }
    }
}

pub async fn select_record(path: PathBuf, query: Vec<Entry>) -> Vec<Entry> {
    let mut entries = vec![];

    let readable_stream = stream! {
        for q in query {
            yield q;
        }
    };

    let s = select_record_stream(readable_stream, path);

    pin_mut!(s); // needed for iteration

    while let Some(entry) = s.next().await {
        entries.push(entry);
    }

    entries
}

pub async fn select_schema(path: PathBuf) -> Schema {
    let readable_stream = stream! {
        yield Entry {
            base: "_".to_string(),
            base_value: Some("_".to_string()),
            leader_value: None,
            leaves: HashMap::new(),
        };
    };

    let s = select_schema_stream(readable_stream, path);

    pin_mut!(s); // needed for iteration

    let mut entries = vec![];

    while let Some(entry) = s.next().await {
        entries.push(entry);
    }

    entries[0].clone().try_into().unwrap()
}
