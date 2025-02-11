use crate::types::{entry::Entry, schema::Schema};
mod line;
mod types;
mod strategy;
mod schema;
use strategy::{plan_select, plan_select_schema};
mod tablet;
use async_stream::stream;
use futures_core::stream::{BoxStream, Stream};
use futures_util::pin_mut;
use futures_util::stream::StreamExt;
use std::collections::HashMap;
use std::path::PathBuf;
use tablet::select_tablet;
use types::state::State;

pub fn select_schema_stream<S: Stream<Item = Entry>>(
    input: S,
    path: PathBuf,
) -> impl Stream<Item = Entry> {
    stream! {
        for await query in input {
            // TODO merge with select_record_stream
            let is_schema = query.base == *"_";

            if is_schema {
                let strategy = plan_select_schema(query.clone());

                let query_stream = stream! {
                    yield State {
                        entry: None,
                        query: Some(query),
                        match_map: Some(HashMap::new()),
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
            } ;
        }
    }
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

pub fn select_record_stream<S: Stream<Item = Entry>>(
    input: S,
    path: PathBuf,
) -> impl Stream<Item = Entry> {
    stream! {
        for await query in input {
            let is_schema = query.clone().base == *"_";

            if is_schema {
                // TODO merge with select_schema_stream
                let strategy = plan_select_schema(query.clone());

                let query_stream = stream! {
                    yield State {
                        entry: None,
                        query: Some(query),
                        match_map: Some(HashMap::new()),
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
                let schema = select_schema(path.clone()).await;

                let strategy = plan_select(schema.clone(), query.clone());

                let query_push = query.clone();

                let query_stream = stream! {
                    yield State {
                        entry: None,
                        query: Some(query_push),
                        is_match: false,
                        has_match: false,
                        thing_querying: None,
                        fst: None,
                        match_map: Some(HashMap::new()),
                    };
                };


                let mut stream: BoxStream<'static, State> = Box::pin(query_stream);

                for tablet in strategy.clone() {
                    stream = Box::pin(select_tablet(stream, path.clone(), tablet));
                }

                for await state in stream {
                    // TODO move to leader stream
                    let base_new = if state.entry.clone().unwrap().base != query.clone().base {
                        query.clone().base
                    } else {
                        state.entry.clone().unwrap().base
                    };

                    // if query has __, return leader
                    // TODO what if leader is nested? what if many leaders? use mow
                    let entry_new = match query.clone().leader_value {
                        None => {
                            let mut entry = state.entry.clone().unwrap();

                            entry.base = base_new;

                            entry
                        },
                        Some(s) => {
                            let bar = state.query.clone().unwrap();

                            let baz = bar.leaves.get(&s).unwrap();

                            baz.first().unwrap().clone()
                        }
                    };

                    // do not return search result
                    // if state comes from the end of accumulating
                    if state.match_map.is_none() {
                        yield entry_new;
                    }
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
