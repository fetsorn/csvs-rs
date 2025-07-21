use crate::{Error, Result, Dataset, Entry, Schema};
mod line;
mod schema;
mod strategy;
mod types;
use strategy::{plan_select, plan_select_schema};
mod tablet;
use async_stream::{stream, try_stream};
use futures_core::stream::{BoxStream, Stream};
use futures_util::pin_mut;
use futures_util::stream::StreamExt;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use serde::{Deserialize, Serialize};
use tablet::select_tablet;
use types::state::State;
use serde_json::Value;

pub fn select_schema_stream<S: Stream<Item = Result<Entry>>>(
    dataset: Dataset,
    input: S,
) -> impl Stream<Item = Result<Entry>> {
    try_stream! {
        for await query in input {
            let query = query?;

            // TODO merge with select_record_stream
            let is_schema = query.base == *"_";

            if is_schema {
                let strategy = plan_select_schema(&query);

                let query_stream = try_stream! {
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

                let mut stream: BoxStream<'static, Result<State>> = Box::pin(query_stream);

                for tablet in strategy {
                    stream = Box::pin(select_tablet(dataset.dir.clone(), tablet, stream));
                }

                for await state in stream {
                    let state = state?;

                    match state.entry {
                        None => (),
                        Some(e) => yield e
                    }
                }
            } ;
        }
    }
}

pub async fn select_schema(dataset: Dataset) -> Result<Schema> {
    let readable_stream = try_stream! {
        yield Entry {
            base: "_".to_owned(),
            base_value: Some("_".to_owned()),
            leader_value: None,
            leaves: HashMap::new(),
        };
    };

    let s = dataset.select_schema_stream(readable_stream);

    pin_mut!(s); // needed for iteration

    let mut entries = vec![];

    while let Some(entry) = s.next().await {
        let entry = entry?;

        entries.push(entry);
    }

    Ok(entries[0].clone().try_into()?)
}

pub fn select_record_stream<S: Stream<Item = Result<Entry>>>(
    dataset: Dataset,
    input: S,
) -> impl Stream<Item = Result<Entry>> {
    try_stream! {
        for await query in input {
            let query = query?;

            let is_schema = query.base == "_";

            if is_schema {
                // TODO merge with select_schema_stream
                let strategy = plan_select_schema(&query);

                let query_stream = try_stream! {
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

                let mut stream: BoxStream<'static, Result<State>> = Box::pin(query_stream);

                for tablet in strategy {
                    stream = Box::pin(select_tablet(dataset.dir.clone(), tablet, stream));
                }

                for await state in stream {
                    let state = state?;

                    match state.entry {
                        None => (),
                        Some(e) => yield e
                    }
                }
            } else {
                let schema = dataset.clone().select_schema().await?;

                let strategy = plan_select(&schema, &query);

                let query_push = query.clone();

                let query_stream = try_stream! {
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


                let mut stream: BoxStream<'static, Result<State>> = Box::pin(query_stream);

                for tablet in strategy {
                    stream = Box::pin(select_tablet(dataset.dir.clone(), tablet, stream));
                }

                for await state in stream {
                    let state = state?;

                    // TODO move to leader stream
                    let base_new = match &state.entry {
                        None => panic!("unreachable"),
                        Some(e) => if e.base == query.base {
                            &e.base
                        } else {
                            &query.base
                        }
                    };

                    // if query has __, return leader
                    // TODO what if leader is nested? what if many leaders? use mow
                    match &query.leader_value {
                        None => {
                            match &state.entry {
                                None => (),
                                Some(e) => {
                                    let mut entry = e.clone();

                                    entry.base = base_new.to_owned();

                                    // do not return search result
                                    // if state comes from the end of accumulating
                                    if state.match_map.is_none() {
                                        yield entry;
                                    }
                                }
                            }
                        },
                        Some(s) => {
                            match &state.query {
                                None => (),
                                Some(q) => {
                                    match q.leaves.get(s) {
                                        None => (),
                                        Some(ls) => {
                                            match ls.first() {
                                               None => (),
                                                Some(l) => {
                                                    // do not return search result
                                                    // if state comes from the end of accumulating
                                                    if state.match_map.is_none() {
                                                        yield l.clone();
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    };
                }
            };

        }
    }
}

pub async fn select_record(dataset: Dataset, query: Vec<Entry>) -> Result<Vec<Entry>> {
    let mut entries = vec![];

    let readable_stream = try_stream! {
        for q in query {
            yield q;
        }
    };

    let s = dataset.select_record_stream(readable_stream);

    pin_mut!(s); // needed for iteration

    while let Some(entry) = s.next().await {
        let entry = entry?;

        entries.push(entry);
    }

    Ok(entries)
}

pub async fn print_record(dataset: Dataset, query: Vec<Entry>) -> Result<()> {
    let readable_stream = try_stream! {
        for q in query {
            yield q;
        }
    };

    let s = dataset.select_record_stream(readable_stream);

    pin_mut!(s); // needed for iteration

    while let Some(entry) = s.next().await {
        let entry = entry?;

        println!("{}", entry);
    }

    Ok(())
}
