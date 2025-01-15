use crate::types::entry::Entry;
use crate::schema::{Leaves, Schema, Trunks};
mod strategy;
use strategy::{plan_select, plan_select_schema};
mod tablet;
use tablet::select_tablet;
use async_stream::stream;
use futures_core::stream::Stream;
use futures_util::pin_mut;
use futures_util::stream::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

fn select_schema_stream<S: Stream<Item = Entry>>(input: S, path: PathBuf) -> impl Stream<Item = Entry> {
    stream! {
        for await query in input {
            let is_schema = query.base == "_".to_string();

            if is_schema {
                let strategy = plan_select_schema(query.clone());

                let query_stream = stream! {
                    yield query;
                };

                // TODO fold strategy to streams
                let select_stream = select_tablet(query_stream, path.clone(), strategy[0].clone());
                // let select_stream: Stream<Item = Entry> = strategy.iter().fold(query_stream, |with_stream, tablet| {
                //     stream! {}
                //     // select_tablet(with_stream, tablet.clone())
                // });

                for await record in select_stream {
                    yield record;
                }
            } else {
            };
        }
    }
}

fn select_record_stream<S: Stream<Item = Entry>>(input: S, path: PathBuf) -> impl Stream<Item = Entry> {
    stream! {
        for await query in input {
            let is_schema = query.base == "_".to_string();

            if is_schema {
            } else {
                let schema = select_schema(path.clone()).await;

                let strategy = plan_select(schema.clone(), query.clone());

                let query_stream = stream! {
                    yield query;
                };

                // TODO fold strategy to streams
                let select_stream = select_tablet(query_stream, path.clone(), strategy[0].clone());
                // let select_stream: Stream<Item = Entry> = strategy.iter().fold(query_stream, |with_stream, tablet| {
                //     stream! {}
                //     // select_tablet(with_stream, tablet.clone())
                // });

                for await record in select_stream {
                    yield record;
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
