use crate::types::entry::Entry;
use crate::schema::{Leaves, Schema, Trunks};
use crate::select::select_schema;
use async_stream::stream;
use futures_core::stream::Stream;
use futures_util::pin_mut;
use futures_util::stream::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Tablet {
    pub filename: String,
}

async fn sort_file(filename: &str) {
    return;
}

fn plan_insert(schema: Schema, query: Entry) -> Vec<Tablet> {
    vec![Tablet {
        filename: "datum-actdate.csv".to_string(),
    }]
}

fn insert_tablet<S: Stream<Item = Entry>>(input: S, tablet: Tablet) -> impl Stream<Item = Entry> {
    stream! {
        for await entry in input {
            yield entry;
        }
    }
}

async fn insert_record_stream<S: Stream<Item = Entry>>(input: S, path: PathBuf) -> impl Stream<Item = Entry> {
    // TODO rewrite to select
    let schema = select_schema(path.clone()).await;

    let mut strategy = vec![];

    stream! {
        for await query in input {

            strategy = plan_insert(schema.clone(), query.clone());

            // let streams: Vec<&mut dyn Stream<Item = Entry>> = strategy.iter().map(|tablet| insert_tablet(tablet)).collect();

            let query_stream = stream! {
                yield query;
            };

            // TODO fold strategy to streams
            let insert_stream = insert_tablet(query_stream, strategy[0].clone());
            // let insert_stream: Stream<Item = Entry> = strategy.iter().fold(query_stream, |with_stream, tablet| {
            //     stream! {}
            //     // insert_tablet(with_stream, tablet.clone())
            // });

            for await record in insert_stream {
                yield record;
            }
        }

        for tablet in strategy {
            sort_file(&tablet.filename).await;
        }
    }
}

pub async fn insert_record(path: PathBuf, query: Vec<Entry>) -> Vec<Entry> {
    let mut entries = vec![];

    let readable_stream = stream! {
        for q in query {
            yield q;
        }
    };

    let s = insert_record_stream(readable_stream, path).await;

    pin_mut!(s); // needed for iteration

    while let Some(entry) = s.next().await {
        entries.push(entry);
    }

    entries
}
