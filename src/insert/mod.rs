use crate::types::entry::Entry;
use crate::schema::{Leaves, Schema, Trunks};
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

fn insert_record_stream<S: Stream<Item = Entry>>(input: S) -> impl Stream<Item = Entry> {
    // TODO rewrite to select
    let schema = Schema(HashMap::from([
        (
            "datum".to_string(),
            (
                Trunks(vec![]),
                Leaves(vec![
                    "actdate".to_string(),
                    "name".to_string()]
                ),
            ),
        ),
        (
            "date".to_string(),
            (Trunks(vec!["datum".to_string()]), Leaves(vec![])),
        ),
        (
            "name".to_string(),
            (Trunks(vec!["datum".to_string()]), Leaves(vec![])),
        ),
    ]));

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

pub async fn insert_record(path: PathBuf, query: Entry) -> Vec<Entry> {
    let mut entries = vec![];

    let readable_stream = stream! {
        yield query;
    };

    let s = insert_record_stream(readable_stream);

    pin_mut!(s); // needed for iteration

    while let Some(entry) = s.next().await {
        entries.push(entry);
    }

    entries
}
