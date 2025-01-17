use crate::types::entry::Entry;
use crate::types::line::Line;
use crate::schema::{Leaves, Schema, Trunks};
use super::strategy::Tablet;
use async_stream::stream;
use futures_core::stream::Stream;
use futures_util::pin_mut;
use futures_util::stream::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::fs::File;
use std::io::{self, prelude::*, BufReader};

fn select_schema_line_stream<S: Stream<Item = Line>>(input: S, entry: Entry) -> impl Stream<Item = Entry> {
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
                leaves: HashMap::new()
            }]].concat();

            // set leaves of trunk
            schema_entry.leaves.insert(trunk.clone(), leaves_new);
        }

        yield schema_entry;
    }
}

pub fn select_tablet<S: Stream<Item = Entry>>(input: S, path: PathBuf, tablet: Tablet) -> impl Stream<Item = Entry> {
    stream! {
        for await entry in input {

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

            // do select_schema_line_stream
            let s = select_schema_line_stream(line_stream, entry);

            pin_mut!(s); // needed for iteration

            // yield output
            while let Some(entry) = s.next().await {
                yield entry;
            }
        }
    }
}
