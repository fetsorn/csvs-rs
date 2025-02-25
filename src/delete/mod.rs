use crate::select::select_schema;
use crate::types::schema::{Schema, Leaves, Trunks};
use crate::types::entry::Entry;
use crate::types::line::Line;
use async_stream::stream;
use futures_core::stream::Stream;
use futures_util::pin_mut;
use futures_util::stream::StreamExt;
use serde::{Deserialize, Serialize};
use std::fs;
use std::fs::File;
use std::io::prelude::*;
use std::path::{PathBuf, Path};
use temp_dir::TempDir;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Tablet {
    pub filename: String,
    pub trait_: String,
    pub trait_is_first: bool,
}

fn plan_delete(schema: &Schema, query: &Entry) -> Vec<Tablet> {
    let (Trunks(trunks), Leaves(leaves)) = schema.0.get(&query.base).unwrap();

    let trunk_tablets: Vec<Tablet> = trunks
        .iter()
        .map(|trunk| Tablet {
            filename: format!("{}-{}.csv", trunk, query.base),
            trait_: query.base_value.as_ref().unwrap().to_owned(),
            trait_is_first: false,
        })
        .collect();

    let leaf_tablets = leaves
        .iter()
        .map(|leaf| Tablet {
            filename: format!("{}-{}.csv", query.base, leaf),
            trait_: query.base_value.as_ref().unwrap().to_owned(),
            trait_is_first: true,
        })
        .collect();

    [trunk_tablets, leaf_tablets].concat()
}

async fn delete_tablet(path: &Path, tablet: Tablet) {
    let filepath = path.join(&tablet.filename);

    match fs::metadata(&filepath) {
        Err(_) => return,
        Ok(m) => {
            if m.len() == 0 {
                return;
            }
        }
    }

    let file = File::open(&filepath).unwrap();

    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(file);

    let temp_path = TempDir::new().unwrap();

    let output = temp_path.as_ref().join(filepath.file_name().unwrap());

    let temp_file = File::create(&output).unwrap();

    let mut wtr = csv::WriterBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_writer(temp_file);

    for result in rdr.records() {
        let record = result.unwrap();

        let line = Line {
            key: match record.get(0) { None => String::from(""), Some(s) => s.to_owned() },
            value: match record.get(1) { None => String::from(""), Some(s) => s.to_owned() }
        };

        let trait_ = if tablet.trait_is_first {
            line.key.to_owned()
        } else {
            line.value.to_owned()
        };

        let is_match = trait_ == tablet.trait_;

        if !is_match {
            wtr.serialize(line).unwrap();
        }
    }

    wtr.flush().unwrap();

    // if empty
    match fs::metadata(&output) {
        Err(_) => fs::remove_file(filepath).unwrap(),
        Ok(m) => {
            if m.len() == 0 {
                fs::remove_file(filepath).unwrap();
            } else {
                fs::copy(output, filepath).unwrap();
            }
        }
    }
}

async fn delete_record_stream<S: Stream<Item = Entry>>(
    input: S,
    path: PathBuf,
) -> impl Stream<Item = Entry> {
    let schema = select_schema(&path).await;

    stream! {
        for await query in input {
            let strategy = plan_delete(&schema, &query);

            for tablet in strategy {
                delete_tablet(&path, tablet).await;
            }

            yield query;
        }
    }
}

pub async fn delete_record(path: PathBuf, query: Vec<Entry>) -> Vec<Entry> {
    let mut entries = vec![];

    let readable_stream = stream! {
        for q in query {
            yield q;
        }
    };

    let s = delete_record_stream(readable_stream, path).await;

    pin_mut!(s); // needed for iteration

    while let Some(entry) = s.next().await {
        entries.push(entry);
    }

    entries
}
