use crate::record::mow::mow;
use crate::schema::find_crown;
use crate::types::schema::Schema;
use crate::select::select_schema;
use crate::types::entry::Entry;
use crate::types::grain::Grain;
use crate::types::line::Line;
use async_stream::stream;
use futures_core::stream::{BoxStream, Stream};
use futures_util::pin_mut;
use futures_util::stream::StreamExt;
use serde::{Deserialize, Serialize};
use std::fs;
use std::fs::OpenOptions;
use std::fs::{rename, File};
use std::path::PathBuf;
use temp_dir::TempDir;
use text_file_sort::sort::Sort;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Tablet {
    pub filename: String,
    pub trunk: String,
    pub branch: String,
}

async fn sort_file(filepath: PathBuf) {
    let temp_path = TempDir::new().unwrap();

    let output = temp_path.as_ref().join(filepath.file_name().unwrap());

    let text_file_sort = Sort::new(vec![filepath.clone()], output.clone());

    text_file_sort.sort().unwrap();

    rename(output, filepath.clone()).unwrap();
}

fn plan_insert(schema: Schema, query: Entry) -> Vec<Tablet> {
    let crown = find_crown(&schema, &query.base);

    let tablets = crown.iter().fold(vec![], |with_branch, branch| {
        let tablets_new = schema
            .0
            .get(branch)
            .unwrap()
            .0
             .0
            .iter()
            .map(|trunk| Tablet {
                filename: format!("{}-{}.csv", trunk, branch),
                trunk: trunk.clone(),
                branch: branch.clone(),
            })
            .collect();

        [with_branch, tablets_new].concat()
    });

    tablets
}

fn insert_tablet<S: Stream<Item = Entry>>(
    input: S,
    path: PathBuf,
    tablet: Tablet,
) -> impl Stream<Item = Entry> {
    let filepath = path.join(&tablet.filename);

    // create file if it doesn't exist
    if fs::metadata(filepath.clone()).is_err() {
        File::create(filepath.clone()).unwrap();
    }

    let file = OpenOptions::new()
        .append(true)
        .open(filepath)
        .expect("cannot open file");

    let mut wtr = csv::WriterBuilder::new()
        .has_headers(false)
        .from_writer(file);

    stream! {
        for await entry in input {
            // pass the query early on to start other tablet streams
            yield entry.clone();

            let grains: Vec<Grain> = mow(entry, &tablet.trunk, &tablet.branch).iter().filter(|grain| grain.leaf_value.is_some()).cloned().collect();

            let lines: Vec<Line> = grains.iter().map(|grain| {
                Line {
                    key: grain.clone().base_value.unwrap_or("".to_string()),
                    value: grain.clone().leaf_value.unwrap_or("".to_string())
                }
            }).collect();

            for line in lines.iter() {
                wtr.serialize(line).unwrap();
            }
        }
    }
}

async fn insert_record_stream<S: Stream<Item = Entry>>(
    input: S,
    path: PathBuf,
) -> impl Stream<Item = Entry> {
    let schema = select_schema(path.clone()).await;

    let mut strategy = vec![];

    stream! {
        for await query in input {
            strategy = plan_insert(schema.clone(), query.clone());

            let query_stream = stream! {
                yield query;
            };

            let mut stream: BoxStream<'static, Entry> = Box::pin(query_stream);

            for tablet in strategy.clone() {
                stream = Box::pin(insert_tablet(stream, path.clone(), tablet));
            }

            for await entry in stream {
                yield entry;
            }
        }

        for tablet in strategy {
            let filepath = path.join(&tablet.filename);


            match fs::metadata(filepath.clone()) {
                Err(_) => return,
                Ok(m) => if m.len() == 0 {
                    // remove file if it is empty
                    fs::remove_file(filepath).unwrap();
                } else {
                    sort_file(filepath).await;
                }
            }

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
