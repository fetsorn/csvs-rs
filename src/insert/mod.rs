use crate::record::mow::mow;
use crate::schema::find_crown;
use crate::types::schema::Schema;
use crate::select::select_schema;
use crate::types::entry::Entry;
use crate::types::grain::Grain;
use crate::types::line::Line;
use crate::error::{Error, Result};
use async_stream::{try_stream, stream};
use futures_core::stream::{BoxStream, Stream};
use futures_util::pin_mut;
use futures_util::stream::StreamExt;
use serde::{Deserialize, Serialize};
use std::fs;
use std::fs::OpenOptions;
use std::fs::{rename, File};
use std::path::{Path, PathBuf};
use temp_dir::TempDir;
use text_file_sort::sort::Sort;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Tablet {
    pub filename: String,
    pub trunk: String,
    pub branch: String,
}

async fn sort_file(filepath: &Path) -> Result<()> {
    let temp_path = TempDir::new()?;

    let filename = match filepath.file_name() {
        None => return Err(Error::from_message("unexpected missing filename")),
        Some(s) => s
    };

    let output = temp_path.as_ref().join(filename);

    Sort::new(vec![filepath.to_path_buf()], output.to_path_buf()).sort();

    rename(output, filepath)?;

    Ok(())
}

fn plan_insert(schema: &Schema, query: &Entry) -> Result<Vec<Tablet>> {
    let crown = find_crown(&schema, &query.base);

    let tablets = crown.iter().try_fold(vec![], |with_branch, branch| {
        let node = match schema
            .0
            .get(branch) {
                None => return Err(Error::from_message("unexpected missing branch")),
                Some(vs) => vs
            };

        let tablets_new = node
            .trunks
             .0
            .iter()
            .map(|trunk| Tablet {
                filename: format!("{}-{}.csv", trunk, branch),
                trunk: trunk.to_owned(),
                branch: branch.to_owned(),
            })
            .collect();

        Ok([with_branch, tablets_new].concat())
    });

    Ok(tablets?)
}

fn insert_tablet<S: Stream<Item = Result<Entry>>>(
    input: S,
    path: PathBuf,
    tablet: Tablet,
) -> impl Stream<Item = Result<Entry>> {
    try_stream! {
        let filepath = path.join(&tablet.filename);

        // create file if it doesn't exist
        if fs::metadata(&filepath).is_err() {
            File::create(&filepath)?;
        }

        let file = OpenOptions::new()
            .append(true)
            .open(filepath)
            .expect("cannot open file");

        let mut wtr = csv::WriterBuilder::new()
            .has_headers(false)
            .from_writer(file);

        for await entry in input {
            let entry = entry?;

            // pass the query early on to start other tablet streams
            yield entry.clone();

            let grains = mow(&entry, &tablet.trunk, &tablet.branch);

            let lines: Vec<Line> = grains.iter().filter(|grain| grain.leaf_value.is_some()).map(|grain| {
                Line {
                    key: match &grain.base_value { None => String::from(""), Some(s) => s.to_owned() },
                    value: match &grain.leaf_value { None => String::from(""), Some(s) => s.to_owned() }
                }
            }).collect();

            for line in lines.iter() {
                wtr.serialize(line)?;
            }
        }
    }
}

async fn insert_record_stream<S: Stream<Item = Result<Entry>>>(
    input: S,
    path: PathBuf,
) -> impl Stream<Item = Result<Entry>> {
    try_stream! {
        let schema = select_schema(&path).await?;

        let mut strategy = vec![];

        for await query in input {
            let query = query?;

            strategy = plan_insert(&schema, &query)?;

            let query_stream = try_stream! {
                yield query;
            };

            let mut stream: BoxStream<'static, Result<Entry>> = Box::pin(query_stream);

            for tablet in &strategy {
                stream = Box::pin(insert_tablet(stream, path.clone(), tablet.clone()));
            }

            for await entry in stream {
                let entry = entry?;

                yield entry;
            }
        }

        for tablet in strategy {
            let filepath = path.join(&tablet.filename);


            match fs::metadata(&filepath) {
                Err(_) => return,
                Ok(m) => if m.len() == 0 {
                    // remove file if it is empty
                    fs::remove_file(filepath)?;
                } else {
                    sort_file(&filepath).await?;
                }
            }

        }
    }
}

pub async fn insert_record(path: PathBuf, query: Vec<Entry>) {
    let readable_stream = try_stream! {
        for q in query {
            yield q;
        }
    };

    let s = insert_record_stream(readable_stream, path).await;

    pin_mut!(s); // needed for iteration

    while let Some(entry) = s.next().await {
    }
}
