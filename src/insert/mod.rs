use crate::types::entry::Entry;
use crate::schema::find_crown;
use temp_dir::TempDir;
use std::fs::{File, rename};
use std::cmp;
use text_file_sort::sort::Sort;
use crate::types::grain::Grain;
use crate::types::line::Line;
use crate::schema::{Leaves, Schema, Trunks};
use crate::select::select_schema;
use crate::record::mow::mow;
use async_stream::stream;
use futures_core::stream::{Stream, BoxStream};
use futures_util::pin_mut;
use futures_util::stream::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::fs::OpenOptions;
use std::fs;
use std::io::Write;

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
        let tablets_new = schema.0.get(branch).unwrap().0.0.iter().map(|trunk| {
            Tablet {
                filename: format!("{}-{}.csv", trunk, branch),
                trunk: trunk.clone(),
                branch: branch.clone(),
            }
        }).collect();

        vec![with_branch, tablets_new].concat()
    });

    tablets
}

fn passthrough_tablet <S: Stream<Item = Entry>>(input: S) -> impl Stream<Item = Entry> {
    stream! {
        for await entry in input {
            yield entry
        }
    }
}

fn insert_tablet<S: Stream<Item = Entry>>(input: S, path: PathBuf, tablet: Tablet) -> impl Stream<Item = Entry> {
    let filepath = path.join(&tablet.filename);

    println!("{}", filepath.clone().display());

    // create file if it doesn't exist
    if fs::metadata(filepath.clone()).is_err() { File::create(filepath.clone()).unwrap(); }

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
                wtr.serialize(line);
            }
        }
    }
}

async fn insert_record_stream<S: Stream<Item = Entry>>(input: S, path: PathBuf) -> impl Stream<Item = Entry> {
    let schema = select_schema(path.clone()).await;

    let mut strategy = vec![];

    stream! {
        for await query in input {
            strategy = plan_insert(schema.clone(), query.clone());

            // let streams: Vec<&mut dyn Stream<Item = Entry>> = strategy.iter().map(|tablet| insert_tablet(tablet)).collect();

            // let query_stream = stream! {
            //     yield query;
            // };

            // boilerplate
            // let insert_stream = insert_tablet(query_stream, path.clone(), strategy[0].clone());
            // let insert_stream_1 = insert_tablet(insert_stream, path.clone(), strategy[1].clone());

            // streams can't Vec
            //let insert_streams = strategy.iter().fold(vec![], |with_stream, tablet| {
            //    let insert_stream = if with_stream.len() == 0 {
            //        insert_tablet(query_stream, path.clone(), tablet.clone())
            //    } else {
            //        insert_tablet(with_stream, path.clone(), tablet.clone())
            //    };

            //    vec![with_stream, vec![insert_stream]].concat()
            //});

            // query_stream and insert_tablet are different types
            // let insert_streams = strategy.iter().fold(query_stream, |with_stream, tablet| {
            //     insert_tablet(with_stream, path.clone(), tablet.clone())
            // });

            // query_stream and passthrough are different types
            // let passthrough = |s| { stream! {
            //     for await entry in input {
            //         yield entry
            //     }
            // }};

            // let insert_streams = strategy.iter().fold(passthrough(query_stream), |with_stream, tablet| {
            //     insert_tablet(with_stream, path.clone(), tablet.clone())
            // });

            // even two calls to insert_tablet are different types
            match strategy.len() {
                0 => { yield query; },
                1 => {
                    let query_stream = stream! {
                        yield query;
                    };

                    let insert_stream = insert_tablet(query_stream, path.clone(), strategy[0].clone());

                    for await entry in insert_stream {
                        yield entry;
                    }
                },
                _ => {
                    let query_stream = stream! {
                        yield query;
                    };

                    // https://users.rust-lang.org/t/working-with-streams-combine-a-vector-of-streams-into-a-single-stream-of-a-custom-type/79244/4
                    let mut stream: BoxStream<'static, Entry> = Box::pin(query_stream);

                    for tablet in strategy.clone() {
                        let insert_stream = insert_tablet(stream, path.clone(), tablet);

                        stream = Box::pin(insert_stream);
                    }
                    // let insert_stream_first = insert_tablet(stream, path.clone(), strategy[0].clone());

                    // let insert_stream = insert_tablet(insert_stream_first, path.clone(), strategy[1].clone());

                    // let mut stream: BoxStream<'static, Entry> = Box::pin(insert_stream);

                    // for i in 2..strategy.len()-1 {
                    //     let insert_stream = insert_tablet(BoxStream::into_inner(stream), path.clone(), strategy[i].clone());
                    // }

                    for await entry in stream {
                        yield entry;
                    }
                }
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
