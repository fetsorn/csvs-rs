use crate::{Branch, Entry, Error, Leaves, line::Line, Result, Schema, Trunks, Dataset};
use async_stream::{stream, try_stream};
use futures_core::stream::{BoxStream, Stream};
use futures_util::pin_mut;
use futures_util::stream::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::path::PathBuf;
use temp_dir::TempDir;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Tablet {
    pub filename: String,
    pub trunk: String,
    pub branch: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct State {
    fst: Option<String>,
    is_match: bool,
}

fn plan_update(schema: &Schema, query: &Entry) -> Vec<Tablet> {
    let is_schema = query.base == "_";

    if is_schema {
        return vec![Tablet {
            filename: "_-_.csv".to_owned(),
            trunk: "_".to_owned(),
            branch: "_".to_owned(),
        }];
    }

    let crown = schema.find_crown(&query.base);

    let tablets = crown.iter().fold(vec![], |with_branch, branch| {
        let trunks = match &schema.0.get(branch) {
            None => vec![],
            Some(Branch {
                trunks: Trunks(ts), ..
            }) => ts.to_vec(),
        };

        let tablets_new = trunks
            .iter()
            .map(|trunk| Tablet {
                filename: format!("{}-{}.csv", trunk, branch),
                trunk: trunk.to_owned(),
                branch: branch.to_owned(),
            })
            .collect();

        [with_branch, tablets_new].concat()
    });

    tablets
}

fn update_schema_line_stream(entry: Entry) -> impl Stream<Item = Result<Line>> {
    try_stream! {
        let mut keys: Vec<String> = entry.leaves.keys().cloned().collect();

        keys.sort();

        for trunk in keys {
            let mut leaves: Vec<Entry> = match entry.leaves.get(&trunk) {
                None => vec![],
                Some(es) => es.clone()
            };

            leaves.sort_by(|a, b| a.base.cmp(&b.base).then(
                a.base_value.cmp(&b.base_value)
            ));

            for leaf in leaves {
                yield Line {
                    key: trunk.to_owned(),
                    value: match leaf.base_value { None => "".to_owned(), Some(s) => s }
                }
            }
        }
    }
}

fn update_line_stream<S: Stream<Item = Result<Line>>>(
    input: S,
    entry: Entry,
    tablet: Tablet,
) -> impl Stream<Item = Result<Line>> {
    let grains = entry.mow(&tablet.trunk, &tablet.branch);

    let mut keys: Vec<String> = grains
        .iter()
        .map(|grain| match &grain.base_value {
            None => "".to_owned(),
            Some(s) => s.to_owned(),
        })
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect::<Vec<String>>();

    keys.sort();

    let values: HashMap<String, Vec<String>> =
        grains.iter().fold(HashMap::new(), |with_grain, grain| {
            let key = match &grain.base_value {
                None => "",
                Some(s) => s,
            };

            if grain.leaf_value.is_none() {
                return with_grain;
            }

            let value = match &grain.leaf_value {
                None => "",
                Some(s) => s,
            };

            let values_old: Vec<String> = match with_grain.get(key) {
                None => vec![],
                Some(vs) => vs.to_vec(),
            };

            let mut values_new = [&values_old[..], &[value.to_owned()]].concat();

            values_new.sort();

            let mut with_grain_new = with_grain;

            with_grain_new.insert(key.to_owned(), values_new);

            with_grain_new
        });

    let mut state_intermediary = State {
        fst: None,
        is_match: false,
    };

    try_stream! {
        for await line in input {
            let line = line?;

            let fst_is_new = state_intermediary.fst.is_none() || state_intermediary.fst.as_ref() != Some(&line.key);

            if state_intermediary.is_match && fst_is_new {
                match &state_intermediary.fst {
                    None => (),
                    Some(f) => match values.get(f) {
                        None => (),
                        Some(vs) => {
                            for value in vs {
                                yield Line {
                                    key: f.to_owned(),
                                    value: value.to_owned()
                                };
                            }
                        }
                    }
                }

                keys = keys.iter().filter(
                    |k| Some(k) != state_intermediary.fst.as_ref().as_ref()
                ).cloned().collect();
            }

            if fst_is_new {
                let keys_between: Vec<String> = keys.iter().filter(|key| {
                    let is_first: bool = state_intermediary.fst.is_none();

                    let is_after: bool = is_first || state_intermediary.fst.as_ref() <= Some(key);

                    let is_before: bool = **key < line.key;

                    let is_between: bool = is_after && is_before;

                    is_between
                }).cloned().collect();

                for key in keys_between {
                    match values.get(&key) {
                        None => (),
                        Some(vs) => {
                            for value in vs {
                                yield Line {
                                    key: key.to_owned(),
                                    value: value.to_owned()
                                };
                            }
                        }
                    }

                    keys = keys.iter().filter(|k| **k != key).cloned().collect();
                }
            }

            let is_match: bool = keys.contains(&line.key);

            if !is_match {
                yield line.clone();
            }

            state_intermediary = State {
                fst: Some(line.key),
                is_match
            }
        }

        for key in keys.clone() {
            match values.get(&key) {
                None => (),
                Some(vs) => {
                    for value in vs {
                        yield Line {
                            key: key.to_owned(),
                            value: value.to_owned()
                        };
                    }
                }
            }

            keys = keys.iter().filter(|k| **k != key).cloned().collect();
        }
    }
}

fn line_stream(filepath: PathBuf) -> impl Stream<Item = Result<Line>> {
    try_stream! {
        if fs::metadata(&filepath).is_err() { File::create(&filepath)?; }

        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .flexible(true)
            .from_reader(File::open(&filepath)?);

        for result in rdr.records() {
            let record = result?;

            let line = Line {
                key: match record.get(0) { None => String::from(""), Some(s) => s.to_owned() },
                value: match record.get(1) { None => String::from(""), Some(s) => s.to_owned() }
            };

            yield line
        }
    }
}

fn update_tablet<S: Stream<Item = Result<Entry>>>(
    path: PathBuf,
    tablet: Tablet,
    input: S,
) -> impl Stream<Item = Result<Entry>> {
    let filepath = path.join(&tablet.filename);

    let is_schema = tablet.filename == "_-_.csv";

    try_stream! {
        // must assign a variable to create the directory
        // must assign inside the stream scope to keep the directory
        let temp_d = TempDir::new()?;

        // wrap in result here for try_stream! proc to pick up error from ?
        let filename = match filepath.file_name() {
            None => Err(Error::from_message("unexpected missing filename")),
            Some(s) => Ok(s)
        }?;

        let temp_path = temp_d.as_ref().join(filename);

        File::create(&temp_path)?;

        let temp_file = OpenOptions::new()
            .append(true)
            .open(&temp_path)
            .expect("cannot open file");

        let mut wtr = csv::WriterBuilder::new()
            .has_headers(false)
            .from_writer(temp_file);

        for await entry in input {
            let entry = entry?;

            // pass the query early on to start other tablet streams
            yield entry.clone();

            // duplicate code instead of reusing a boxpinned update stream
            if is_schema {
                let update_stream = update_schema_line_stream(entry);

                pin_mut!(update_stream);

                for await line_new in update_stream {
                    let line_new = line_new?;

                    wtr.serialize(line_new)?;

                    wtr.flush()?;
                }
            } else {
                let update_stream = update_line_stream(line_stream(filepath.clone()), entry.clone(), tablet.clone());

                pin_mut!(update_stream);

                for await line_new in update_stream {
                    let line_new = line_new?;

                    wtr.serialize(line_new)?;

                    wtr.flush()?;
                }
            }
        }

        match fs::metadata(&temp_path) {
            Err(_) => (),
            Ok(m) => if m.len() == 0 {
                if fs::metadata(&filepath).is_ok() {
                    fs::remove_file(&filepath)?;
                }
            } else {
                // fs::rename fails with invalid cross-device link
                fs::copy(temp_path, &filepath)?;
            }
        }

        match fs::metadata(&filepath) {
            Err(_) => return,
            Ok(m) => if m.len() == 0 {
                fs::remove_file(filepath)?;
            }
        }

    }
}

pub fn update_record_stream<S: Stream<Item = Result<Entry>>>(
    dataset: Dataset,
    input: S,
) -> impl Stream<Item = Result<Entry>> {
    try_stream! {
        let schema = dataset.clone().select_schema().await?;

        for await query in input {
            let query = query?;

            let strategy = plan_update(&schema, &query);

            let query_stream = try_stream! {
                yield query;
            };

            let mut stream: BoxStream<'static, Result<Entry>> = Box::pin(query_stream);

            for tablet in strategy {
                stream = Box::pin(update_tablet(dataset.dir.clone(), tablet, stream));
            }

            for await entry in stream {
                let entry = entry?;

                yield entry;
            }
        }
    }
}

pub async fn update_record(dataset: Dataset, query: Vec<Entry>) -> Result<()> {
    let readable_stream = try_stream! {
        for q in query {
            yield q;
        }
    };

    let s = dataset.update_record_stream(readable_stream);

    pin_mut!(s); // needed for iteration

    while let Some(entry) = s.next().await {}

    Ok(())
}
