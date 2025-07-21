use crate::{Branch, Leaves, Schema, Trunks, Line, Entry, Error, Result, Dataset};
use async_stream::{stream, try_stream};
use futures_core::stream::Stream;
use futures_util::pin_mut;
use futures_util::stream::StreamExt;
use serde::{Deserialize, Serialize};
use std::fs;
use std::fs::File;
use std::io::prelude::*;
use std::path::{Path, PathBuf};
use temp_dir::TempDir;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Tablet {
    pub filename: String,
    pub trait_: String,
    pub trait_is_first: bool,
}

fn plan_delete(schema: &Schema, query: &Entry) -> Result<Vec<Tablet>> {
    let (trunks, leaves) = match schema.0.get(&query.base) {
        None => (vec![], vec![]),
        Some(Branch {
            trunks: Trunks(ts),
            leaves: Leaves(ls),
        }) => (ts.to_vec(), ls.to_vec()),
    };

    let base_value = match &query.base_value {
        None => return Err(Error::from_message("unexpected missing option")),
        Some(v) => v,
    };

    let trunk_tablets: Vec<Tablet> = trunks
        .iter()
        .map(|trunk| Tablet {
            filename: format!("{}-{}.csv", trunk, query.base),
            trait_: base_value.to_owned(),
            trait_is_first: false,
        })
        .collect();

    let leaf_tablets = leaves
        .iter()
        .map(|leaf| Tablet {
            filename: format!("{}-{}.csv", query.base, leaf),
            trait_: base_value.to_owned(),
            trait_is_first: true,
        })
        .collect();

    Ok([trunk_tablets, leaf_tablets].concat())
}

async fn delete_tablet(path: &Path, tablet: Tablet) -> Result<()> {
    let filepath = path.join(&tablet.filename);

    match fs::metadata(&filepath) {
        Err(_) => return Ok(()),
        Ok(m) => {
            if m.len() == 0 {
                return Ok(());
            }
        }
    }

    let file = File::open(&filepath)?;

    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(file);

    let temp_path = TempDir::new()?;

    let filename = match filepath.file_name() {
        None => return Err(Error::from_message("unexpected missing filename")),
        Some(s) => s,
    };

    let output = temp_path.as_ref().join(filename);

    let temp_file = File::create(&output)?;

    let mut wtr = csv::WriterBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_writer(temp_file);

    for result in rdr.records() {
        let record = result?;

        let line = Line {
            key: match record.get(0) {
                None => String::from(""),
                Some(s) => s.to_owned(),
            },
            value: match record.get(1) {
                None => String::from(""),
                Some(s) => s.to_owned(),
            },
        };

        let trait_ = if tablet.trait_is_first {
            line.key.to_owned()
        } else {
            line.value.to_owned()
        };

        let is_match = trait_ == tablet.trait_;

        if !is_match {
            wtr.serialize(line)?;
        }
    }

    wtr.flush()?;

    // if empty
    match fs::metadata(&output) {
        Err(_) => fs::remove_file(filepath)?,
        Ok(m) => {
            if m.len() == 0 {
                fs::remove_file(filepath)?;
            } else {
                fs::copy(output, filepath)?;
            }
        }
    }

    Ok(())
}

pub async fn delete_record_stream<S: Stream<Item = Result<Entry>>>(
    dataset: &Dataset,
    input: S,
) -> impl Stream<Item = Result<Entry>> {
    try_stream! {
        let schema = dataset.select_schema().await?;

        for await query in input {
            let query = query?;

            let strategy = plan_delete(&schema, &query)?;

            for tablet in strategy {
                delete_tablet(&path, tablet).await?;
            }

            yield query;
        }
    }
}

pub async fn delete_record(dataset: Dataset, query: Vec<Entry>) -> Result<()> {
    let readable_stream = try_stream! {
        for q in query {
            yield q;
        }
    };

    let s = dataset.delete_record_stream(readable_stream).await;

    pin_mut!(s); // needed for iteration

    while let Some(entry) = s.next().await {}

    Ok(())
}
