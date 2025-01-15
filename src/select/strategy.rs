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
pub struct Tablet {
    pub filename: String,
}

pub fn plan_select_schema(query: Entry) -> Vec<Tablet> {
    vec![Tablet {
        filename: "_-_.csv".to_string(),
    }]
}

pub fn plan_select(schema: Schema, query: Entry) -> Vec<Tablet> {
    vec![Tablet {
        filename: "datum-actdate.csv".to_string(),
    }]
}
