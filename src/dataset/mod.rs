mod create;
mod delete;
mod insert;
mod select;
mod update;
use crate::{Entry, Result, Schema};
use futures_core::stream::Stream;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Dataset {
    dir: PathBuf,
}

impl Dataset {
    pub fn new(dir: &PathBuf) -> Self {
        Dataset { dir: dir.clone() }
    }

    pub fn create(&self, name: &str) -> Result<()> {
        create::create_dataset(self, name);

        Ok(())
    }

    pub async fn delete_record(self, query: Vec<Entry>) -> Result<()> {
        delete::delete_record(self, query).await?;

        Ok(())
    }

    pub fn delete_record_stream<S>(self, input: S) -> impl Stream<Item = Result<Entry>>
    where
        S: Stream<Item = Result<Entry>>,
    {
        delete::delete_record_stream(self, input)
    }

    pub async fn insert_record(self, query: Vec<Entry>) -> Result<()> {
        insert::insert_record(self, query).await?;

        Ok(())
    }

    pub fn insert_record_stream<S>(self, input: S) -> impl Stream<Item = Result<Entry>>
    where
        S: Stream<Item = Result<Entry>>,
    {
        insert::insert_record_stream(self, input)
    }

    pub async fn select_record(self, query: Vec<Entry>) -> Result<Vec<Entry>> {
        select::select_record(self, query).await
    }

    pub fn select_record_stream<S>(self, input: S) -> impl Stream<Item = Result<Entry>>
    where
        S: Stream<Item = Result<Entry>>,
    {
        select::select_record_stream(self, input)
    }

    pub async fn select_schema(self) -> Result<Schema> {
        select::select_schema(self).await
    }

    pub fn select_schema_stream<S>(self, input: S) -> impl Stream<Item = Result<Entry>>
    where
        S: Stream<Item = Result<Entry>>
    {
        select::select_schema_stream(self, input)
    }

    pub async fn update_record(self, query: Vec<Entry>) -> Result<()> {
        update::update_record(self, query).await
    }

    pub fn update_record_stream<S>(self, input: S) -> impl Stream<Item = Result<Entry>>
    where
        S: Stream<Item = Result<Entry>>,
    {
        update::update_record_stream(self, input)
    }

    async fn print_record(self, query: Vec<Entry>) -> Result<()> {
        select::print_record(self, query).await
    }
}
