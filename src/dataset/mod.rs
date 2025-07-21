mod create;
mod delete;
mod insert;
mod select;
mod update;
use crate::{Entry, Result, Schema};
use futures_core::stream::Stream;
use std::path::PathBuf;

pub struct Dataset {
    dir: PathBuf,
}

impl Dataset {
    fn new(dir: &PathBuf) -> Self {
        Dataset { dir: dir.clone() }
    }

    fn create(&self, name: &str) -> Result<()> {
        create::create_dataset(self.dir, name);

        Ok(())
    }

    async fn delete(&self, query: Vec<Entry>) -> Result<()> {
        delete::delete_record(self.dir, query).await?;

        Ok(())
    }

    async fn insert(&self, query: Vec<Entry>) -> Result<()> {
        insert::insert_record(self.dir, query).await?;

        Ok(())
    }

    async fn select_record(&self, query: Vec<Entry>) -> Result<Vec<Entry>> {
        select::select_record(self.dir, query).await
    }

    fn select_record_stream<S>(&self, input: S) -> impl Stream<Item = Result<Entry>>
    where
        S: Stream<Item = Result<Entry>>,
    {
        select::select_record_stream(input, self.dir)
    }

    async fn select_schema(&self) -> Result<Schema> {
        select::select_schema(self.dir).await
    }

    fn select_schema_stream(&self, input: S) -> impl Stream<Item = Result<Entry>> {
        select::select_schema_stream(input, self.dir)
    }

    async fn update(&self, query: Vec<Entry>) -> Result<()> {
        update::update_record(self.dir, query).await
    }

    fn update_stream<S>(&self, input: S) -> impl Stream<Item = Result<Entry>>
    where
        S: Stream<Item = Result<Entry>>,
    {
        update::update_record_stream(input, self.dir)
    }

    async fn print_record(&self, query: Vec<Entry>) -> Result<()> {
        select::print_record(self.dir, query).await
    }
}
