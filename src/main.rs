#![allow(warnings)]
use clap::{Parser, Subcommand};
mod delete;
mod insert;
mod record;
mod schema;
mod select;
mod test;
mod types;
mod update;
mod create;
use insert::insert_record;
use select::select_record;
use update::update_record;
use delete::delete_record;
use create::create_dataset;
use std::env;

/// A command-line utility for comma separated value store datasets
#[derive(Parser)]
#[command(version, arg_required_else_help = true)]
struct Cli {
    /// Path to the dataset
    #[arg(short, long)]
    path: Option<String>,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Find entries that match query
    Select {
        /// A json string in query object notation
        #[arg(short, long)]
        query: String,
    },
    /// Delete entries that match query
    Delete {
        /// A json string in query object notation
        #[arg(short, long)]
        query: String,
    },
    /// Update an entry from query
    Update {
        /// A json string in query object notation
        #[arg(short, long)]
        query: String,
    },
    /// Add an entry from query
    Insert {
        /// A json string in query object notation
        #[arg(short, long)]
        query: String,
    },
    /// Create a new dataset
    Create {
        /// Name of the dataset directory
        #[arg(short, long)]
        name: String,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let path = match cli.path {
        Some(p) => std::path::Path::new(&p).to_owned(),
        None => env::current_dir().unwrap(),
    };

    // println!("Hello {}!", path.display());

    match &cli.command {
        Some(Commands::Select { query }) => {
            let query_json: serde_json::Value = serde_json::from_str(query).unwrap();

            let query_record: types::entry::Entry = query_json.try_into().unwrap();

            let entries = select_record(path, vec![query_record]).await;

            for entry in entries.iter() {
                println!("{}", entry);
            }
        },
        Some(Commands::Delete { query }) => {
            let query_json: serde_json::Value = serde_json::from_str(query).unwrap();

            let query_record: types::entry::Entry = query_json.try_into().unwrap();

            let entries = delete_record(path, vec![query_record]).await;

            for entry in entries.iter() {
                // println!("{}", entry);
            }
        },
        Some(Commands::Update { query }) => {
            let query_json: serde_json::Value = serde_json::from_str(query).unwrap();

            let query_record: types::entry::Entry = query_json.try_into().unwrap();

            let entries = update_record(path, vec![query_record]).await;

            for entry in entries.iter() {
                // println!("{}", entry);
            }
        },
        Some(Commands::Insert { query }) => {
            let query_json: serde_json::Value = serde_json::from_str(query).unwrap();

            let query_record: types::entry::Entry = query_json.try_into().unwrap();

            let entries = insert_record(path, vec![query_record]).await;

            for entry in entries.iter() {
                // println!("{}", entry);
            }
        },
        Some(Commands::Create { name }) => {
            create_dataset(path, name);
        },
        None => {
            // show help
        },
    }
}
