#![allow(warnings)]
use clap::{Parser, Subcommand};
use csvs::{Entry, Error, Result, Dataset};
use serde_json::{from_str, Value};
mod test;
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
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let path = match cli.path {
        Some(p) => std::path::Path::new(&p).to_owned(),
        None => env::current_dir()?,
    };

    let dataset = Dataset::new(&path);

    // println!("Hello {}!", path.display());

    match &cli.command {
        Some(Commands::Select { query }) => {
            let query_json: Value = from_str(query)?;

            let query_record: Entry = query_json.try_into()?;

            dataset.print_record(vec![query_record]).await?
        }
        Some(Commands::Delete { query }) => {
            let query_json: Value = from_str(query)?;

            let query_record: Entry = query_json.try_into()?;

            dataset.delete_record(vec![query_record]).await;
        }
        Some(Commands::Update { query }) => {
            let query_json: Value = from_str(query)?;

            let query_record: Entry = query_json.try_into()?;

            dataset.update_record(vec![query_record]).await;
        }
        Some(Commands::Insert { query }) => {
            let query_json: Value = from_str(query)?;

            let query_record: Entry = query_json.try_into()?;

            dataset.insert_record(vec![query_record]).await;
        }
        Some(Commands::Create { name }) => {
            dataset.create(name);
        }
        None => {
            // show help
        }
    }

    Ok(())
}
