use clap::{Parser, Subcommand};
mod insert;
mod delete;
mod select;
mod update;
mod types;
mod record;
mod schema;
mod test;
use insert::insert_record;
use std::env;

#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[arg(short, long)]
    path: Option<String>,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    Insert {
        #[arg(short, long)]
        query: String,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let path = match cli.path {
        Some(p) => std::path::Path::new(&p).to_owned(),
        None => env::current_dir().unwrap(),
    };

    println!("Hello {}!", path.display());

    match &cli.command {
        Some(Commands::Insert { query }) => {
            let query_json: serde_json::Value = serde_json::from_str(query).unwrap();

            let query_record: types::entry::Entry = query_json.try_into().unwrap();

            let entries = insert_record(path, vec![query_record]).await;

            for entry in entries.iter() {
                println!("Hello {}!", entry);
            }
        }
        None => (),
    }
}
