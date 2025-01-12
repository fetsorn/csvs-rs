use clap::Parser;
mod entry;
mod grain;
mod into_value;
mod mow;
mod schema;
mod sow;

#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    #[arg(short = 'i', long)]
    source_path: Option<String>,

    #[arg(short = 'o', long)]
    target_path: Option<String>,

    #[arg(short = 't', long, default_value = "json")]
    target_type: String,

    #[arg(short, long)]
    query: String,
}

fn main() {
    let args = Args::parse();

    println!("Hello {}!", args.query);
}
