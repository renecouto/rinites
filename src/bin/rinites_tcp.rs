
use tokio::net::TcpListener;
use tokio::prelude::*;
use structopt::StructOpt;
use std::net::SocketAddr;
/// Rinites
#[derive(StructOpt, Debug)]
#[structopt(name = "basic")]
struct Opts {
    #[structopt(short, long)]
    mount_path: String,

    #[structopt(short, long, default_value = "127.0.0.1")]
    host: String,
    #[structopt(short, long)]
    port: u16,
}

fn handle_localhost(opts: & mut Opts) {
    if opts.host == "localhost" { opts.host = "127.0.0.1".to_string() }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut opts = Opts::from_args();
    handle_localhost(& mut opts);
    let addr = format!("{}:{}", &opts.host, opts.port).parse::<SocketAddr>()?;
    let _socket = TcpListener::bind(&addr)?;
    println!("Listening on: {}", &addr);


    Ok(())
}