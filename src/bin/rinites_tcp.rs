use tokio::io;
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
    let socket = TcpListener::bind(&addr)?;
    println!("Listening on: {}", &addr);

    let done =
        socket
            .incoming()
//            .map_err(|x| dbg!(x))
            .for_each(move |socket| {
                let (reader, writer) = socket.split();
                let amt = io::copy(reader, writer);

                // After our copy operation is complete we just print out some helpful
                // information.
                let msg = amt.then(move |result| {
                    match result {
                        Ok((amt, _, _)) => println!("wrote {} bytes", amt),
                        Err(e) => println!("error: {}", e),
                    }

                    Ok(())
                });

                // And this is where much of the magic of this server happens. We
                // crucially want all clients to make progress concurrently, rather than
                // blocking one on completion of another. To achieve this we use the
                // `tokio::spawn` function to execute the work in the background.
                //
                // This function will transfer ownership of the future (`msg` in this
                // case) to the Tokio runtime thread pool that. The thread pool will
                // drive the future to completion.
                //
                // Essentially here we're executing a new task to run concurrently,
                // which will allow all of our clients to be processed concurrently.
                tokio::spawn(msg)
            });

    tokio::run(done);

    Ok(())
}