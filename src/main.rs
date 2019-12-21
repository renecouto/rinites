use std::ops::Deref;
use std::fs::{OpenOptions, File};
use std::io::{Write, Seek, BufRead};
use std::io::SeekFrom;
use std::io::BufReader;
use base64;
use std::sync::{Arc, Mutex};
use std::thread;
use std::net::{UdpSocket, SocketAddr};
use std::fmt::UpperExp;
use std::num::ParseIntError;
use std::sync::mpsc::{channel, Sender, Receiver};
use std::time::Duration;
use std::thread::JoinHandle;

mod udp_server;
mod shards;
use shards::{ ShardId, start_shard_workers };
use udp_server::*;


#[derive(Debug)]
pub struct Response(String);




use structopt::StructOpt;
use crate::shards::ShardDir;
use std::path::Path;

/// Rinites
#[derive(StructOpt, Debug)]
#[structopt(name = "basic")]
struct Opts {
    #[structopt(short, long)]
    mount_path: String,
}

// multi-threaded with udp server on a single thread

fn main() {
    let opts: Opts = Opts::from_args();
    let udp_server = UdpServer::default();


    let shard_dir = ShardDir { mount_dir: Path::new(    &opts.mount_path).to_path_buf() };


    let (task_tx, rx) = channel();

    let task_rx = Arc::new(Mutex::new(rx));

    let (response_tx, response_rx) = channel();

    let threads = start_shard_workers(shard_dir, task_rx, response_tx);

    loop {
        match udp_server.poll_request() {
            Some(Ok((Some(req), addr))) => {
                task_tx.send((req, addr));
            },
            Some(Err((s, addr))) => {
                udp_server.socket.send_to(s.as_bytes(), addr);
                dbg!(s);
            },
            _ => ()
        }

        if let Ok((response, addr)) = response_rx.try_recv() {
            udp_server.socket.send_to(response.0.as_bytes(), addr);
        }
    }

    for x in threads {
        x.join().unwrap();
    }

    println!("exited");

}
