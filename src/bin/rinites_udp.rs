use base64;
use std::fmt::UpperExp;
use std::fs::{File, OpenOptions};
use std::io::BufReader;
use std::io::SeekFrom;
use std::io::{BufRead, Seek, Write};
use std::net::{SocketAddr, UdpSocket};
use std::num::ParseIntError;
use std::ops::Deref;
use std::path::Path;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;

use structopt::StructOpt;

use rinites::shards::{start_shard_workers, SegmentId, ShardDir};
use rinites::udp_server::*;
use rinites::Response;

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

    let shard_dir = ShardDir {
        mount_dir: Path::new(&opts.mount_path).to_path_buf(),
    };

    let (task_tx, rx) = channel();

    let task_rx = Arc::new(Mutex::new(rx));

    let (response_tx, response_rx) = channel();

    let threads = start_shard_workers(shard_dir, task_rx, response_tx);

    loop {
        match udp_server.poll_request() {
            Some(Ok((Some(req), addr))) => {
                task_tx.send((req, addr));
            }
            Some(Err((s, addr))) => {
                udp_server.socket.send_to(s.as_bytes(), addr);
                dbg!(s);
            }
            _ => (),
        }

        if let Ok((response, addr)) = response_rx.try_recv() {
            udp_server.socket.send_to(response.0.as_bytes(), addr);
        }
    }
}
