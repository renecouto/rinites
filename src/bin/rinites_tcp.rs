use tokio::io;
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;
use tokio::prelude::stream::{SplitSink, SplitStream};
use structopt::StructOpt;
use std::net::SocketAddr;
use tokio::codec::{BytesCodec, Framed, LinesCodec};
use tokio::codec::Decoder;
use core::borrow::BorrowMut;
use bytes::{BytesMut, BufMut, BigEndian};
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

fn get_cli_opts() -> Opts {
    let mut opts = Opts::from_args();
    if opts.host == "localhost" {
        opts.host = "127.0.0.1".to_string()
    }
    opts
}

fn handle_socket(socket: TcpStream) -> impl Future<Item=(), Error=()> + 'static + Send {

    // Once we're inside this closure this represents an accepted client
//    // from our server. The `socket` is the client connection (similar to
//    // how the standard library operates).
//    //
//    // We're parsing each socket with the `BytesCodec` included in `tokio_io`,
//    // and then we `split` each codec into the reader/writer halves.
//    //
//    // See https://docs.rs/tokio-codec/0.1/src/tokio_codec/bytes_codec.rs.html
    type MyCodec = BytesCodec;
    let framed: Framed<TcpStream, MyCodec> = MyCodec::new().framed(socket); //
    let (writer, reader): (SplitSink<Framed<TcpStream, MyCodec>>, SplitStream<Framed<TcpStream, MyCodec>>) = framed.split();
//                reader.
//                let xxx = reader.and_then(|x|Ok(()));
//    reader.and_then(|b| println!(b));
//    let processor = writer.send_all(xxx)
//                    ;
//
    let processor = reader
//        .map(|z| Vec::from(z.as_bytes()))
        .concat2()
        .map(|bytes| {
            let bytes =  std::str::from_utf8(&bytes).expect("se fodeu");
            println!("bytes: {:?}", &bytes);
//            let mut kk: Vec<u8> = vec![1, 2, 3];
//            bytes.as_bytes().iter().for_each(|x| {
//                kk.push(*x);
//            } );
//            kk
//            Ok(());

            let mut buf = BytesMut::with_capacity(1024);
//            let processor = .as_bytes().to_owned();
            buf.put("HTTP/1.1 301 Moved Permanently\n");
            let kk = buf.freeze();
            writer.send(kk)
        })
        .then(|x| {
        println!("meucu");
        Ok(())
    } );

    processor
//        .and_then(|bytes| {
//            println!("bytes: {:?}", bytes);
//            Ok(())
//        })
//    writer.send()).then(|X|{
////            match result {
////                Ok(_) => {
////                    println!("Socket closed with result: {:?}", 1);
////                    Ok(())
////                },
////                Err(ref e) => {
//                    println!("Socket closed with result: {:?}", 2);
//                    Ok(());
//            ()
////                }
////            }
//
//        })
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opts = get_cli_opts();
    let addr = format!("{}:{}", &opts.host, opts.port).parse::<SocketAddr>()?;
    let socket = TcpListener::bind(&addr)?;
    println!("Listening on: {}", &addr);

    let done = socket
            .incoming()
            .map_err(|x| println!("{}", x))
            .for_each(move |socket| {
                let processor = handle_socket(socket);
                tokio::spawn(processor)
            });

    tokio::run(done);

    Ok(())
}