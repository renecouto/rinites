use std::net::UdpSocket;
use std::time::SystemTime;

fn main() {
    let socket = UdpSocket::bind("127.0.0.1:3401").expect("couldn't bind to address");

    let buf = base64::encode("meucumeucumeucumeucumeucumeucumeucumeucumeucumeucu".as_bytes());
    let buf = "PutRecords ".to_string() + &buf;
    let now = SystemTime::now();
//        .duration_since(SystemTime::UNIX_EPOCH).expect("se fodeu")
    let n = 10000000;
    let s = buf.len();
    for _ in 0..n {
        let buf = buf.as_bytes();
        socket.send_to(buf, "127.0.0.1:3400");
    }
    let end = now.elapsed().expect("se fodeu 2").as_millis();
    println!("it took {} ms to send {} bytes per request in {} request, totaling {} bytes and {} bytes/sec", end, s, n, n*s, n*s/(end as usize / 1000));



}