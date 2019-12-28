

use std::net::{SocketAddr, UdpSocket};
use std::num::ParseIntError;
use std::time::Duration;
use serde_derive::{Deserialize, Serialize};
#[derive(Serialize, Deserialize)]
pub enum ShardIteratorType {
    Latest,
    Oldest,
}

pub enum Request {
    GetShardIterator(ShardIteratorType),
    GetRecords(u64),
    PutRecords(Vec<u8>),
}

pub struct UdpServer {
    pub socket: UdpSocket,
}

impl UdpServer {
    pub fn default() -> Self {
        let socket = UdpSocket::bind("127.0.0.1:3400").expect("couldn't bind to address");
        socket
            .set_read_timeout(Some(Duration::from_millis(10)))
            .expect("couldn't set read timeout");
        UdpServer { socket }
    }

    pub fn poll_request(
        &self,
    ) -> Option<Result<(Option<Request>, SocketAddr), (String, SocketAddr)>> {
        let mut buf = [0; 500];
        let recv = self.socket.recv_from(&mut buf);

        if let Err(_e) = recv {
            return None;
        }

        let (number_of_bytes, src_addr) = recv.unwrap();

        let mut result = move || {
            let filled_buf = &mut buf[..number_of_bytes];

            let request = std::str::from_utf8(filled_buf).map_err(|x| (x.to_string(), src_addr))?;
            let request_split: Vec<&str> = request.split_whitespace().collect();
            match request_split[..] {
                ["GetShardIterator", "Latest"] => Ok((
                    Some(Request::GetShardIterator(ShardIteratorType::Latest)),
                    src_addr,
                )),
                ["GetShardIterator", "Oldest"] => Ok((
                    Some(Request::GetShardIterator(ShardIteratorType::Oldest)),
                    src_addr,
                )),
                ["GetRecords", x] => {
                    let shit: u64 = x
                        .parse()
                        .map_err(|err: ParseIntError| (err.to_string(), src_addr))?;
                    Ok((Some(Request::GetRecords(shit)), src_addr))
                }
                ["PutRecords", base64_stuff] => match base64::decode(base64_stuff) {
                    Err(_e) => Err((
                        "invalid data. It must be base64 encoded".to_string(),
                        src_addr,
                    )),
                    Ok(_) => {
                        let records: Vec<u8> = base64_stuff.as_bytes().to_vec();
                        Ok((Some(Request::PutRecords(records)), src_addr))
                    }
                },
                [] => Ok((None, src_addr)),
                _ => Err(("invalid request".to_string(), src_addr)),
            }
        };

        Some(result())
    }
}
