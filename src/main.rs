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

#[derive(Debug)]
struct Record(Vec<u8>);

#[derive(Debug, Clone)]
struct ShardId(String);


struct ShardWriter {
    pub p_shard_id: ProtectedShardId,
//    pub partition_id: String
}

// We use an Arc to distribute the references with other threads, we use a mutex to make it so only one thread ever has acces to the file each time
type ProtectedShardId = Arc<Mutex<ShardId>>;


// This is very strange, since it can be thought of as the shard iterator a client uses for a request
struct ShardReader {
    pub p_shard_id: ProtectedShardId,
    pub offset: u64,
    pub chunk_size: usize
}

fn build_record(base64_data: &[u8]) -> Record {
    let mut res = Vec::with_capacity(base64_data.len() + 1);
    base64_data.iter().for_each(|b| res.push(b.clone()));
//    res.clone_from_slice(&base64_data);
    res.push(b'\n');
//    let mut line = std::str::from_utf8(base64_data);
//    line.push_str("\n");
    Record(res)
}

impl ShardWriter {

    fn append(&self, data: &[u8]) -> std::io::Result<()> {

        let shard_id = &self.p_shard_id.lock().unwrap();

        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .append(true)
            .open(&shard_id.0).unwrap();
        let record = build_record(data);
        file.write(&record.0)?;


        Ok(())
    }

    fn from_shard(shard_id: ProtectedShardId) -> Self {
        ShardWriter { p_shard_id: shard_id }
    }
}

impl ShardReader {
    fn read(&mut self) -> std::io::Result<Vec<Record>>  {
        let shard_id = &self.p_shard_id.lock().unwrap();

        let f = File::open(&shard_id.0)?;
        let mut reader = BufReader::new(f);
        reader.seek(SeekFrom::Start(self.offset));

        let mut res = Vec::new();
        loop {
            let mut b = String::new();
            let added_offset = reader.read_line(&mut b)?;
            if added_offset == 0 {
                return Ok(res)
            }
            self.offset += added_offset as u64;

            if b.ends_with('\n') {
                println!("removed trailing newline");
                b.pop();
            }
//            dbg!(&b);

            res.push(Record(b.as_bytes().to_vec()));
//            dbg!(&res);
            if res.len() > self.chunk_size {
                return Ok(res)
            }
        }
    }

    fn get_end_offset(&self) -> u64 {
        let shard_id = &self.p_shard_id.lock().unwrap();

        let f = File::open(&shard_id.0).unwrap();
        let mut reader = BufReader::new(f);
        reader.seek(SeekFrom::End(0)).unwrap()
    }

}
enum ShardIteratorType {
    Latest,
    Oldest
}


enum Request {
    GetShardIterator(ShardIteratorType),
    GetRecords(u64),
    PutRecords(Vec<u8>)
}

struct UdpServer {
    socket: UdpSocket
}

impl UdpServer {
    fn default() -> Self {
        let socket = UdpSocket::bind("127.0.0.1:3400").expect("couldn't bind to address");
        UdpServer { socket }
    }

    fn poll_request(&self) -> Result<(Option<Request>, SocketAddr), String> {
        let mut buf = [0; 500];
        let (number_of_bytes, src_addr) = self.socket.recv_from(&mut buf).expect("Didn't receive data");

        let filled_buf = &mut buf[..number_of_bytes];

        let request = std::str::from_utf8(filled_buf).map_err(|x| x.to_string())?;
        let mut request_split: Vec<&str> = request.split_whitespace().collect();
        match request_split[..] {
            ["GetShardIterator", "Latest"] => Ok((Some(Request::GetShardIterator(ShardIteratorType::Latest)), src_addr)),
            ["GetShardIterator", "Oldest"] => Ok((Some(Request::GetShardIterator(ShardIteratorType::Oldest)), src_addr)),
            ["GetRecords", x] => {
                let shit: u64  = x.parse().map_err(|err: ParseIntError| err.to_string())?;
                Ok((Some(Request::GetRecords(shit)), src_addr))
            },
            ["PutRecords", base64_stuff] => {
                match base64::decode(base64_stuff) {
                    Err(e) => Err("invalid data. It must be base64 encoded".to_string()),
                    Ok(_) => {
                        let records: Vec<u8>   = base64_stuff.as_bytes().to_vec();
                        Ok((Some(Request::PutRecords(records)), src_addr))
                    }
                }

            },
            [] => Ok((None, src_addr)),
            _ => Err("invalid request".to_string())
        }
    }
}

#[derive(Debug)]
struct Response(String);

fn handle_request(p_shard_id: ProtectedShardId, request: Request) -> Result<Response, String> {
    match request {
        Request::GetRecords(shit) => {
            let mut reader: ShardReader = ShardReader { p_shard_id, offset: shit, chunk_size: 10 };
            let records: Vec<Record> = reader.read().unwrap();
            println!("read {} records", records.len());

            if records.len() == 0 {
                Ok(Response(format!("no more records remaining. offset was: {}", reader.offset)))
            } else {
                let text: Vec<&str> = records.iter().map(|r| std::str::from_utf8(&r.0).unwrap()).collect();
                Ok(Response(format!("next shard iterator: {}, records: {:?}",reader.offset, text)))
            }

        },
        Request::GetShardIterator(ShardIteratorType::Latest) => {
            let mut reader: ShardReader = ShardReader { p_shard_id, offset: 0, chunk_size: 10 };
            let shit = reader.get_end_offset();
            let result = Response(format!("shard iterator: {}", shit));
            Ok(result)
        },
        Request::GetShardIterator(ShardIteratorType::Oldest) => {
            let result = Response("shard iterator: 0".to_string());
            Ok(result)
        },
        Request::PutRecords(record) => {
            let mut writer = ShardWriter::from_shard(p_shard_id);
            writer.append(&record).map_err(|x| x.to_string()).map(|nothing| Response("data was written!!!".to_string()))
        }
    }
}

// single-thread with udp server
fn main3() {
    let udp_server = UdpServer::default();
    let shard_id = ShardId("/home/pessoal/code/rinites/samples/1".to_string());
    let p_shard_id = Arc::new(Mutex::new(shard_id));
    loop {
        match udp_server.poll_request() {
            Ok((Some(req), addr)) => {
                let response = handle_request(p_shard_id.clone(), req).unwrap();
                dbg!(&response);

                udp_server.socket.send_to(response.0.as_bytes(), addr);
            },
            Err(s) => {dbg!(s);},
            _ => ()
        }
    }
}


// multi-threaded with udp server

fn main() {
    let udp_server = Arc::new(Mutex::new(UdpServer::default()));
    let shard_id = ShardId("/home/pessoal/code/rinites/samples/1".to_string());
    let p_shard_id = Arc::new(Mutex::new(shard_id));
    let mut threads = Vec::new();
    for n in 0..8 {
        let udp_server = udp_server.clone();
        let p_shard_id = p_shard_id.clone();
        let x = thread::spawn(move || {
            println!("started thread {}", n);

            loop {
                let res = {
                    let udp_s = udp_server.lock().unwrap();
                    println!("got lock of udp server on thread {}", n);
                    udp_s.poll_request()
                };
                match res {
                    Ok((Some(req), addr)) => {

                        let response = handle_request(p_shard_id.clone(), req).unwrap();
                        dbg!(&response);
                        println!("waiting for udp lock on thead {}", n);
                        let udp_s = udp_server.lock().unwrap();
                        println!("got udp lock on thead {}", n);
                        udp_s.socket.send_to(response.0.as_bytes(), addr);
                    },
                    Err(s) => { dbg!(s); },
                    _ => ()
                }
            }
        });
        threads.push(x);
    }

    for x in threads {
        x.join().unwrap();
    }

    println!("exited");

}

// threaded POC
fn main2() -> std::io::Result<()> {
    let UdpServer = UdpServer::default();
    let shard_id = ShardId("/home/pessoal/code/rinites/samples/1".to_string());
    let p_shard_id = Arc::new(Mutex::new(shard_id));

    let my_writer = ShardWriter::from_shard(p_shard_id.clone());

    let data = b"some new dataonly not so new!";


    my_writer.append(data)?;

    let mut threads = Vec::new();
    for n in 0..5 {
        let mut reader: ShardReader = ShardReader { p_shard_id: p_shard_id.clone(), offset: 0, chunk_size: 10 };
        println!("instantiated reader in thread {}", n);
        let x = thread::spawn(move || {
            println!("started thread {}", n);
            loop {
                let records: Vec<Record> = reader.read().unwrap();
                println!("read {} records in thread {}", records.len(), n);

                if records.len() == 0 {
                    println!("no more records remaining. offset was: {}", reader.offset);
                    break

                }
                let text: Vec<&str> = records.iter().map(|r| std::str::from_utf8(&r.0).unwrap()).collect();
                dbg!(text);
            }

        });

        threads.push(x);

    }

    for x in threads {
        x.join().unwrap();
    }

    Ok(())
}
