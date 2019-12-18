use std::sync::{Arc, Mutex};
use std::fs::{OpenOptions, File, DirEntry};
use std::io::{Write, Seek, BufRead};
use std::io::SeekFrom;
use std::io::BufReader;
use std::sync::mpsc::{Receiver, Sender};
use crate::udp_server::{Request, ShardIteratorType};
use std::net::SocketAddr;
use crate::Response;
use std::thread::JoinHandle;
use std::{thread, fs};

#[derive(Debug)]
pub struct Record(pub Vec<u8>);

#[derive(Debug, Clone)]
pub struct ShardId(pub String);


pub struct ShardWriter {
    pub latest_shard: ProtectedShardId,
    pub shard_dir: ShardDir,
    pub offset: u64,
    pub max_shard_size: u64
}

// We use an Arc to distribute the references with other threads, we use a mutex to make it so only one thread ever has acces to the file each time
pub type ProtectedShardId = Arc<Mutex<ShardId>>;


// This is very strange, since it can be thought of as the shard iterator a client uses for a request
pub struct ShardReader {
    pub shard_id: ShardId,
    pub offset: u64,
    pub chunk_size: usize,
    pub shard_dir: ShardDir
}

pub fn build_record(base64_data: &[u8]) -> Record {
    let mut res = Vec::with_capacity(base64_data.len() + 1);
    base64_data.iter().for_each(|b| res.push(b.clone()));
    res.push(b'\n');
    Record(res)
}

impl ShardWriter {

    pub fn append(&mut self, data: &[u8]) -> std::io::Result<()> {

        let mut shard_id = self.latest_shard.lock().unwrap();

        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .append(true)
            .open(format!("{}{}", self.shard_dir.mount_dir, &shard_id.0)).unwrap();
        let record = build_record(data);
        let written = file.write(&record.0)?;
        self.offset += written as u64;
        if self.offset > self.max_shard_size {
            println!("getting lock for new partition");
//            let mut shard_id = self.latest_shard.lock().unwrap();

            shard_id.0 = format!("{:04}", self.offset);
            self.offset = 0;
            println!("releasing lock for new partition");
        }
        Ok(())
    }


}

impl ShardReader {
    pub fn read(&mut self) -> std::io::Result<Vec<Record>>  {
        let shard_id = &self.shard_id;

        let path = format!("{}{}", self.shard_dir.mount_dir, &shard_id.0);

        dbg!(&path);

        let f = File::open(path)?;
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

            res.push(Record(b.as_bytes().to_vec()));

            if res.len() > self.chunk_size {
                return Ok(res)
            }
        }
    }

//    pub fn get_end_offset(&self) -> u64 {
//        let shard_id = &self.p_shard_id.lock().unwrap();
//
//        let f = File::open(&shard_id.0).unwrap();
//        let mut reader = BufReader::new(f);
//        reader.seek(SeekFrom::End(0)).unwrap()
//    }
}

#[derive(Clone, Debug)]
pub struct ShardDir {
    pub mount_dir: String
}

#[derive(Debug)]
enum Shard {
    Latest,
    Old((String, u64))
}

impl ShardDir {
    fn get_latest_shard(&self) -> ShardId {
        let paths = fs::read_dir(&self.mount_dir).unwrap();

        let path = paths
            .map(|p|
                p.unwrap()
                    .file_name()
                    .into_string()
                    .unwrap())
            .max();

        ShardId(path.unwrap_or("0000".to_string()))
    }

    fn get_oldest_shard(&self) -> ShardId {
        let paths = fs::read_dir(&self.mount_dir).unwrap();

        let path = paths.map(|p| p.unwrap().file_name().into_string().unwrap()).min();

        ShardId(path.unwrap_or("0000".to_string()))
    }

    fn find_belonging_shard(&self, shit: u64) -> Shard {

        let paths = fs::read_dir(&self.mount_dir).unwrap();
        dbg!(&paths);
        let mut candidate: (u64, i64) = (0, i64::max_value());
//        let mut closest: u64 =  u64::max_value();
        let mut max_n: u64 = 0;
//        let mut diff: i64 = 0;
        for p in paths.map(|x|x.unwrap()) {
            dbg!(&p);
            let n: i64 = p.file_name().into_string().unwrap().parse().unwrap();
            let diff = (shit as i64) - n;
            dbg!(diff);
            if diff >= 0 && diff <= candidate.1 {
                candidate = (n as u64, diff);
            }
            dbg!(candidate);

            max_n = (n as u64).max(max_n);
            dbg!(max_n);
        }

//        let res = if candidate.0 == max_n {
//            Shard::Latest
//        } else {
            Shard::Old((format!("{:04}", candidate.0), candidate.1 as u64))
//        };

//        println!("will return: {:?}", res);
//        res

    }

    pub fn get_end_offset(&self, shard_id: &ShardId) -> u64 {

        let f = File::open(format!("{}{}", &self.mount_dir,&shard_id.0)).unwrap();
        let mut reader = BufReader::new(f);
        reader.seek(SeekFrom::End(0)).unwrap()
    }

}

pub fn start_shard_workers(
    shard_dir: ShardDir,
    task_rx: Arc<Mutex<Receiver<(Request, SocketAddr)>>>,
    response_tx: Sender<(Response, SocketAddr)>
) -> Vec<JoinHandle<()>> {

    let latest_shard = shard_dir.get_latest_shard();
    let latest_shard_offset = shard_dir.get_end_offset(&latest_shard);

    let latest_shard: Arc<Mutex<ShardId>> = Arc::new(Mutex::new(latest_shard));
    let latest_shard_writer = Arc::new(Mutex::new(ShardWriter { latest_shard: latest_shard.clone(), shard_dir: shard_dir.clone(), offset: latest_shard_offset , max_shard_size: 100}));


    let mut threads = Vec::new();
    for n in 0..8 {
        let latest_shard_writer = latest_shard_writer.clone();
        let latest_shard = latest_shard.clone();

        let task_rx = task_rx.clone();
        let response_tx = response_tx.clone();
        let shard_dir = shard_dir.clone();
        let x = thread::spawn(move || {

            println!("started thread {}", n);
            loop {
                let (req, addr) = {
                    let rx = task_rx.lock().unwrap();
                    println!("got rx lock in thread {}", n);
                    rx.recv().unwrap()

                };

                println!("got req in thread {}", n);
                let response = handle_request(&latest_shard_writer, &latest_shard, &shard_dir, req).unwrap();
                dbg!(&response);
                println!("sending from thread {}", n);
                if let Err(e) = response_tx.send((response, addr)) {
                    println!("got err: {:?}", e);
                }

//                println!("waiting for udp lock on thead {}", n);
            }

        });
        threads.push(x);
    }
    threads
}


pub fn handle_request(latest_shard_writer: &Arc<Mutex<ShardWriter>>, latest_shard: &ProtectedShardId, shard_dir: &ShardDir ,request: Request) -> Result<Response, String> {
    match request {
        Request::GetRecords(shit) => {
            let shard_dir = shard_dir.clone();
            let (shard_id, offset): (ShardId, u64) = {
                match shard_dir.find_belonging_shard(shit) {
                    Shard::Latest => {
                        let shard_id = latest_shard.lock().unwrap();
                        let end_offset = shard_dir.get_end_offset(&shard_id);
                        dbg!(end_offset);
                        (shard_id.clone(), end_offset)
                    },
                    Shard::Old((shard_id, shard_offset)) => (ShardId(shard_id), shard_offset)
                }
            };
            // FIXME do maths to figure out offset
            let mut reader: ShardReader = ShardReader { shard_id: shard_id, offset: offset, chunk_size: 10, shard_dir: shard_dir.clone() };
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
            let shit = shard_dir.get_latest_shard();
//            let mut reader: ShardReader = ShardReader { p_shard_id, offset: 0, chunk_size: 10 };
//            let shit = reader.get_end_offset();
            let result = Response(format!("shard iterator: {}", shit.0));
            Ok(result)
        },
        Request::GetShardIterator(ShardIteratorType::Oldest) => {
            let shit = shard_dir.get_oldest_shard();
            let result = Response(format!("shard iterator: {}", shit.0));
            Ok(result)
        },
        Request::PutRecords(record) => {
            let mut writer = latest_shard_writer.lock().unwrap();
            writer.append(&record).map_err(|x| x.to_string()).map(|nothing| Response("data was written!!!".to_string()))
        }
    }
}