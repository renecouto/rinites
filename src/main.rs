use std::ops::Deref;
use std::fs::{OpenOptions, File};
use std::io::{Write, Seek, BufRead};
use std::io::SeekFrom;
use std::io::BufReader;
use base64;
use std::sync::{Arc, Mutex};
use std::thread;

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

fn build_record(data: &[u8]) -> Record {
    let mut line = base64::encode(&data);
    line.push_str("\n");
    Record(line.into_bytes())
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
                b.pop();
            }
//            dbg!(&b);
            let decoded = base64::decode(&b).unwrap();

            res.push(Record(decoded));
//            dbg!(&res);
            if res.len() > self.chunk_size {
                return Ok(res)
            }
        }
    }

}




fn main() -> std::io::Result<()> {
    let shard_id = ShardId("/home/pessoal/code/rinites/samples/1".to_string());
    let p_shard_id = Arc::new(Mutex::new(shard_id));

    let my_writer = ShardWriter::from_shard(p_shard_id.clone());

    let data = b"some new dataonly not so new!";


    my_writer.append(data)?;

//    let mut my_reader_1 = ShardReader { p_shard_id: ProtectedShardId(p_shard_id.clone()), offset: 0, chunk_size: 10 };
//    let mut my_reader_2 = ShardReader { p_shard_id: ProtectedShardId(p_shard_id.clone()), offset: 138, chunk_size: 10 };
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


//    loop {
//
//        let records = my_reader_1.read()?;
//        if records.len() == 0 {
//            println!("no more records remaining. offset was: {}", my_reader_1.offset);
//            return Ok(())
//        }
//        let text: Vec<&str> = records.iter().map(|r| std::str::from_utf8(&r.0).unwrap()).collect();
//        dbg!(text);
//
//    }

    Ok(())
}
