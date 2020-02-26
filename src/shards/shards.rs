use std::{fs, thread};
use std::fs::{DirEntry, File, OpenOptions};
use std::io::{BufRead, Seek, Write};
use std::io::BufReader;
use std::io::SeekFrom;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, Sender};
use std::thread::JoinHandle;

use serde_derive::{Deserialize, Serialize};

use crate::Response;

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


#[derive(Deserialize, Serialize)]
#[derive(Debug, Clone, PartialEq)]
pub struct Record(pub Vec<u8>);

impl Record {

    pub fn serialized(mut self) -> Vec<u8> {
        let mut res = self.0;

        res.push(b'\n');
        res
    }
    pub fn as_string(&self) -> String {
        std::str::from_utf8(&self.0).expect("data was corrupt").to_string()
    }

    pub fn from_string(s: String) -> Result<Record, failure::Error> {

        base64::decode(&s)?;
        Ok(Record(s.as_bytes().to_vec()))
    }

}

pub type SegmentId = u64;

type ShardOffset = u64;

pub trait ShaW {
    fn write(&mut self, record: Record) -> std::io::Result<()>;
}

impl ShaW for ShardWriter {
    fn write(& mut self, record: Record) -> std::io::Result<()> {
        self.writez(record)
    }
}

pub struct ShardWriter {
    pub latest_segment: u64,
    pub shard_dir: ShardDir,
    pub offset: ShardOffset,
    pub max_segment_size: u64,
}

impl ShardWriter {
    fn writez(&mut self, record: Record) -> std::io::Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .append(true)
            .open(
                self.shard_dir
                    .path_to_segment(self.latest_segment),
            )
            .expect("could not open file to append");

        let written = file.write(&record.serialized())?;
        self.offset += written as u64;
        if self.offset > self.max_segment_size {
            self.latest_segment += self.offset;
            self.offset = 0;
            println!("releasing lock for new partition");
        }
        Ok(())
    }
}

pub struct ShardReader {
    pub segment_id: SegmentId,
    pub latest_log_offset: usize,
    pub offset: ShardOffset,
    pub chunk_size: usize,
    pub shard_dir: ShardDir,
}

impl ShardReader {
    // FIXME this will read a partially written line
    pub fn read(&mut self) -> std::io::Result<Vec<Record>> {
        let path = self.shard_dir.path_to_segment(self.segment_id);

        dbg!(&path);

        let mut res = Vec::new();

        let mut f = File::open(path)?;
        let mut reader = BufReader::new(f);
        reader.seek(SeekFrom::Start(self.offset));

        // FIXME this is unreadable
        loop {
            let mut b = String::new();
            let added_offset = reader.read_line(&mut b)?;

            if added_offset == 0 && self.segment_id < (self.latest_log_offset as u64) {
                println!("rolling to next file");
                f = File::open(
                    self.shard_dir
                        .path_to_segment(self.segment_id + self.offset),
                )
                .unwrap();
                reader = BufReader::new(f);
                continue;
            }

            if added_offset == 0 {
                break;
            }
            self.offset += added_offset as u64;

            if b.ends_with('\n') {
                println!("removed trailing newline");
                b.pop();
            }

            res.push(Record(b.as_bytes().to_vec()));

            if res.len() >= self.chunk_size {
                break;
            }

            if self.offset + self.segment_id >= (self.latest_log_offset as u64) {
                break;
            }
        }

        Ok(res)
    }
}

#[derive(Clone, Debug)]
pub struct ShardDir {
    pub mount_dir: PathBuf,
}

impl ShardDir {
    pub fn path_to_segment(&self, shard_id: SegmentId) -> PathBuf {
        self.mount_dir.join(format!("{:08}", shard_id))
    }

    pub fn get_latest_segment(&self) -> SegmentId {
        let paths = fs::read_dir(&self.mount_dir).unwrap();

        let path = paths
            .map(|p| p.unwrap().file_name().into_string().unwrap())
            .max();

        path.expect("missing shards!!!!!! none found")
            .parse()
            .expect("file not in shard format1111")
    }

    pub fn get_oldest_segment(&self) -> SegmentId {
        let paths = fs::read_dir(&self.mount_dir).unwrap();

        let path = paths
            .map(|p| p.unwrap().file_name().into_string().unwrap())
            .min();

        path.expect("missing shards!!!!!! none found")
            .parse()
            .expect("not in shard style")
    }

    pub fn find_belonging_segment(&self, shard_iterator: u64) -> (SegmentId, ShardOffset) {
        let paths = fs::read_dir(&self.mount_dir).unwrap();
        dbg!(&paths);
        let mut candidate_shard_id: u64 = 0;
        let mut candidate_shard_id_offset: i64 = i64::max_value();

        for p in paths.map(|x| x.unwrap()) {
            dbg!(&p);
            let n: i64 = p.file_name().into_string().unwrap().parse().unwrap();
            let diff = (shard_iterator as i64) - n;
            dbg!(diff);
            if diff >= 0 && diff <= candidate_shard_id_offset {
                candidate_shard_id = n as u64;
                candidate_shard_id_offset = diff;
            }
            dbg!(candidate_shard_id);
            dbg!(candidate_shard_id_offset);
        }

        (candidate_shard_id, candidate_shard_id_offset as u64)
    }

    pub fn create_first_segment(&self) {
        let path = self.path_to_segment(0);
        dbg!(&path);
        File::create(&path).expect(&format!(
            "could not create file {}",
            &path.to_string_lossy()
        ));
    }

    pub fn assert_mount_path(&self) {
        if self.mount_dir.exists() && !self.mount_dir.is_dir() {
            panic!("mount path exists and is not a directory")
        }

        if !self.mount_dir.exists() {
            println!(
                "creating mounting dir in {}",
                &self.mount_dir.to_string_lossy()
            );
            fs::create_dir(&self.mount_dir);
        }

        let paths = fs::read_dir(&self.mount_dir).expect("Could not read dir entries");
        let valid_paths: Vec<DirEntry> = paths.map(|x| x.expect("invalid record")).collect();
        match valid_paths.len() {
            0 => {
                println!("about to create first segment");
                self.create_first_segment()
            }
            _x => println!("all is ok, found segment"),
        }
    }

    pub fn get_end_offset(&self, shard_id: SegmentId) -> u64 {
        let f = File::open(self.path_to_segment(shard_id)).unwrap();
        let mut reader = BufReader::new(f);
        reader.seek(SeekFrom::End(0)).unwrap()
    }
}

pub fn assert_recordable(data: &[u8]) -> Result<(), failure::Error> {
    let xxx = std::str::from_utf8(&data)?;
    base64::decode(xxx)?;
    Ok(())
}



#[cfg(test)]
mod tests {
    use std::{env, panic, thread, time};
    use std::fs::{create_dir, File};
    use std::io::BufReader;
    use std::io::prelude::*;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use rand::{Rng, thread_rng};
    use rand::distributions::Alphanumeric;

    use crate::shards::shards::{Record, ShardDir, ShardReader, ShardWriter, ShaW};

    fn with_tmp_dir<T>(test: T) -> ()
        where T: FnOnce(PathBuf) -> () + panic::UnwindSafe
    {
        let rand_string: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(10)
            .collect();
        let mount_dir = env::temp_dir().join(format!("to_mount-test-{}", rand_string));
        std::fs::remove_dir_all(&mount_dir);
        wait_a_bit();

        let result = panic::catch_unwind(|| {
            test(mount_dir.clone())
        });

        std::fs::remove_dir_all(mount_dir);

        wait_a_bit();
        assert!(result.is_ok())
    }


    fn wait_a_bit() {
        let ten_millis = time::Duration::from_millis(10);
        thread::sleep(ten_millis);
    }



    #[test]
    fn shard_dir_creates_dir() {

        with_tmp_dir(|mount_dir: PathBuf| {
            assert!(!mount_dir.exists());
            let shard_dir = ShardDir { mount_dir: mount_dir.clone() };

            shard_dir.assert_mount_path();

            assert!(mount_dir.exists());

        });

    }

    #[test]
    fn shard_dir_does_not_delete_existing_dir() {
        with_tmp_dir(|mount_dir: PathBuf| {
            create_dir(&mount_dir).unwrap();

            let shard_dir = ShardDir { mount_dir: mount_dir.clone() };

            shard_dir.assert_mount_path();

            assert!(&mount_dir.exists());

        });
    }

    #[test]
    fn shard_dir_gives_a_0_segment_on_new_dir() {
        with_tmp_dir(|mount_dir: PathBuf| {
            create_dir(&mount_dir).unwrap();

            let shard_dir = ShardDir { mount_dir: mount_dir.clone() };

            shard_dir.assert_mount_path();
            wait_a_bit();
            let latest_segment = shard_dir.get_latest_segment();

            assert_eq!(latest_segment, 0);

        });
    }

    #[test]
    fn shard_writer2_writes_to_shard() {
        with_tmp_dir(|mount_dir| {
            let shard_dir = ShardDir { mount_dir: mount_dir.clone() };

            shard_dir.assert_mount_path();

            let latest_segment = shard_dir.get_latest_segment();
            let latest_shard_offset = shard_dir.get_end_offset(latest_segment);
            let mut shard_writer = ShardWriter {
                latest_segment: latest_segment,
                shard_dir: shard_dir.clone(),
                offset: latest_shard_offset,
                max_segment_size: 1000000,
            };
            let string_data = base64::encode("meucu_tem_oculos".as_bytes());
            let record = Record(string_data.clone().into_bytes());

            shard_writer.write(record);

            let path = mount_dir.join(shard_dir.path_to_segment(shard_writer.latest_segment));
            let expected = format!("{}\n", &string_data);
            let res = {
                let mut f = File::open(path).unwrap();
                let mut res = String::new();
                f.read_to_string(& mut res);
                res
            };
            let string_data_size_in_bytes = string_data.into_bytes().len() as u64;
            let new_offset =  shard_writer.offset;
            assert_eq!(res, expected);
            assert_eq!(new_offset, string_data_size_in_bytes + 1)
        })
    }

    #[test]
    fn shard_writer2_writes_to_shard_rolls() {
        with_tmp_dir(|mount_dir| {
            let shard_dir = ShardDir { mount_dir: mount_dir.clone() };

            shard_dir.assert_mount_path();

            wait_a_bit();

            let original_latest_segment = shard_dir.get_latest_segment();
            let latest_shard_offset = shard_dir.get_end_offset(original_latest_segment);
            let mut shard_writer = ShardWriter {
                latest_segment: latest_shard_offset,
                shard_dir: shard_dir.clone(),
                offset: latest_shard_offset,
                max_segment_size: 10,
            };
            let string_data = base64::encode("meucu_tem_oculos".as_bytes());
            let record = Record(string_data.clone().into_bytes());

            shard_writer.write(record.clone());

            shard_writer.write(record.clone());
            let new_latest_segment = shard_dir.get_latest_segment();
            assert!(original_latest_segment < new_latest_segment);
            let string_data_size_in_bytes = string_data.into_bytes().len() as u64;


            assert_eq!(new_latest_segment, string_data_size_in_bytes + 1); // \n is 1 byte long
        })
    }


    #[test]
    fn shard_reader_read_empty() {
        with_tmp_dir(|mount_dir| {
            let shard_dir = ShardDir { mount_dir: mount_dir.clone() };

            shard_dir.assert_mount_path();
            wait_a_bit();

            let mut shard_reader = ShardReader {
                segment_id: shard_dir.get_latest_segment(),
                latest_log_offset: shard_dir.get_latest_segment() as usize,
                offset: 0,
                chunk_size: 10,
                shard_dir
            };

            let res = shard_reader.read();
            assert!(res.is_ok());
            assert_eq!(res.unwrap().len(), 0);
        })
    }

    #[test]
    fn shard_reader_read_containing() {
        with_tmp_dir(|mount_dir| {
            let shard_dir = ShardDir { mount_dir: mount_dir.clone() };

            shard_dir.assert_mount_path();
            wait_a_bit();

            let original_latest_segment = shard_dir.get_latest_segment();
            let latest_shard_offset = shard_dir.get_end_offset(original_latest_segment);
            let mut shard_writer = ShardWriter {
                latest_segment: latest_shard_offset,
                shard_dir: shard_dir.clone(),
                offset: latest_shard_offset,
                max_segment_size: 100,
            };
            let string_data_1 = base64::encode("meucu_tem_oculos_1".as_bytes());
            let record_1 = Record(string_data_1.clone().into_bytes());

            let string_data_2 = base64::encode("meucu_tem_oculos_2".as_bytes());
            let record_2 = Record(string_data_1.clone().into_bytes());

            shard_writer.write(record_1.clone());

            shard_writer.write(record_2.clone());


            let segment_id = shard_dir.get_oldest_segment();
            let latest_log_offset = shard_dir.get_end_offset(shard_dir.get_latest_segment()) as usize;
            let mut shard_reader = ShardReader {
                segment_id,
                latest_log_offset,
                offset: shard_dir.get_oldest_segment(),
                chunk_size: 10,
                shard_dir
            };

            let res = shard_reader.read();
            assert!(res.is_ok());
            assert_eq!(res.unwrap(), vec![record_1.clone(), record_2.clone()])
        })
    }
}