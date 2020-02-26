use std::net::SocketAddr;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};

use actix_web::{App, get, HttpRequest, HttpResponse, HttpServer, post, Responder, web};
use actix_web::body::Body;
use json::JsonValue;
use serde_derive::{Deserialize, Serialize};
use structopt::StructOpt;

use crate::Response;
use crate::shards::shards::{assert_recordable, Record, ShardDir, ShardIteratorType, ShardReader, ShardWriter, ShaW};

pub struct ShardController {
    pub shard_dir: ShardDir,
    pub latest_log_offset: Arc<AtomicUsize>,
    pub write_lock: Mutex<()>,
}

#[derive(Deserialize, Serialize, PartialEq, Debug)]
pub struct GetRecordsResponse {
    pub next_shard_iterator: u64,
    pub records: Vec<String>,
}

#[derive(Deserialize, Serialize, PartialEq, Debug)]
pub struct PutRecordsResponse {

}


impl ShardController {
    pub fn get_records(&self, shard_iterator: u64) -> GetRecordsResponse {
        let shard_dir = self.shard_dir.clone();
        let (shard_id, offset) = shard_dir.find_belonging_segment(shard_iterator);

        let mut reader: ShardReader = ShardReader {
            segment_id: shard_id,
            offset,
            chunk_size: 10,
            shard_dir,
            latest_log_offset: self.latest_log_offset.load(Ordering::Relaxed),
        };
        let records: Vec<Record> = reader.read().unwrap();
        println!("read {} records", records.len());

        let records = records.iter().map(|r| r.as_string()).collect();

        GetRecordsResponse {
            next_shard_iterator: reader.segment_id + reader.offset,
            records
        }
    }

    pub fn put_records(&self, records: Record) -> PutRecordsResponse {
        let _ = self.write_lock.lock();

        let latest_segment = self.shard_dir.get_latest_segment();
        let latest_shard_offset = self.shard_dir.get_end_offset(latest_segment);
        let mut shard_writer = ShardWriter {
            latest_segment: self.latest_log_offset.load(Ordering::Relaxed) as u64,
            shard_dir: self.shard_dir.clone(),
            offset: latest_shard_offset,
            max_segment_size: 1000000,
        };



        shard_writer.write(records);
        self.latest_log_offset.store(shard_writer.latest_segment as usize, Ordering::Relaxed);

        PutRecordsResponse {}
    }
}

#[cfg(test)]
mod tests {
    use std::{env, panic, thread, time};
    use std::fs::{create_dir, File};
    use std::io::BufReader;
    use std::io::prelude::*;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};
    use std::sync::atomic::{AtomicUsize, Ordering};

    use rand::{Rng, thread_rng};
    use rand::distributions::Alphanumeric;

    use crate::shards::shard_controller::{GetRecordsResponse, PutRecordsResponse, ShardController};
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
    fn get_records_should_just_work_empty() {
        with_tmp_dir(|mount_dir| {

            let shard_dir = ShardDir {mount_dir};
            shard_dir.assert_mount_path();
            let latest_segment = Arc::new(AtomicUsize::new(shard_dir.get_latest_segment() as usize));
            let write_lock = Mutex::new(());

            let shac = ShardController {
                shard_dir,
                latest_log_offset: latest_segment,
                write_lock
            };

            let result = shac.get_records(0);
            let expected = GetRecordsResponse { next_shard_iterator: 0, records: vec![] };
            assert_eq!(result, expected);
        });
    }

    #[test]
    fn put_records_should_just_work() {
        with_tmp_dir(|mount_dir| {

            let shard_dir = ShardDir {mount_dir};
            shard_dir.assert_mount_path();
            let latest_segment = Arc::new(AtomicUsize::new(shard_dir.get_latest_segment() as usize));
            let write_lock = Mutex::new(());

            let shac = ShardController {
                shard_dir,
                latest_log_offset: latest_segment,
                write_lock
            };

            let string_data_1 = base64::encode("meucu_tem_oculos_1".as_bytes());
            let record_1 = Record(string_data_1.clone().into_bytes());



            let result = shac.put_records(record_1.clone());
            let expected = PutRecordsResponse {};
            assert_eq!(result, expected);

            let result = shac.get_records(0);
            let data_len = (string_data_1.as_bytes().len() + 1) as u64;
            let expected = GetRecordsResponse { next_shard_iterator: data_len, records: vec![record_1.as_string()]};
            assert_eq!(result, expected);
        });
    }
}

