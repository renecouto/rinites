use std::net::SocketAddr;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};

use actix_web::{App, get, HttpRequest, HttpResponse, HttpServer, post, Responder, web};
use actix_web::body::Body;
use actix_web::Result;
use actix_web::web::Json;
use json::JsonValue;
use serde_derive::{Deserialize, Serialize};
use structopt::StructOpt;

use rinites::Response;
use rinites::shards::shard_controller::{GetRecordsResponse, PutRecordsResponse, ShardController};
use rinites::shards::shards::{assert_recordable, Record, ShardDir, ShardReader, ShardWriter, ShaW};

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



#[get("/get-records/{shard_iterator}")]
async fn get_records(shard_controller: web::Data<ShardController>, shard_iterator: web::Path<u64>) -> Result<Json<GetRecordsResponse>> {

    let result = shard_controller.get_records(shard_iterator.into_inner());

    Ok(Json(result))
}

#[derive(Deserialize, Serialize)]
struct PutRecordsRequest {
    record: String,
}

#[post("/put-records")]
async fn put_records(shard_controller: web::Data<ShardController>, body: web::Json<PutRecordsRequest>) -> Result<Json<PutRecordsResponse>> {
    let record = Record::from_string(body.record.clone())?;
    let result = shard_controller.put_records(record);
    Ok(Json(result))
}

#[derive(Deserialize, Serialize)]
struct GetShardIteratorRequest {
    iterator_type: String
}

#[post("/get-shard-iterator")]
async fn get_shard_iterator(shard_controller: web::Data<ShardController>, body: web::Json<GetShardIteratorRequest>) -> impl Responder {
    let shard_dir = &shard_controller.shard_dir;
    let res = {
        match body.iterator_type.as_str() {
            "Latest" => {
                let shard_iterator = shard_controller.latest_log_offset.load(Ordering::Relaxed);
                Response(format!("shard iterator: {}", shard_iterator))
            },
            "Oldest" => {
                let shard_iterator = shard_dir.get_oldest_segment();

                Response(format!("shard iterator: {}", shard_iterator))
            }
            _ => Response(format!("shard iterator type not supported"))
        }
    };
    HttpResponse::Ok().body(res.0)

}

fn setup_shard_controller(opts: &Opts) -> ShardController {
    let shard_dir = ShardDir {
        mount_dir: Path::new(&opts.mount_path).to_path_buf(),
    };
    shard_dir.assert_mount_path();
    let latest_segment = shard_dir.get_latest_segment();

    let latest_segment = Arc::new(AtomicUsize::new(latest_segment as usize));

    let write_lock = Mutex::new(());
    ShardController { shard_dir, latest_log_offset: latest_segment.clone(), write_lock }
}

#[actix_rt::main]
async fn main() -> std::io::Result<()> {
    let opts = get_cli_opts();
    let addr = format!("{}:{}", opts.host, opts.port);
    let shard_controller = web::Data::new(setup_shard_controller(&opts));

    HttpServer::new(move|| App::new()
        .app_data(shard_controller.clone())
        .service(get_records)
        .service(put_records)
        .service(get_shard_iterator))
        .bind(addr)?
        .start()
        .await
}