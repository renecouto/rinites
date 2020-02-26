use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime};

use actix_web::client::{Client, ClientResponse};
use futures;
use rand::{Rng, thread_rng};
use rand::distributions::Alphanumeric;

//use futures_util::future::future::FutureExt;
#[actix_rt::main]
async fn main() {
    let data = "{\"record\":\"\
    VFpDS1Bpb1FPdlJwWWgzdzVwZ1RBNG50UXhHT2pRUUlqc0tQM3ZPSUJBdDA5Q285S0dNejkxc1djMzYxNHJWMTJyVnphSjBWa2JQMEpmNjhiUm9RRUlnN0I0SHV5OE1PRlEwZQ\
    OXk3NkVDMXVPbHRYc1dpT1g3NmhlNXNxbXc2Q2RrRzlYWVp1UlZTU000TU9ONUlLOUJsUEVZb1VOSllpYjFGcjU1ZU5kVzJpbDlObGVBeVdwUmpRaFl5Q2NIUUYwMVZWRjlSZg==\"}";
    dbg!(data);
    let mut client = Client::default();
    let s = data.len();
    let z = 50;
    let n = 1000;
    // Create request builder and send request
    let req = || {
        client.post("http://127.0.0.1:2301/put-records")
            .header("User-Agent", "Actix-web")
            .header("Content-Type", "application/json")
            .timeout(Duration::from_secs(30))
            .send_body(data)
    };

    let now = SystemTime::now();
    let c: AtomicUsize = AtomicUsize::new(0);
    let errors: AtomicUsize = AtomicUsize::new(0);
    for _ in 0..(n/z) {
        let mut yyz = vec![];
        for _ in 0..z {
            yyz.push(Box::new(req()));
        }
        let res = futures::future::join_all(yyz).await;
        for x in res {
            match x {
                Ok(res) if res.status() == 200 => {
                    c.fetch_add(1, Ordering::Relaxed);
                },
                Ok(res) => {
                    dbg!(res);
                    errors.fetch_add(1, Ordering::Relaxed);
                },
                _ => {
                    errors.fetch_add(1, Ordering::Relaxed);
                },
            }
        }
    }

    dbg!(c.load(Ordering::Relaxed));
    dbg!(errors.load(Ordering::Relaxed));
    let end = now.elapsed().expect("se fodeu 2").as_millis();
    println!("it took {} ms to send {} bytes per request in {} request, totaling {} bytes and {} bytes/sec", end, s, n, n*s, (n as f64 *s as f64)/(end as f64 / 1000.0));


}