use actix_web::client::{Client, ClientResponse};
use futures;
use std::time::{Duration, SystemTime};
use rand::{thread_rng, Rng};
use rand::distributions::Alphanumeric;
use std::sync::atomic::{AtomicUsize, Ordering};

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
//        let stuff = r#"{"record": ""#;
//        let stuff2 = r#""}"#;
//        let random_data =  {
//            let rand_string: String = thread_rng()
//                .sample_iter(&Alphanumeric)
//                .take(s)
//                .collect();
//            base64::encode(rand_string.as_bytes())
//        };
        client.post("http://127.0.0.1:2301/put-records")
            .header("User-Agent", "Actix-web")
            .header("Content-Type", "application/json")
            .timeout(Duration::from_secs(30))
            .send_body(data)
//        format!("{}{}{}", stuff, random_data, stuff2))

    };

    let now = SystemTime::now();
//    let mut kkk = Vec::new();
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

//    let res = futures::future::join_all(kkk).await;

//    dbg!(res);
    dbg!(c.load(Ordering::Relaxed));
    dbg!(errors.load(Ordering::Relaxed));
    let end = now.elapsed().expect("se fodeu 2").as_millis();
    println!("it took {} ms to send {} bytes per request in {} request, totaling {} bytes and {} bytes/sec", end, s, n, n*s, (n as f64 *s as f64)/(end as f64 / 1000.0));




                 // <- Send http request

//    println!("Response: {:?}", response);
}