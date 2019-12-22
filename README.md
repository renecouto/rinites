# RINITES

Kinesis copy. in the present moment, it emulates a kinesis shard through a udp api. The focus of this project is to learn about low level multithreading in rust.

# Requirements
- Cargo https://doc.rust-lang.org/cargo/getting-started/installation.html

# DO NOT USE THIS IN PROD LOLLLLLLL

# Features

## UDP API
### Run
```
cargo run --bin udp_api -- -m <mount_path>
```
### UDP 'Request'
Send data through udp to the 3400 port and the application will send it back to the port the data was sent from. One way of receiving the 'responses' is to open a netcat
```
$ netcat -u localhost 3400
PutRecords <base64-encoded-string> # A Record is a base64 encoded string. you can encode any string by doing $ echo -n 'something' | base64
GetShardIterator <'Oldest'/'Latest'> # A shard iterator is a offset on a shard in bytes to the beggining of a Record
GetRecords <shard-iterator> # retrieve a chunk of records beggining at the requested shard-iterator
```

# Doing
- Use tokio instead of my custom threadpool lol
- Use tcp instead of udp. Udp seems useless if you want to retrieve records through multiple requests. I mean, it is technically possible, but it would be a huge hack.

# TO DO
- Tests. Don't know how to do them, since everything is IO and Rust does not let any race condition past it
- I need some logic to index by timestamp. Maybe keep a '<log-segment>.metadata' file with creation date of something. I don't want to use the actual file's metadata because it won't work if the shard leadership is transferred (thinking about replication).
- Delete old log-segments. This might depend on timestamp or on max offset.
- multiple shards (list shards, add shards)
- Replication
- Clustering etc
- remove/merge shards?

# License
I have no idea, but I guess MIT????