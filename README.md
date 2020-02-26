# RINITES

This aspires to be somewhat of a AwsKinesis/Kafka copy with very few features. In the present moment, it emulates a Kinesis shard through a udp/tcp api. The focus of this project is to learn about low level multithreading in rust.

# Requirements
- Cargo https://doc.rust-lang.org/cargo/getting-started/installation.html

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

## TCP API

As it emulates kinesis, there are 3 main operations: PutRecords, GetShardIterator and GetRecords. A ShardIterator is a number that points to the consumer's present position in the consumption of the stream.
To make a GetRecords request, you must supply a ShardIterator.
### Run
```
mkdir ./mount-data-here
cargo run -- --mount_path ./mount-data-here --port 8080
```

### Put Records
the endpoint /put-records accepts a json with the base64 encoded data you want to insert in the 'records' field
```
PUT_RECORDS_DATA="{\"records\":\"$(echo 'hello, world' | base64)\"}"
curl -i localhost:8080/put-records --data $PUT_RECORDS_DATA -H 'Content-Type:application/json'
```

### Get Shard Iterator
```
curl -i localhost:8080/get-shard-iterator -d '{"iterator_type":"Oldest"}' -H 'Content-Type:application/json'
```
This should return HTTP200, and 'shard iterator: 0'

### Get Records
Using the retrieved shard iterator,
```
curl -i localhost:8080/get-records/<shard-iterator>
```

# TO DO
- More tests
- Allow getting shard iterator by timestamp
- Delete old log-segments. This might depend on timestamp or on max offset.
- multiple shards (list shards, add shards)
- Replication
- Clustering etc
- remove/merge shards?
