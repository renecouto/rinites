# RINITES

kinesis copy, to learn a bit about low level threading in rust

# Features
![You can request to read data from a file lol](img/rinites_udp.png)

# TO DO
`in this todo: partition == file`
- UDP server API. Request a shardIterator: oldest or latest.
- Tests. Don't know how to do them, since everything is IO and Rust does not let any race condition past it
- create new partitions after they reach some size. This might need a lookup table or something to hold whats the next file
- I need some logic to index by timestamp or sequencenumber like kinesis does. lookup table?
- Use an async runtime with async locks and a real http server (actix-web, probably)
- Delete old partitions

