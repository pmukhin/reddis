## Reddis

Simple Redis implmentation in Rust Programming Language. This is by no means a production ready implementation. Developed for fun and for the purpose of learning Rust.

### Benchmarks
- `redis-benchmark -p 6380 -t GET` 145137.88 requests per second
- `redis-benchmark -p 6380 -t SET` 52548.61 requests per second
- `redis-benchmark -p 6380 -t LPUSH` 44385.27 requests per second
