# redis-c-cluster

A c++ client library supports redis cluster, inspired by [redis-rb-cluster].
Based on hiredis for redis implement, and redis3m for handling connection pool.

[redis-rb-cluster]: https://github.com/antirez/redis-rb-cluster

# Current support

Multi-keys commands are not supported yet. It'll be the future issue.
Currently supported commands as followed.
* GET
* SET
* HGET
* HSET

# Example

# Install
* c++ 11 is required.

