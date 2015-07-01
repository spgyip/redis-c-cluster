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
```cpp
if( cluster->setup("127.0.0.1:7000, 127.0.0.1:7001", true)!=0 ) {
    std::cerr << "cluster setup fail" << std::endl;
    return 1;
}
 
std::vector<std::string> commands;
commands.push_back("SET");   
commands.push_back("foo");   
commands.push_back("hello world");
redisReply *reply = cluster->run(commands);
if( !reply ) {
    std::cerr << "(error)" << cluster->strerr() << ", " << cluster->err() << std::endl;
    return 1;
}
```

# Install
* c++ 11 is required.

