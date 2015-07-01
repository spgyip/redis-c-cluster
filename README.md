# redis-c-cluster

A c++ client library, based on hiredis, inspired by [redis-rb-cluster], supports redis cluster.
The library will cache slots data locally, refresh it when change happens, and handle redirections for redis's MOVED and ASK.

[redis-rb-cluster]: https://github.com/antirez/redis-rb-cluster

# Current support

Multi-keys commands are not supported yet. 
It'll be in the future issue.
Currently supported commands as followed.
* GET
* SET
* HGET
* HSET

# Example
  Lookup example/ for more examples.
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
  make && make install
* c++ 11 is required.
* gtest is required for unittest.
* hiredis is required for redis api.

# DEBUG
  Remove Makefile's '-DDEBUG' to avoid debug message, which will be printed to standard output.

# Future
* Reentrance support.
