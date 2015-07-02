# redis-c-cluster

A c++ client library, based on hiredis, inspired by [redis-rb-cluster], supports redis cluster.
The library will cache slots data locally, refresh it when change happens, and handle redirections for redis's MOVED and ASK.

[redis-rb-cluster]: https://github.com/antirez/redis-rb-cluster

# Current support

The principle is that only data read/write commands are supported in cluster mode.
None-key commands are not supported in cluster mode, for example, INFO/SHUTDOWN.
Multi-keys commands are not supported yes, it will be in the future issue.

It's not reality to list all unsupported commands here, so just follow the principle.
Explicitly unsupported commands are as followed.
* INFO
* SHUTDOWN
* MULTI
* SLAVEOF
* CONFIG

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

//doing some stuff

freeReplyObject(reply);
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
