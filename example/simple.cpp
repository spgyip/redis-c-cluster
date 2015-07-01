#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <string.h>
#include "../redis_cluster.hpp"

int main(int argc, char *argv[])
{
    std::string startup = "127.0.0.1:7000,127.0.0.1:7001";
    if( argc>1 ) {
        startup = argv[1];
    }
    std::cout << "cluster startup with " << startup << std::endl;
    redis::cluster::Cluster *cluster = new redis::cluster::Cluster();

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
    std::cout << "[done]" << "set " << commands[1] << " '" << commands[2] << "' " << std::endl;
    return 0;
}
