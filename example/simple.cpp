#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <string.h>
#include <hiredis/hiredis.h>
#include "../redis_cluster.hpp"

int main(int argc, char *argv[]) {
    std::string startup = "127.0.0.1:7000,127.0.0.1:7001";
    if( argc>1 ) {
        startup = argv[1];
    }
    std::cout << "cluster startup with " << startup << std::endl;
    redis::cluster::Cluster *cluster = new redis::cluster::Cluster();

    if( cluster->setup(startup.c_str(), true)!=0 ) {
        std::cerr << "cluster setup fail" << std::endl;
        return 1;
    }

    std::vector<std::string> commands;

    /* set */
    
    std::cerr << "set foo ..." << std::endl;
    commands.push_back("SET");
    commands.push_back("foo");
    commands.push_back("hello world");
    redisReply *reply = cluster->run(commands);
    if( !reply ) {
        std::cerr << "(error) " << cluster->strerr() << ", " << cluster->err() << std::endl;
    } else if( reply->type==REDIS_REPLY_ERROR ) {
        std::cerr << "(error) " << reply->str << std::endl;
    } else {
        std::cout << "[SET DONE] " << "set " << commands[1] << " '" << commands[2] << "' " << std::endl;
    }

    if( reply )
        freeReplyObject( reply );

    /* get */
    
    std::cerr << "get foo ..." << std::endl;
    commands.clear();
    commands.push_back("GET");
    commands.push_back("foo");
    reply = cluster->run(commands);
    if( !reply ) {
        std::cerr << "(error) " << cluster->strerr() << ", " << cluster->err() << std::endl;
    } else if( reply->type==REDIS_REPLY_ERROR ) {
        std::cerr << "(error) " << reply->str << std::endl;
    } else if( reply->type==REDIS_REPLY_NIL ) {
        std::cerr << "(nil)" << std::endl;
    } else if (reply->type==REDIS_REPLY_STRING) {
        std::cerr << "[GET DONE] " << reply->str << std::endl;
    } else {
        std::cerr << "(error) unexpected reply type " << reply->type << std::endl;
    }

    if( reply )
        freeReplyObject( reply );

    delete cluster;

    return 0;
}
