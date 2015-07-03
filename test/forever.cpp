#include "../redis_cluster.hpp"
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <iostream>
#include <set>
#include <hiredis/hiredis.h>

int main(int argc, char *argv[])
{
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
    commands.push_back("GET");   
    commands.push_back("foo");   
    //commands.push_back("hello world");
    while( true ) {
        redisReply *reply = cluster->run(commands);
        if( !reply ) {
            std::cerr << "(error) " << cluster->strerr() << ", " << cluster->err() << std::endl;
            return 1;
        } else if( reply->type==REDIS_REPLY_ERROR ) {
            std::cerr << "(error) " << reply->str << std::endl;
        } else if( reply->type==REDIS_REPLY_NIL){
            std::cerr << "(nil)" << std::endl;
        }
        else {
            std::cout << "[DONE] " << reply->str << std::endl;
        }
        freeReplyObject( reply );
        std::cerr << "Input anything to continue." << std::endl;
        //std::cerr << std::endl;
        //sleep(1);
        getchar();
    }

    return 0;
}
