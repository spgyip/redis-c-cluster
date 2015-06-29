#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <iostream>
#include "../redis_cluster.hpp"

int main(int argc, const char *argv[])
{
    std::string startup;
    if( argc<2 ) {
        std::cout << "Usage: " << argv[0] << " <startup>" << std::endl;
        return 1;
    }
    startup = argv[1];
    
    RedisCluster *cluster = new RedisCluster();
    int rv = cluster->setup(startup.c_str());
    std::cout << rv << std::endl;
    
    return 0;
}
