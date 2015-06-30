#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <string.h>
#include "../redis_cluster.hpp"

int main(int argc, char *argv[])
{
    redis::cluster::Cluster *cluster = new redis::cluster::Cluster();

    if( cluster->setup("127.0.0.1:7000, 127.0.0.1:7001")!=0 ) {
        std::cerr << "cluster setup fail" << std::endl;
        delete cluster;
        return 1;
    }
    

    return 0;
}
