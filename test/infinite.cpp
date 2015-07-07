#include "../redis_cluster.hpp"
#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <iostream>
#include <map>
#include <hiredis/hiredis.h>

struct StatS {
    int read;
    int write;
    int read_error;
    int write_error;
    int unmatch;
};

typedef std::map<std::string, std::string> CacheType;
typedef CacheType::iterator CacheIter;
CacheType cache_;
int now_sec_ = 0;
int now_us_ = 0;
redis::cluster::Cluster *cluster_ = NULL;
struct StatS stat_;

std::string get_random_key() {
    int n = 10000*(rand()/(RAND_MAX+0.1));
    std::ostringstream ss;
    ss << "key_" << n;
    return ss.str();
}

std::string get_random_value() {
    int n = 100000*(rand()/(RAND_MAX+0.1));
    std::ostringstream ss;
    ss << "value_" << n;
    return ss.str();
}

int redis_set(const std::string &key, const std::string& value);
int redis_get(const std::string &key, std::string& value);

int main(int argc, char *argv[])
{
    std::string startup = "127.0.0.1:7000,127.0.0.1:7001";
    if( argc>1 ) {
        startup = argv[1];
    }
    std::cout << "cluster startup with " << startup << std::endl;
    cluster_ = new redis::cluster::Cluster();

    if( cluster_->setup(startup.c_str(), true)!=0 ) {
        std::cerr << "cluster setup fail" << std::endl;
        return 1;
    }

    int last_t = 0;
    while( true ) {
        struct timeval tv;
        gettimeofday( &tv, NULL );
        now_sec_ = tv.tv_sec;
        now_us_ = tv.tv_sec*1000000 + tv.tv_usec;
        srand( now_us_ );
    
        std::string key = get_random_key();
        std::string value_read;
        std::string value_write = get_random_value();

        /* check 
         */
        int rv = redis_get(key, value_read);
        stat_.read ++;
        if( rv<0 ) {
            stat_.read_error ++;
        } else {
            CacheIter iter = cache_.find( key );
            if( iter!=cache_.end() )  {
                if( value_read!=iter->second ) 
                    stat_.unmatch ++;
            }
        }

        /* set
         */
        rv = redis_set(key, value_write);
        stat_.write ++;
        if( rv<0 ) {
            stat_.write_error ++;
        } else {
            cache_[key] = value_write;
        }

        usleep( 1000*1000 );

        if( last_t != now_sec_ ) {
            last_t = now_sec_;
            std::cout << stat_.read << " R(" << stat_.read_error << " err) | " 
                      << stat_.write << " W(" << stat_.write_error << " err) | "
                      << stat_.unmatch << " UNMATCH" << std::endl;
        }
    }

    return 0;
}

int redis_set(const std::string &key, const std::string& value) {
    int ret = 0;
    std::vector<std::string> commands;
    commands.push_back("SET");   
    commands.push_back(key);   
    commands.push_back(value);   
    redisReply *reply = cluster_->run(commands);
    if( !reply ) {
        return -1;
    } else if( reply->type==REDIS_REPLY_STATUS && !strcmp(reply->str, "OK") ) {
        ret = 0;
    } else if( reply->type==REDIS_REPLY_ERROR ){
        //std::cout << "redis_set error " << reply->str << std::endl; 
        ret = -1;
    } else {
        ret = -1;
    }
    freeReplyObject( reply );
    return ret;
}
int redis_get(const std::string &key, std::string& value) {
    int ret = 0;
    std::vector<std::string> commands;
    commands.push_back("GET");   
    commands.push_back(key);   
    redisReply *reply = cluster_->run(commands);
    if( !reply ) {
        return -1;
    } else if( reply->type==REDIS_REPLY_NIL ) {
        ret = 1; //not found
    } else if( reply->type==REDIS_REPLY_STRING ) {
        value = reply->str;
        ret = 0;
    } else if( reply->type==REDIS_REPLY_ERROR ){
        //std::cout << "redis_get error " << reply->str << std::endl; 
        ret = -1;
    } else {
        ret = -1;
    }
    freeReplyObject(reply);
    return ret;
}
