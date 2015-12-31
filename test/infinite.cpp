#include "../redis_cluster.hpp"
#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <iostream>
#include <map>
#include <hiredis/hiredis.h>

typedef struct {
    int read;
    int write;
    int read_t;
    int write_t;
    int total_read;
    int total_write;
    int read_error;
    int write_error;
    int unmatch;
}stat_item_t;

typedef struct {
    int now_sec_;
    int now_us_;
    int last_us_;
}time_period_t;

typedef std::map<std::string, std::string> CacheType;
typedef CacheType::iterator CacheIter;

CacheType cache_;

int now_sec_ = 0;
int now_us_ = 0;
int last_us_ = 0;

redis::cluster::Cluster *cluster_ = NULL;
stat_item_t stat_;

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

int check_point() {
    struct timeval tv;
    gettimeofday( &tv, NULL );
    now_sec_ = tv.tv_sec;
    now_us_ = tv.tv_sec*1000000 + tv.tv_usec;

    int ret = now_us_ - last_us_;
    last_us_ = now_us_;

    return ret;
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
    check_point();
    srand( now_us_ );
    while( true ) {
    
        std::string key = get_random_key();
        std::string value_read;
        std::string value_write = get_random_value();

        /* check 
         */
    	check_point();
        int rv = redis_get(key, value_read);
        stat_.read ++;
        stat_.total_read ++;
        if( rv<0 ) {
            stat_.read_error ++;
        } else {
            CacheIter iter = cache_.find( key );
            if( iter!=cache_.end() )  {
                if( value_read!=iter->second ) 
                    stat_.unmatch ++;
            }
        }
	stat_.read_t += check_point();

        /* set
         */
        rv = redis_set(key, value_write);
        stat_.write ++;
        stat_.total_write ++;
        if( rv<0 ) {
            stat_.write_error ++;
        } else {
            cache_[key] = value_write;
        }
	stat_.write_t += check_point();

        usleep( 1000*10 );

        if( last_t != now_sec_ ) {
            last_t = now_sec_;
            std::cout << stat_.total_read << " R(" << stat_.read_error << " err, " << stat_.read_t/stat_.read << " usec per op ) | " 
                      << stat_.total_write << " W(" << stat_.write_error << " err, " << stat_.write_t/stat_.write<< " usec per op ) | "
                      << stat_.unmatch << " UNMATCH" << std::endl;
	    stat_.read_t = stat_.write_t = 0;
	    stat_.read = stat_.write = 0;
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
        ret = -1;
    } else if( reply->type==REDIS_REPLY_STATUS && !strcmp(reply->str, "OK") ) {
        ret = 0;
    } else if( reply->type==REDIS_REPLY_ERROR ){
        //std::cout << "redis_set error " << reply->str << std::endl; 
        ret = -1;
    } else {
        ret = -1;
    }

    if( reply )
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
        ret = -1;
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

    if( reply )
        freeReplyObject( reply );

    return ret;
}
