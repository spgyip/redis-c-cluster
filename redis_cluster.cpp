#include <stdlib.h>
#include "redis_cluster.hpp"

/*static bool node_info_equal(RedisCluster::NodeInfoRef l, RedisCluster::NodeInfoRef r) {
    return (l.host==r.host && l.port==r.port);
}*/

RedisCluster::RedisCluster() {
}

RedisCluster::~RedisCluster() {
}

int RedisCluster::init(const char *startup) {

    if( parse_startup(startup)<0 ) {
        return -1;
    }

    slots_.resize( HASH_SLOTS );

    if( load_slots_cache()<0 ) {
        return -1;
    }

    return 0;
}

int RedisCluster::parse_startup(const char *startup) {
    char *p1, *p2, *p3;
    char *tmp = (char *)malloc( strlen(startup)+1 );
    memcpy(tmp, startup, strlen(startup)+1);

    startup_nodes_.clear();

    p1 = p2 = tmp;
    do {
        p2 = strchr(p1, ',');
        if( p2 ) {
            *p2 = '\0';
        }
        p3 = strchr(p1, ':');
        if( p3 ) {
            NodeInfoType node;

            *p3 = '\0';
            node.port = atoi(p3+1); //get port 

            while(p1<p3 && *p1==' ')//trim left
                p1++;

            p3--;
            while(p3>p1 && *p3==' ')//trim right
                *(p3--) = '\0';

            node.host = p1; //get host
            startup_nodes_.push_back(node);        
        }
        if( p2 )
            p1 = p2+1;
        else
            break;
    }while(1);
    
    free( tmp );
    return startup_nodes_.size();
}
int RedisCluster::load_slots_cache() {
    
    int start, end;
    int count = 0;
    redisContext *c;
    redisReply *reply, *subr, *innr;
    NodeInfoType node;
    NodeInfoPtr np;
    for(size_t i=0; i<startup_nodes_.size(); i++) {
        np = &startup_nodes_[i];
        c = redisConnect(np->host.c_str(), np->port);
        if( c && c->err ) {
            redisFree( c );
            continue;
        }

        reply = (redisReply *)redisCommand(c, "cluster slots");
        if( !reply ) {
            redisFree(c);
            continue;
        }
        if( reply->type==REDIS_REPLY_ARRAY ) {
            for(size_t ii=0; i<reply->elements; ii++) {
                subr = reply->element[i];
                if( subr->type!=REDIS_REPLY_ARRAY
                    || subr->elements<2
                    || subr->element[0]->type!=REDIS_REPLY_INTEGER
                    || subr->element[1]->type!=REDIS_REPLY_INTEGER ) {
                    redisFree(c);
                    freeReplyObject(reply);
                    continue;
                }

                start = subr->element[0]->integer;
                end = subr->element[1]->integer; 

                if( subr->elements>=3 ) {
                    innr = subr->element[2];
                    if( innr->elements<2 
                        || innr->element[0]->type!=REDIS_REPLY_STRING 
                        || innr->element[1]->type!=REDIS_REPLY_INTEGER ) {
                        redisFree(c);
                        freeReplyObject(reply);
                        continue;
                    }
                    NodeInfoType node;
                    node.host = innr->element[0]->str;
                    node.port = innr->element[1]->integer;
                    for(int jj=start; jj<=end; jj++) {
                        slots_[jj] = node;
                    }
                    count += (end-start+1);
                }       
            }
        }
        redisFree(c);
        freeReplyObject(reply);
        break;
    }
    
    return count;
}

int RedisCluster::clear_slots_cache() {
    slots_.clear();
    slots_.resize(HASH_SLOTS);
    return 0;
}

int RedisCluster::test_parse_startup(const char *startup) {
    return parse_startup( startup );
}

std::vector<RedisCluster::NodeInfoType>& RedisCluster::get_startup_nodes() {
    return startup_nodes_;
}
