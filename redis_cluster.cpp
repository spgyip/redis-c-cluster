#include <stdlib.h>
#include <iostream>
#include <string>
#include <hiredis/hiredis.h>
#include "redis_cluster.hpp"
#include "deps/crc16.c"

RedisCluster::RedisCluster() {
}

RedisCluster::~RedisCluster() {
}

int RedisCluster::setup(const char *startup) {

    if( parse_startup(startup)<0 ) {
        return -1;
    }

    slots_.resize( HASH_SLOTS );

    if( load_slots_cache()<0 ) {
        return -1;
    }

    return 0;
}

int RedisCluster::get(const std::string &key, std::string &value) {
    redisContext *c = NULL;
    redisReply *reply = NULL;
    std::vector<const char *> argv;
    std::vector<size_t> argvlen;
    uint16_t hashing = get_key_hash(key);
    NodeInfoPtr pnode = &(slots_[ hashing % HASH_SLOTS ]);
    
    argv.push_back(key.c_str());
    argv.push_back(value.c_str());
    argvlen.push_back(strlen("GET"));
    argvlen.push_back(value.length());

    /* slot not hit
     */
    if( pnode->host.empty() ) {
        return -1;
    }

    ConnectionsCIter citer = connections_.find( *pnode );
    if( citer==connections_.end() ) {
        c = redisConnect( pnode->host.c_str(), pnode->port );
        if( c && c->err )  {
            redisFree( c );
            return -2;      
        }
        connections_.insert( std::make_pair(*pnode, c) );
    }

    citer = connections_.find( *pnode );
    c = (redisContext *)citer->second;

    reply = (redisReply *)redisCommandArgv(c, argv.size(), argv.data(), argvlen.data());
    if( !reply ) {
        redisFree( c );
        connections_.erase( citer );
        return -3;
    }

    if( reply->type==REDIS_REPLY_ERROR 
        &&(!strncmp(reply->str,"MOVED",5) || !strcmp(reply->str,"ASK")) ) {

        char *p = reply->str, *s; 
        int slot;
        /*
         * [S] for pointer 's'
         * [P] for pointer 'p'
         */
        s = strchr(p,' ');      /* MOVED[S]3999 127.0.0.1:6381 */
        p = strchr(s+1,' ');    /* MOVED[S]3999[P]127.0.0.1:6381 */
        *p = '\0';
        slot = atoi(s+1);
        s = strchr(p+1,':');    /* MOVED 3999[P]127.0.0.1[S]6381 */
        *s = '\0';
        slots_[slot].host = p+1;
        slots_[slot].port = atoi(s+1);

    } else if( reply->type==REDIS_REPLY_STRING ) {
        value.assign(reply->str, reply->len);
        return 1;
    } else if( reply->type==REDIS_REPLY_NIL ) {
        return 0;
    }
    return -1;
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
            for(size_t ii=0; ii<reply->elements; ii++) {
                subr = reply->element[ii];
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
uint16_t RedisCluster::get_key_hash(const std::string &key) {
    std::string::size_type pos1, pos2;
    std::string hashing_key = key;

    pos1 = key.find("{");
    if( pos1!=std::string::npos ) {
        pos2 = key.find("}", pos1+1);
        if( pos2!=std::string::npos ) {
            hashing_key = key.substr(pos1+1, pos2-pos1-1);
        }
    }
    return crc16(hashing_key.c_str(), hashing_key.length());
}

int RedisCluster::test_parse_startup(const char *startup) {
    return parse_startup( startup );
}

std::vector<RedisCluster::NodeInfoType>& RedisCluster::get_startup_nodes() {
    return startup_nodes_;
}
