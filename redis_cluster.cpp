#include "redis_cluster.hpp"
#include "deps/crc16.c"
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <iostream>
#include <string>
#include <set>
#include <iterator>
#include <hiredis/hiredis.h>

#ifdef DEBUG
#define DEBUGINFO(msg) std::cerr << "[DEBUG] "<< msg << std::endl;
#else
#define DEBUGINFO(msg)
#endif

namespace redis {
namespace cluster {

static const char *UNSUPPORT = "#INFO#SHUTDOWN#MULTI#SLAVEOF#CONFIG#";

static inline std::string to_upper(const std::string& in) {
    std::string out;
    out.reserve( in.size() );
    for(std::size_t i=0; i<in.length(); i++ ) {
        out += char( toupper(in[i]) );       
    }
    return out;
}

Cluster::Cluster()
:load_slots_asap_(false), errno_(E_OK)
{
}

Cluster::~Cluster() {
}

int Cluster::setup(const char *startup, bool lazy) {

    if( parse_startup(startup)<0 ) {
        return -1;
    }

    slots_.resize( HASH_SLOTS );

    if( !lazy && load_slots_cache()<0 ) {
            return -1;
    } 

    if( lazy ){
        load_slots_asap_ = true;
    }

    return 0;

}

redisReply* Cluster::run(const std::vector<std::string> &commands) {
    std::vector<const char *> argv;
    std::vector<size_t> argvlen;

    if( commands.size()<2 ) {
        set_error(E_COMMANDS) << "none-key commands are not supported";
        return NULL;
    }
    
    std::string cmd = to_upper(commands[0]);

    do{
        std::ostringstream ss;
        ss << "#" << cmd << "#";
        const char *p = strstr(UNSUPPORT, ss.str().c_str());
        if( p ) {
            set_error(E_COMMANDS) << "command [" << cmd << "] not supported";
            return NULL;
        }
    }while(0);

    for( size_t i=0; i<commands.size(); i++ ) {
        argv.push_back(commands[i].c_str());
        argvlen.push_back(commands[i].length());
    }

    return redis_command_argv(commands[1], argv.size(), argv.data(), argvlen.data());
}


std::ostringstream& Cluster::set_error(ErrorE e) {
    errno_ = e;
    error_.str("");
    return error_;
}
int Cluster::parse_startup(const char *startup) {
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
            if( startup_nodes_.insert(node).second ) {
                DEBUGINFO("add parse startup " << node.host << ":" << node.port);
            }
        }
        if( p2 )
            p1 = p2+1;
        else
            break;
    }while(1);
    
    free( tmp );
    return startup_nodes_.size();
}

int Cluster::load_slots_cache() {

    int start, end;
    int count = 0;
    redisReply *reply, *subr, *innr;
    NodePoolType newnodes;
    
    NodePoolIter citer = startup_nodes_.begin();
    for(; citer!=startup_nodes_.end(); citer++) {

        redisContext *c = redisConnect(citer->host.c_str(), citer->port);
        if( c && c->err ) {
            redisFree( c );
            continue;
        }

        reply = (redisReply *)redisCommand(c, "cluster slots");
        if( !reply ) {
            redisFree(c);
            continue;
        } else if( reply->type==REDIS_REPLY_ERROR ) {
            redisFree(c);
            freeReplyObject(reply);
            continue;
        }

        for(size_t i=0; i<reply->elements; i++) {

            subr = reply->element[i];
            if( subr->elements<3
                || subr->element[0]->type!=REDIS_REPLY_INTEGER
                || subr->element[1]->type!=REDIS_REPLY_INTEGER 
                || subr->element[2]->type!=REDIS_REPLY_ARRAY )
                continue;

            start = subr->element[0]->integer;
            end = subr->element[1]->integer;
            innr = subr->element[2];

            if( innr->elements<2 
                || innr->element[0]->type!=REDIS_REPLY_STRING 
                || innr->element[1]->type!=REDIS_REPLY_INTEGER )
                continue;

            NodeInfoType node;
            node.host = innr->element[0]->str;
            node.port = innr->element[1]->integer;

            for(int jj=start; jj<=end; jj++)
                slots_[jj] = node;
            count += (end-start+1);

            if( newnodes.insert(node).second )
                DEBUGINFO("add startup " << node.host << ":" << node.port);
           

        }//for i

        redisFree(c);
        freeReplyObject(reply);
        break;

    }//for citer

    if( citer!=startup_nodes_.end() )  {
        DEBUGINFO("load_slots_cache count " << count << " from " << citer->host << ":" << citer->port);
    }
    else {
        DEBUGINFO("load_slots_cache fail from all startup node");
    }

    //append new nodes
    for( citer=newnodes.begin(); citer!=newnodes.end(); citer++ )
        startup_nodes_.insert( *citer );

    return count;
}

int Cluster::clear_slots_cache() {
    slots_.clear();
    slots_.resize(HASH_SLOTS);
    return 0;
}

redisContext *Cluster::get_random_from_startup(NodeInfoPtr pnode) {
    std::size_t len = startup_nodes_.size();
    if( len==0 )
        return NULL;

    struct timeval tp;
    gettimeofday(&tp, NULL);
    NodePoolCIter node_citer = startup_nodes_.begin();
    std::advance( node_citer, tp.tv_usec % len );//random position

    for( size_t i=0; i<len; i++, node_citer++ ) {

        if( node_citer==startup_nodes_.end() ) //reset
            node_citer = startup_nodes_.begin();

        ConnectionsCIter conn_citer = connections_.find( *node_citer );
        redisContext *c = NULL;

        if( conn_citer==connections_.end() ) {

            c = redisConnect(node_citer->host.c_str(), node_citer->port);
            if( c && c->err )  { //try next node
                redisFree( c );
                continue;
            }
            *pnode = *node_citer;
            connections_.insert( std::make_pair(*node_citer, c) );

        } else { //connection already exists

            *pnode = conn_citer->first;
            c = (redisContext *)conn_citer->second;

        }
        DEBUGINFO("get random node from startup " << pnode->host << ":" << pnode->port);
        return c;

    }
    return NULL;
}

uint16_t Cluster::get_key_hash(const std::string &key) {
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

redisReply* Cluster::redis_command_argv(const std::string& key, int argc, const char **argv, const size_t *argvlen) {
    uint16_t hashing = get_key_hash(key);
    int ttl = 5;
    NodeInfoType node;
    redisContext *c = NULL;
    redisReply *reply = NULL;
    bool try_random_node = false;
 
    set_error(E_OK); 

    if( load_slots_asap_ ) {
        load_slots_asap_ = false;
        load_slots_cache();
    }

    int slot = hashing % HASH_SLOTS;
    while( ttl>0 ) {
        ttl--;
        DEBUGINFO("ttl " << ttl);
        c = NULL;

        if( try_random_node ) {

            try_random_node = false;
            DEBUGINFO("try random node");
            c = get_random_from_startup(&node);
            if( !c ) {
                set_error(E_IO) << "try random from startup fail";
                return NULL;
            }

        } else {

            node = slots_[slot];
            if( node.host.empty() ) { //not hit

                DEBUGINFO("slot not hit, try get connection from startup nodes.");
                c = get_random_from_startup(&node);
                if( !c ) {
                    set_error(E_SLOT_MISSED) << "slot missed " << slot << ", key. and try get random node fail. " << key;
                    return NULL;
                }

            } else {

                DEBUGINFO("slot " << slot << " hit at " << node.host << ":" << node.port);

            }

        }
        
        if( !c ) {

            ConnectionsCIter citer = connections_.find( node );
            if( citer==connections_.end() ) { 

                c = redisConnect( node.host.c_str(), node.port );
                if( c && c->err )  { //next ttl
                    redisFree( c );
                    set_error(E_IO) << "redis connect error " << node.host << ":" << node.port;
                    try_random_node = true;//try random next ttl
                    DEBUGINFO("redis connect error " << node.host << ":" << node.port);
                    continue;
                }
                connections_.insert( std::make_pair(node, c) );

            } else {//connection already exists

                c = (redisContext *)citer->second;

            }

        }
        
        reply = (redisReply *)redisCommandArgv(c, argc, argv, argvlen);
        if( !reply ) {//next ttl

            redisFree( c );
            connections_.erase( node );
            set_error(E_IO) << "redisCommandArgv error. " << c->errstr << "(" << c->err << ")";
            try_random_node = true;//try random next ttl
            DEBUGINFO("redisCommandArgv error. " << c->errstr << "(" << c->err << ")");

        } else if( reply->type==REDIS_REPLY_ERROR 
            &&(!strncmp(reply->str,"MOVED",5) || !strcmp(reply->str,"ASK")) ) { //next ttl

            char *p = reply->str, *s; 
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

            load_slots_asap_ = true;//cluster nodes must have being changed, load slots cache as soon as possible.
            DEBUGINFO( "redirect to [" << slot << "] " << slots_[slot].host << ", " << slots_[slot].port);

        } else {

            return reply;

        }

        //next ttl
    }

    set_error(E_TTL) << "max ttl fail";
    return NULL;
}

int Cluster::test_parse_startup(const char *startup) {
    return parse_startup( startup );
}

Cluster::NodePoolType& Cluster::get_startup_nodes() {
    return startup_nodes_;
}
int Cluster::test_key_hash(const std::string &key) {
    return get_key_hash(key);
}

}//namespace cluster
}//namespace redis
