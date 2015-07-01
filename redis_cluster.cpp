#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <iostream>
#include <string>
#include <hiredis/hiredis.h>
#include "redis_cluster.hpp"
#include "deps/crc16.c"

#ifdef DEBUG
#define DEBUGINFO(msg) std::cerr << "[DEBUG] "<< msg << std::endl;
#else
#define DEBUGINFO(msg)
#endif

namespace redis {
namespace cluster {

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

    return 0;
}

redisReply* Cluster::run(const std::vector<std::string> &commands) {
    std::vector<const char *> argv;
    std::vector<size_t> argvlen;

    if( commands.size()<2 ) {
        set_error(E_COMMANDS) << "commands size should at lease 2";
        return NULL;
    }
    
    std::string cmd = to_upper(commands[0]);
    if( cmd!="GET" 
        && cmd!="SET"
        && cmd!="HGET"
        && cmd!="HSET" ) {
        set_error(E_COMMANDS) << "command [" << commands[0] <<"] not support";
        return NULL;
    }

    for( size_t i=0; i<commands.size(); i++ ) {
        argv.push_back(commands[i].c_str());
        argvlen.push_back(commands[i].length());
    }

    return redis_command_argv(commands[1], argv.size(), argv.data(), argvlen.data());
}


std::ostringstream& Cluster::set_error(ErrorE e) {
    errno_ = e;
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

int Cluster::load_slots_cache() {
    
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
    DEBUGINFO("load_slots_cache count " << count << " from " << np->host << ":" << np->port);
    return count;
}

int Cluster::clear_slots_cache() {
    slots_.clear();
    slots_.resize(HASH_SLOTS);
    return 0;
}

redisContext *Cluster::get_random_from_startup(NodeInfoPtr pnode) {
    if( startup_nodes_.size()==0 ) {
        return NULL;
    }

    int s = time(NULL) % startup_nodes_.size();
    redisContext *c = NULL;
    for( ; s<(int)startup_nodes_.size(); s++ ) {

        NodeInfoType n = startup_nodes_[s];
        ConnectionsCIter citer = connections_.find(n);

        if( citer==connections_.end() ) {

            c = redisConnect(n.host.c_str(), n.port);
            if( c && c->err )  { //try next node
                redisFree( c );
                continue;
            }
            *pnode = n;
            connections_.insert( std::make_pair(n, c) );

        } else { //connection already exists

            *pnode = citer->first;
            c = (redisContext *)citer->second;

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
        c = NULL;

        if( try_random_node ) {
            try_random_node = false;
            DEBUGINFO("try random node ttl " << ttl);
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
                DEBUGINFO("slot hit " << node.host << ":" << node.port);
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

            load_slots_asap_ = true;
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

std::vector<Cluster::NodeInfoType>& Cluster::get_startup_nodes() {
    return startup_nodes_;
}
int Cluster::test_key_hash(const std::string &key) {
    return get_key_hash(key);
}

#if 0
Reply::Reply(const redisReply *rp):
type_(T_ERROR), 
integer_(0){
    type_ = static_cast<TypeE>(rp->type);
    switch (type_) {
        case T_ERROR:
        case T_STRING:
        case T_STATUS:
           str_ = std::string(rp->str, rp->len);
           break;
        case T_INTEGER:
           integer_ = rp->integer;
           break;
        case T_ARRAY:
            for (size_t i=0; i < rp->elements; ++i) {
                 elements_.push_back(Reply(rp->element[i]));
            }
            break;
        default:
            break;
    }
}
#endif
}//namespace cluster
}//namespace redis
