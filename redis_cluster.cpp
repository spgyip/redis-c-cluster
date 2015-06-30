#include <stdlib.h>
#include <iostream>
#include <string.h>
#include <string>
#include <hiredis/hiredis.h>
#include "redis_cluster.hpp"
#include "deps/crc16.c"

#define DEBUGINFO(msg) std::cerr << "[DEBUG] "<< msg << std::endl;

namespace redis {
namespace cluster {

static inline std::string to_upper(const std::string& in) {

    std::string out;
    out.reserve( in.length() );
    for(std::size_t i=0; i<in.length(); i++ ) {
        out[i] = std::toupper( in[i] );       
    }
    return out;
}

Cluster::Cluster():
        errno_(E_OK) {
}

Cluster::~Cluster() {
}

int Cluster::setup(const char *startup) {

    if( parse_startup(startup)<0 ) {
        return -1;
    }

    slots_.resize( HASH_SLOTS );

    if( load_slots_cache()<0 ) {
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
        || cmd!="SET"
        || cmd!="HGET"
        || cmd!="HSET" ) {
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
    return count;
}

int Cluster::clear_slots_cache() {
    slots_.clear();
    slots_.resize(HASH_SLOTS);
    return 0;
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
    int ttl = 3;
    NodeInfoType node;
    redisContext *c = NULL;
    redisReply *reply = NULL;
 
    set_error(E_OK); 

    int slot = hashing % HASH_SLOTS;
    while( ttl>0 ) {
        ttl--;

        node = slots_[slot];
        if( node.host.empty() ) {
            set_error(E_SLOT_MISSED) << "slot missed " << slot << ", key " << key;
            break;
        }

        ConnectionsCIter citer = connections_.find( node );
        if( citer==connections_.end() ) {
            c = redisConnect( node.host.c_str(), node.port );
            if( c && c->err )  {
                redisFree( c );
                set_error(E_IO) << "redis connect error " << node.host << ":" << node.port;
                continue;//retry 
            }
            connections_.insert( std::make_pair(node, c) );
        }

        citer = connections_.find( node );//never fail
        c = (redisContext *)citer->second;
        
        reply = (redisReply *)redisCommandArgv(c, argc, argv, argvlen);
        if( !reply && (c->err==REDIS_ERR_IO || c->err==REDIS_ERR_EOF )) { //retry ttl for io error
            redisFree( c );
            connections_.erase( citer );
            set_error(E_IO) << "redisCommandArgv error. " << c->errstr << "(" << c->err << ")";
            continue;
        } else if( !reply ) {
            redisFree( c );
            connections_.erase( citer );
            set_error(E_OTHER) << "redisCommandArgv error. " << c->errstr << "(" << c->err << ")";
            break;
        }
        else if( reply->type==REDIS_REPLY_ERROR 
            &&(!strncmp(reply->str,"MOVED",5) || !strcmp(reply->str,"ASK")) ) {

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

            DEBUGINFO( "redirect to [" << slot << "] " << slots_[slot].host << ", " << slots_[slot].port);
        }
        /* next ttl
         */
    }

    if( errno_==E_OK ) {
        set_error(E_TTL) << "max redirect fail";
    }
    return reply;
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
