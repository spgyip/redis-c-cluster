#include "redis_cluster.hpp"
#include "deps/crc16.c"
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <iostream>
#include <sstream>
#include <string>
#include <cassert>
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


/**
 * class Node
 */
Node::Node(const std::string& host, int port, int max_conn) {
    host_ = host;
    port_ = port;
    max_conn_ = max_conn;
}

Node::~Node() {

    std::list<void *>::iterator iter = connections_.begin();
    for(; iter!=connections_.end(); iter++) {
        redisContext *conn = (redisContext *)*iter;
        redisFree( conn );
    }

}

void *Node::get_conn() {

    redisContext *conn = NULL;
    for(; connections_.size()>0;) {
        conn = (redisContext *)connections_.back();
        connections_.pop_back();

        if( conn->err==REDIS_OK )
            break;

        redisFree( conn );
        conn = NULL;
    }

    if( !conn ) {
        conn = redisConnect(host_.c_str(), port_);
        if( conn && conn->err ) {
            redisFree( conn );
            conn = NULL;
        }
    }

    return conn;
}

void Node::put_conn(void *conn) {
    connections_.push_front( conn );
}

std::string Node::simple_dump() const {
    std::stringstream ss;
    ss<<"Node{"<< host_ << ":" << port_ << "}";
    return ss.str();
}

Cluster::Cluster()
    :load_slots_asap_(false), errno_(E_OK) {
}

Cluster::~Cluster() {

    // release node pool
    NodePoolType::iterator iter = node_pool_.begin();
    for(; iter!=node_pool_.end(); iter++) {
        Node *node = *iter;
        delete node;
    }
    node_pool_.clear();

}

int Cluster::setup(const char *startup, bool lazy) {

    if( parse_startup(startup)<0 ) {
        return -1;
    }

    slots_.resize( HASH_SLOTS );
    for(size_t i = 0; i<slots_.size(); i++) {
        slots_[i] = NULL;
    }

    if( !lazy && load_slots_cache()<0 ) {
        return -1;
    }

    if( lazy ) {
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

    do {
        std::ostringstream ss;
        ss << "#" << cmd << "#";
        if( strstr(UNSUPPORT, ss.str().c_str()) ) {
            set_error(E_COMMANDS) << "command [" << cmd << "] not supported";
            return NULL;
        }
    } while(0);

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

bool Cluster::add_node(const std::string &host, int port, Node *&rpnode) {
    Node *node = new Node(host, port);
    assert( node );

    std::pair<NodePoolType::iterator, bool> reti = node_pool_.insert(node);
    rpnode = *(reti.first);

    if(reti.second) {
        return true;
    } else {
        delete node;
        return false;
    }
}

int Cluster::parse_startup(const char *startup) {
    char *p1, *p2, *p3;
    char *tmp = (char *)malloc( strlen(startup)+1 );
    memcpy(tmp, startup, strlen(startup)+1);
    int  port;
    std::string host;

    p1 = p2 = tmp;
    do {
        p2 = strchr(p1, ',');
        if( p2 ) {
            *p2 = '\0';
        }
        p3 = strchr(p1, ':');
        if( p3 ) {

            *p3 = '\0';
            port = atoi(p3+1); //get port

            while(p1<p3 && *p1==' ')//trim left
                p1++;

            p3--;
            while(p3>p1 && *p3==' ')//trim right
                *(p3--) = '\0';

            host = p1;   //get host

            Node *node_in_pool;
            bool ret = add_node(host, port, node_in_pool);
            if(ret) {
                DEBUGINFO("parse startup add " << node_in_pool->simple_dump());
            } else {
                DEBUGINFO("parse startup duplicate " << node_in_pool->simple_dump() << " ignored");
            }
        }
        if( p2 )
            p1 = p2+1;
        else
            break;
    } while(1);

    free( tmp );
    return node_pool_.size();
}

int Cluster::load_slots_cache() {

    int start, end;
    int count = 0;
    redisReply *reply, *subr, *innr;
    Node *node;

    NodePoolType::iterator iter = node_pool_.begin();
    for(; iter != node_pool_.end(); iter++) {
        node = *iter;

        redisContext *c = (redisContext *)node->get_conn();
        if( !c ) {
            continue;
        }

        reply = (redisReply *)redisCommand(c, "cluster slots");
        if( !reply ) {
            node->put_conn(c);
            continue;
        } else if( reply->type==REDIS_REPLY_ERROR ) {
            freeReplyObject(reply);
            node->put_conn(c);
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

            Node *node_in_pool;
            bool ret = add_node(innr->element[0]->str, innr->element[1]->integer, node_in_pool);
            if(ret) {
                DEBUGINFO("insert new node "<< node_in_pool->simple_dump()<< " from cluster slots map" );
            }

            for(int jj=start; jj<=end; jj++)
                slots_[jj] = node_in_pool;

            count += (end-start+1);
        }//for i

        freeReplyObject(reply);
        node->put_conn(c);
        break;

    }//for citer

    if( iter != node_pool_.end() )  {
        DEBUGINFO("load_slots_cache count " << count <<"("<< (count == HASH_SLOTS? "complete":"incomplete!")<<") from " << node->simple_dump());
    } else {
        DEBUGINFO("load_slots_cache fail from all startup node");
    }

    return count;
}

int Cluster::clear_slots_cache() {
    slots_.clear();
    slots_.resize(HASH_SLOTS);
    for(size_t i = 0; i<slots_.size(); i++) {
        slots_[i] = NULL;
    }
    return 0;
}

Node *Cluster::get_random_node(const Node *last) {
    std::size_t len = node_pool_.size();
    if( len==0 )
        return NULL;

    struct timeval tp;
    gettimeofday(&tp, NULL);

    NodePoolType::iterator iter = node_pool_.begin();
    std::advance( iter, tp.tv_usec % len );//random position
    for(size_t i = 0; i < len; i++) {
        if(iter == node_pool_.end())
            iter = node_pool_.begin();
        DEBUGINFO("get_random_node try "<<(*iter)->simple_dump());
        if(*iter != last) {
            return *iter;
        }
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

    int ttl = 5;
    Node *node = NULL;
    redisContext *c = NULL;
    redisReply *reply = NULL;
    bool try_random_node = false;

    set_error(E_OK);

    if( load_slots_asap_ ) {
        load_slots_asap_ = false;
        load_slots_cache();
    }

    uint16_t hashing = get_key_hash(key);
    const int slot = hashing % HASH_SLOTS;

    while( ttl>0 ) {
        ttl--;
        DEBUGINFO("ttl " << ttl);

        if( try_random_node ) {

            try_random_node = false;
            DEBUGINFO("try random node");
            node = get_random_node(node);
            if( !node ) {
                set_error(E_IO) << "try random node: no avaliable node";
                return NULL;
            }
            DEBUGINFO("slot " << slot << " use random " << node->simple_dump());
        } else {//find slot

            node = slots_[slot];
            if( !node ) { //not hit
                DEBUGINFO("slot "<<slot<<"don't have node, try connection from random node.");
                try_random_node = true;//try random next ttl
                continue;
            }
            DEBUGINFO("slot " << slot << " hit at " << node->simple_dump());
        }

        c = (redisContext*)node->get_conn();
        if( !c ) {
            DEBUGINFO("get connection fail from " << node->simple_dump());
            try_random_node = true;//try random next ttl
            continue;
        }

        reply = (redisReply *)redisCommandArgv(c, argc, argv, argvlen);
        if( !reply ) {//next ttl

            DEBUGINFO("redisCommandArgv error. " << c->errstr << "(" << c->err << ")");
            set_error(E_IO) << "redisCommandArgv error. " << c->errstr << "(" << c->err << ")";
            node->put_conn(c);
            try_random_node = true;//try random next ttl
            continue;

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

            assert( slot == atoi(s+1) );

            s = strchr(p+1,':');    /* MOVED 3999[P]127.0.0.1[S]6381 */
            *s = '\0';

            Node *node_in_pool;
            bool ret = add_node(p+1, atoi(s+1), node_in_pool);
            if(ret) {
                DEBUGINFO("insert new node "<< node_in_pool->simple_dump()<< " from redirection" );
            } else {
                DEBUGINFO("redirect slot "<< slot <<" to " << node_in_pool->simple_dump())
            }

            slots_[slot] = node_in_pool;

            load_slots_asap_ = true;//cluster nodes must have being changed, load slots cache as soon as possible.
            freeReplyObject( reply );
            node->put_conn(c);
            continue;

        }
        node->put_conn(c);
        return reply;
    }

    set_error(E_TTL) << "max ttl fail";
    return NULL;
}

int Cluster::test_parse_startup(const char *startup) {
    return parse_startup( startup );
}

Cluster::NodePoolType & Cluster::get_startup_nodes() {
    return node_pool_;
}
int Cluster::test_key_hash(const std::string &key) {
    return get_key_hash(key);
}

}//namespace cluster
}//namespace redis
