/* Copyright (C)
 * 2015 - supergui@live.cn
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * A c++ client library for redis cluser, simple wrapper of hiredis.
 * Inspired by antirez's (antirez@gmail.com) redis-rb-cluster.
 *
 */
#ifndef REDIS_CLUSTER_H_
#define REDIS_CLUSTER_H_

#include <string>
#include <vector>
#include <list>
#include <set>
#include <sstream>
#include <stdint.h>

struct redisContext;
struct redisReply;

namespace redis {
namespace cluster {


class Node;
struct CompareNodeFunc;

class Node {
public:
    Node(const std::string& host, int port, int max_conn = -1);
    ~Node();

    void *get_conn();
    void put_conn(void *conn);

    /**
         * A comparison function for equality;
         * This is required because the hash cannot rely on the fact
         * that the hash function will always provide a unique hash value for every distinct key
         * (i.e., it needs to be able to deal with collisions),
         * so it needs a way to compare two given keys for an exact match.
         * You can implement this either as a class that overrides operator(),
         * or as a specialization of std::equal,
         * or -easiest of all -by overloading operator==() for your key type (as you did already).
         */
    bool operator==(const Node &other) const {
        return host_ == other.host_ && port_ == other.port_;
    }

    bool operator<(const Node &rs) const {
        if( host_<rs.host_ )
            return true;
        else if ( host_==rs.host_ )
            return port_<rs.port_;
        else
            return false;
    }
    bool smaller(const Node &rs) const {
        if( host_<rs.host_ )
            return true;
        else if ( host_==rs.host_ )
            return port_<rs.port_;
        else
            return false;
    }
    std::string simple_dump() const;

private:
    std::string host_;
    int         port_;
    int         max_conn_;

    std::list<void *>  connections_;
};


struct CompareNodeFunc {
    bool operator()(const Node* l, const Node* r) const {
        return (*l) < (*r);
    }
};

class Cluster {
public:
    const static int HASH_SLOTS = 16384;

    enum ErrorE {
        E_OK = 0,
        E_COMMANDS = 1,
        E_SLOT_MISSED = 2,
        E_IO = 3,
        E_TTL = 4,
        E_OTHERS = 5
    };

    typedef std::set<Node *, CompareNodeFunc> NodePoolType;

    Cluster();
    virtual ~Cluster();

    /**
         *  Setup with startup nodes.
         *
         * @param
         *  startup - '127.0.0.1:7000, 127.0.0.1:8000'
         *  lazy    - if set false, load slot cache immediately when setup.
         *            otherwise the slots cache will be loaded later when first command is executed..
         *
         * @return
         *  0 - success
         *  <0 - fail
         */
    int setup(const char *startup, bool lazy);

    /**
         * Caller should call freeReplyObject to free reply.
         *
         * @return
         *  not NULL - succ
         *  NULL     - error
         *             get the last error message with function err() & errstr()
         */
    redisReply* run(const std::vector<std::string> &commands);

    inline int err() const {
        return errno_;
    }
    inline std::string strerr() const {
        return error_.str();
    }

public:/* for unittest */
    int test_parse_startup(const char *startup);
    NodePoolType& get_startup_nodes();
    int test_key_hash(const std::string &key);

private:
    std::ostringstream& set_error(ErrorE e);
    bool add_node(const std::string &host, int port, Node *&rpnode);
    int parse_startup(const char *startup);
    int load_slots_cache();
    int clear_slots_cache();
    Node *get_random_node(const Node *last);

    /**
     *  Support hash tag, which means if there is a substring between {} bracket in a key, only what is inside the string is hashed.
     *  For example {foo}key and other{foo} are in the same slot, which hashed with 'foo'.
     */
    uint16_t get_key_hash(const std::string &key);

    /**
     *  Agent for connecting and run redisCommandArgv.
     *  Max ttl(default 5) retries or redirects.
     *
     * @return
     *  not NULL: success, return the redisReply object. Caller should call freeReplyObject to free reply object.
     *  NULL    : error
     */
    redisReply* redis_command_argv(const std::string& key, int argc, const char **argv, const size_t *argvlen);

    NodePoolType        node_pool_;
    std::vector<Node *> slots_;
    bool                load_slots_asap_;
    ErrorE              errno_;
    std::ostringstream  error_;
};

}//namespace cluster
}//namespace redis

#endif
