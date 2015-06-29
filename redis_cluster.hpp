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
#include <unordered_map>

class RedisCluster {
public:
    const int HASH_SLOTS = 16384;

    typedef struct NodeInfoS{
        std::string host;
        int port;
        
        /* A comparison function for equality; 
         * This is required because the hash cannot rely on the fact 
         * that the hash function will always provide a unique hash value for every distinct key 
         * (i.e., it needs to be able to deal with collisions), 
         * so it needs a way to compare two given keys for an exact match. 
         * You can implement this either as a class that overrides operator(), 
         * or as a specialization of std::equal, 
         * or – easiest of all – by overloading operator==() for your key type (as you did already).
         */
        bool operator==(const struct NodeInfoS &other) const {
            return (host==other.host && port==other.port);
        }
    }NodeInfoType, *NodeInfoPtr, &NodeInfoRef;

    /*A hash function; 
     * this must be a class that overrides operator() and calculates the hash value given an object of the key-type. 
     * One particularly straight-forward way of doing this is to specialize the std::hash template for your key-type.
     */
    struct KeyHasherS {
        std::size_t operator()(const NodeInfoType &node) const {
            return (std::hash<std::string>()(node.host) ^ std::hash<int>()(node.port));
        }
    };

    typedef std::unordered_map<NodeInfoType, void *, KeyHasherS> ConnectionsType;//NodeInfoType=>redisContext
    typedef ConnectionsType::iterator ConnectionsIter;
    typedef ConnectionsType::const_iterator ConnectionsCIter;

    RedisCluster();

    virtual ~RedisCluster();
    
    /**
     * @brief
     *  Setup with startup nodes.
     *  Immediately loading slots cache from startup nodes.
     *
     * @param 
     *  startup - '127.0.0.1:7000, 127.0.0.1:8000'
     *
     * @return 
     *  0 - success
     *  <0 - fail
     */
    int setup(const char *startup);

    int get(const std::string &key, std::string &value);
    int set(const std::string &key, const std::string &value);

public:/* for unittest */
    int test_parse_startup(const char *startup);
    std::vector<NodeInfoType>& get_startup_nodes();

private:
    int parse_startup(const char *startup);
    int load_slots_cache();
    int clear_slots_cache();
    uint16_t get_key_hash(const std::string &key);

    std::vector<NodeInfoType> startup_nodes_;
    ConnectionsType connections_;
    std::vector<NodeInfoType> slots_;
};

#endif
