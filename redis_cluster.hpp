#ifndef REDIS_CLUSTER_H_
#define REDIS_CLUSTER_H_

#include <string>
#include <vector>
#include <unordered_map>
#include <hiredis/hiredis.h>

class RedisCluster {
public:
    const int HASH_SLOTS = 16384;

    typedef struct {
        std::string host;
        int port;
    } NodeInfoType, *NodeInfoPtr, &NodeInfoRef;

    typedef std::unordered_map<std::string, redisContext *> ConnectionsType;
    typedef ConnectionsType::iterator ConnectionsIter;
    typedef ConnectionsType::const_iterator ConnectionsCIter;

    RedisCluster();
    virtual ~RedisCluster();
    
    int init(const char *startup);

    /* for unittest
     */
    int test_parse_startup(const char *startup);
    std::vector<NodeInfoType>& get_startup_nodes();

private:
    int parse_startup(const char *startup);

    /**
     * @brief load slots from startup
     *
     * @return number of slots loaded sucessful 
     */
    int load_slots_cache();
    int clear_slots_cache();

    std::vector<NodeInfoType> startup_nodes_;
    ConnectionsType connections_;
    std::vector<NodeInfoType> slots_;
};

#endif
