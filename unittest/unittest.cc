#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <vector>
#include <gtest/gtest.h>
#include "../redis_cluster.h"


class ClusterTestObj : public ::testing::Test {
public:
    ClusterTestObj() {
        cluster_ = NULL;
    }
    ~ClusterTestObj() {
    }

    virtual void SetUp() {
        cluster_ = new redis::cluster::Cluster();
    }
    virtual void TearDown() {
        if( cluster_ ) {
            delete cluster_;
        }
    }
    redis::cluster::Cluster *cluster_;
};

TEST_F(ClusterTestObj, errinfo) {
    std::vector<std::string> cmd;
    ASSERT_TRUE(cluster_->setup("",true) == 0);
    redisReply* ret = cluster_->run(cmd);
    ASSERT_FALSE(ret);
    ASSERT_EQ(cluster_->err(), redis::cluster::Cluster::E_COMMANDS);
    ASSERT_TRUE(cluster_->strerr().find("not supported") != std::string::npos);
}

TEST_F(ClusterTestObj, test_parse_startup) {

    ASSERT_TRUE(cluster_->setup("",true) == 0);
    /* one host
     */
    using redis::cluster::Cluster;
    Cluster::NodePoolType::iterator iter;
    Cluster::NodePoolType v;
    redis::cluster::Node node7("127.0.0.1", 7000);
    redis::cluster::Node node8("128.0.0.1", 8000);
    redis::cluster::Node node9("129.0.0.1", 9000);

    int rv = cluster_->test_parse_startup("127.0.0.1:7000, 127.0.0.1:7000");
    v = cluster_->get_startup_nodes();
    ASSERT_EQ(rv, 1) << "parse return is not 1";
    ASSERT_EQ(v.size(), 1) << "node size is not 1";
    ASSERT_TRUE( v.find( &node7 )!=v.end() ) << "node 127.0.0.1:7000 not found in cluster nodes";

    /* one host with spaces */

    rv = cluster_->test_parse_startup(" 127.0.0.1 : 7000 ");
    v = cluster_->get_startup_nodes();
    ASSERT_EQ(rv, 1) << "parse return is not 1";
    ASSERT_EQ(v.size(), 1) << "node size is not 1";
    ASSERT_TRUE( v.find( &node7 )!=v.end() ) << "node 127.0.0.1:7000 not found in cluster nodes";

    /* multiple hosts */

    rv = cluster_->test_parse_startup("127.0.0.1:7000,128.0.0.1:8000,129.0.0.1:9000");
    v = cluster_->get_startup_nodes();
    ASSERT_EQ(rv, 3) << "parse return is not 3";
    ASSERT_EQ(v.size(), 3) << "node size is not 3";
    ASSERT_TRUE( v.find(&node7)!=v.end() ) << "node 127.0.0.1:7000 not found in cluster nodes";
    ASSERT_TRUE( v.find(&node8)!=v.end() ) << "node 128.0.0.1:8000 not found in cluster nodes";
    ASSERT_TRUE( v.find(&node9)!=v.end() ) << "node 129.0.0.1:9000 not found in cluster nodes";

    /* multiple hosts, with spaces */

    rv = cluster_->test_parse_startup(" 127.0.0.1:7000, 128.0.0.1 :8000 , 129.0.0.1:9000 , ");
    v = cluster_->get_startup_nodes();
    ASSERT_EQ(rv, 3) << "parse return is not 3";
    ASSERT_EQ(v.size(), 3) << "node size is not 3";
    ASSERT_TRUE( v.find(&node7)!=v.end() ) << "node 127.0.0.1:7000 not found in cluster nodes";
    ASSERT_TRUE( v.find(&node8)!=v.end() ) << "node 128.0.0.1:8000 not found in cluster nodes";
    ASSERT_TRUE( v.find(&node9)!=v.end() ) << "node 129.0.0.1:9000 not found in cluster nodes";
}

TEST(CaseHashing, test_hash_key) {
    redis::cluster::Cluster *cluster = new redis::cluster::Cluster();
    std::string key1 = "supergui";
    std::string key2 = "{supergui}";
    std::string key3 = "abc{supergui}";
    std::string key4 = "{supergui}123";


    int hash1 = cluster->test_key_hash(key1);
    int hash2 = cluster->test_key_hash(key2);
    int hash3 = cluster->test_key_hash(key3);
    int hash4 = cluster->test_key_hash(key4);

    ASSERT_EQ( hash1, hash2 );
    ASSERT_EQ( hash1, hash3 );
    ASSERT_EQ( hash1, hash4 );

    delete cluster;
}

TEST(CaseNodePool, test_NodePoolType) {
    redis::cluster::Cluster::NodePoolType node_pool;
    redis::cluster::Cluster::NodePoolType::iterator iter;
    redis::cluster::Node node1("126.0.0.1", 6000);
    redis::cluster::Node node2("126.0.0.1", 6001);
    redis::cluster::Node node3("126.0.0.2", 6000);
    redis::cluster::Node node4("126.0.0.2", 6001);

    redis::cluster::Node node11("126.0.0.1", 6000);
    redis::cluster::Node node22("126.0.0.1", 6001);
    redis::cluster::Node node33("126.0.0.2", 6000);
    redis::cluster::Node node44("126.0.0.2", 6001);

    /* insert differents sucessfully */

    ASSERT_TRUE(node_pool.insert(&node1).second);
    ASSERT_TRUE(node_pool.insert(&node2).second);
    ASSERT_TRUE(node_pool.insert(&node3).second);
    ASSERT_TRUE(node_pool.insert(&node4).second);

    /* insert duplicates fail */

    ASSERT_FALSE(node_pool.insert(&node11).second);
    ASSERT_FALSE(node_pool.insert(&node22).second);
    ASSERT_FALSE(node_pool.insert(&node33).second);
    ASSERT_FALSE(node_pool.insert(&node44).second);

    /* find node1 by it's ip and port (not address) sucessfully */

    iter = node_pool.find(&node11);
    ASSERT_NE(iter, node_pool.end());
    ASSERT_EQ(*iter, &node1);

    /* find node2 by it's ip and port (not address) sucessfully */

    iter = node_pool.find(&node22);
    ASSERT_NE(iter, node_pool.end());
    ASSERT_EQ(*iter, &node2);

    /* delete by ip and port */
    node_pool.erase(&node11);
    node_pool.erase(&node22);

    ASSERT_EQ(node_pool.find(&node1), node_pool.end());
    ASSERT_EQ(node_pool.find(&node2), node_pool.end());

}


int main(int argc, char *argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
