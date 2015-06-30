#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <vector>
#include <gtest/gtest.h>
#include "../redis_cluster.hpp"
//#include "../deps/crc16.c"

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

TEST_F(ClusterTestObj, test_parse_startup) {
    /* one host
     */
    int rv = cluster_->test_parse_startup("127.0.0.1:7000");
    std::vector<redis::cluster::Cluster::NodeInfoType> &v = cluster_->get_startup_nodes();
    ASSERT_EQ(rv, 1) << "parse return is not 1";
    ASSERT_EQ(v.size(), 1) << "node size is not 1";
    ASSERT_EQ( v[0].host, "127.0.0.1" ) <<  "host[0] is not '127.0.0.1'";
    ASSERT_EQ( v[0].port, 7000 ) << "port[0] is not 7000";

    /* one host with spaces
     */
    rv = cluster_->test_parse_startup(" 127.0.0.1 : 7000 ");
    v = cluster_->get_startup_nodes();
    ASSERT_EQ(rv, 1) << "parse return is not 1";
    ASSERT_EQ(v.size(), 1) << "node size is not 1";
    ASSERT_EQ( v[0].host, "127.0.0.1" ) <<  "host[0] is not '127.0.0.1'";
    ASSERT_EQ( v[0].port, 7000 ) << "port[0] is not 7000";

    /* multiple hosts
     */
    rv = cluster_->test_parse_startup("127.0.0.1:7000,128.0.0.1:8000,129.0.0.1:9000");
    v = cluster_->get_startup_nodes();
    ASSERT_EQ(rv, 3) << "parse return is not 3";
    ASSERT_EQ(v.size(), 3) << "node size is not 3";
    ASSERT_EQ( v[0].host, "127.0.0.1" ) <<  "host[0] is not '127.0.0.1'";
    ASSERT_EQ( v[0].port, 7000 ) << "port[0] is not 7000";
    ASSERT_EQ( v[1].host, "128.0.0.1" ) <<  "host[1] is not '128.0.0.1'";
    ASSERT_EQ( v[1].port, 8000 ) << "port[1] is not 8000";
    ASSERT_EQ( v[2].host, "129.0.0.1" ) <<  "host[2] is not '129.0.0.1'";
    ASSERT_EQ( v[2].port, 9000 ) << "port[2] is not 9000";

    /* multiple hosts, with spaces
     */
    rv = cluster_->test_parse_startup(" 127.0.0.1:7000, 128.0.0.1 :8000 , 129.0.0.1:9000 , ");
    v = cluster_->get_startup_nodes();
    ASSERT_EQ(rv, 3) << "parse return is not 3";
    ASSERT_EQ(v.size(), 3) << "node size is not 3";
    ASSERT_EQ( v[0].host, "127.0.0.1" ) <<  "host[0] is not '127.0.0.1'";
    ASSERT_EQ( v[0].port, 7000 ) << "port[0] is not 7000";
    ASSERT_EQ( v[1].host, "128.0.0.1" ) <<  "host[1] is not '128.0.0.1'";
    ASSERT_EQ( v[1].port, 8000 ) << "port[1] is not 8000";
    ASSERT_EQ( v[2].host, "129.0.0.1" ) <<  "host[2] is not '129.0.0.1'";
    ASSERT_EQ( v[2].port, 9000 ) << "port[2] is not 9000";
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

int main(int argc, char *argv[])
{
    ::testing::InitGoogleTest(&argc, argv);       
    return RUN_ALL_TESTS();
}
