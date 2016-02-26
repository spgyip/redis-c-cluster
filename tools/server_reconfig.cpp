/* For testing client functions, this program automatically changes the cluster servers
 *
 * It uses ./redis-trib.rb(see http://redis.io/topics/cluster-tutorial) and ./redis-cli commands
 * so before run the program you should make sure redis-trib.rb and redis-cli already existed in
 * the CURRENT directory
 */
#include <stdint.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <iostream>
#include <list>
#include <vector>
#include <cassert>
#include <sstream>


#ifdef DEBUG
#define DBG_TRACE_CMD(cmd) fprintf(stderr, "[CMD] %s %s\r\n", cur_time(), cmd)
#define DBG_SHELL_QUIET ""
#else
#define DBG_TRACE_CMD(cmd)
#define DBG_SHELL_QUIET " 2>&1 >/dev/null"
#endif

#define DBG_ERR(format, args...) fprintf(stderr, "[WARN] %s " format "\r\n", cur_time(), ##args)
#define DBG_INFO(format, args...) fprintf(stderr, "[INFO] %s " format "\r\n", cur_time(), ##args)

#define MAX_LINE_LEN 300

typedef struct {
    std::string id;
    std::string host;
    std::string port;
} Node;
typedef std::list<Node *> NodeSet;

typedef struct {
    uint32_t slots;
    NodeSet  nodes;  // node in front is master, others are slaves
} Group;
typedef std::list<Group *> GroupSet;

typedef struct {
    std::string  unit;  // first slot set  covered by the Master, like '1132-3345' or '8893'
    uint32_t     count; // total slot count covered by the Master
} SlotsDistr;


GroupSet g_cluster;

char *cur_time() {
    static char cur_time_buf[50];
    time_t      now = time(NULL);
    struct tm *result = localtime(&now);
    snprintf(cur_time_buf, sizeof(cur_time_buf), "%04d-%02d-%02d.%02d:%02d:%02d",
             result->tm_year + 1900, result->tm_mon + 1, result->tm_mday,
             result->tm_hour, result->tm_min, result->tm_sec);
    return cur_time_buf;
}

/* note: the length of line output by running command 'cmd', and 'check_exist' should be no more than MAX_LINE_LEN
*/
bool run_shell(const char *cmd, const char *check_exist) {

    char chkcmd[MAX_LINE_LEN + MAX_LINE_LEN + 50];
    snprintf(chkcmd, sizeof(chkcmd), "%s |grep -q -F \"%s\"" DBG_SHELL_QUIET,cmd, check_exist);
    DBG_TRACE_CMD(chkcmd);
    int ret = system(chkcmd);
    if(WIFEXITED(ret) && WEXITSTATUS(ret) == 0) {
        return true;
    } else {
        return false;
    }
}


/* note: the length of line output by running command 'cmd', and 'check' should be no more than MAX_LINE_LEN
*/
bool run_shell(const char *cmd, const char *check, std::vector<std::string> &output) {
    int idx_last;
    output.clear();

    DBG_TRACE_CMD(cmd);

    FILE *result = popen(cmd, "r");
    if(!result) {
        perror(NULL);
        return false;
    }

    char buf[MAX_LINE_LEN];
    char chkcmd[MAX_LINE_LEN + MAX_LINE_LEN + 50];

    while(fgets(buf,sizeof(buf),result)) {
        idx_last = strlen(buf) - 1;
        if(idx_last >= 0 && buf[idx_last] == '\n') {
            buf[idx_last] = '\0';
        }
        snprintf(chkcmd, sizeof(chkcmd), "echo \"%s\"|grep -q -E \"%s\"" DBG_SHELL_QUIET, buf, check);
        DBG_TRACE_CMD(chkcmd);
        int ret = system(chkcmd);
        if(WIFEXITED(ret) && WEXITSTATUS(ret) != 0) {
            DBG_ERR("check \"%s\" fail", chkcmd);
            fclose(result);
            return false;
        }
        output.push_back(buf);
    }
    fclose(result);
    return true;
}

void clear_servers() {
    GroupSet::iterator itg = g_cluster.begin();
    for(; itg != g_cluster.end(); itg++) {
        Group *grp = *itg;
        assert(grp);
        NodeSet::iterator itn = grp->nodes.begin();
        for(; itn != grp->nodes.end(); itn++) {
            Node *node = *itn;
            assert(node);
            delete node;
        }
        delete grp;
    }
    g_cluster.clear();

}
void load_servers(std::string &host, std::string &port) {

    static unsigned int rand_grop_seed = (unsigned int)time(NULL);
    std::vector<SlotsDistr> slots_distr;

    char cmd[MAX_LINE_LEN];
    std::vector<std::string> output;

    clear_servers();

    /* get slots distributions */

    snprintf(cmd, sizeof(cmd), "./redis-trib.rb check %s:%s "
             "|grep -F \") master\" "
             "|sed 's/: (0 slots)/:- (0 slots)/g'" // for master who have 0 slot
             "|tr \", \" \":\""
             "|awk -F: '{print $5\"(\"$0}'"
             "|awk -F\\( '{print $1\" \"$3}'"
             "|awk -F: '{print $1}'", host.c_str(), port.c_str());
    if(!run_shell(cmd, "^[0-9-]+ [0-9]+$", output)) { /* check string like '1132-3345 8893' or '1132 8893'*/
        return;
    }
    for(size_t idx = 0; idx < output.size(); idx++) {
        size_t pos = output[idx].find(' ');
        SlotsDistr sld = {output[idx].substr(0, pos), atoi(output[idx].substr(pos+1).c_str())};
        slots_distr.push_back(sld);
    }

    /* get all masters */

    for(size_t idx = 0; idx < slots_distr.size(); idx++) {

        /* filter specific master node by it's first slot set
           sed 's/connected$/connected -/g' : for master who have 0 slot
           grep -E \"connected.* %s\" : for master who have slots number less than 10
         */
        snprintf(cmd, sizeof(cmd), "echo \"cluster nodes\" | ./redis-cli -h %s -p %s "
                 "|grep -F \"master - \" |sed 's/connected$/connected -/g' |grep -E \"connected.* %s\" |awk  '{print $1\" \"$2}'",
                 host.c_str(), port.c_str(), slots_distr[idx].unit.c_str());

        if(!run_shell(cmd, "^[a-z0-9]+ [0-9.]+:[0-9]+$", output)) { /* check string like 'ee6a90e9e077cdeec98aede832b485b997821188 192.168.102.43:7003' */
            clear_servers();
            return;
        }
        if(output.size() != 1) {
            clear_servers();
            return;  // if server is abnormal
        }

        /* save master's id address and slots count */

        size_t pos_addr = output[0].find(' ');
        size_t pos_port = output[0].find(':');

        Node *node = new Node;
        assert(node);
        node->id = output[0].substr(0, pos_addr);
        node->host = output[0].substr(pos_addr + 1, (pos_port - pos_addr) - 1);
        node->port = output[0].substr(pos_port + 1);
        //DBG_INFO("get master node [%s] [%s]:[%s]", node->id.c_str(), node->host.c_str(), node->port.c_str() );
        Group *grp = new Group;
        assert(grp);
        grp->nodes.push_back(node);
        grp->slots = slots_distr[idx].count;

        g_cluster.push_back(grp);
    }

    if(g_cluster.size() == 0) {
        DBG_ERR("got 0 master");
        return;
    }

    //put a group to header randomly

    size_t distance = rand_r(&rand_grop_seed) % g_cluster.size();
    GroupSet::iterator itg = g_cluster.begin();
    std::advance(itg, distance);
    if(itg != g_cluster.begin()) {
        Group *grp = *itg;
        g_cluster.erase(itg);
        g_cluster.insert(g_cluster.begin(), grp);
    }

    /* get all slaves per master */

    itg = g_cluster.begin();
    for(; itg != g_cluster.end(); itg++) {
        Group *grp = *itg;
        NodeSet::iterator itn = grp->nodes.begin();
        assert(itn != grp->nodes.end());
        Node *node = *itn;

        snprintf(cmd, sizeof(cmd), "echo \"cluster slaves %s \" | ./redis-cli -h %s -p %s "
                 "|grep -E \" connected$\" "
                 "|tr  \"\\\"\" \" \""
                 "|awk '{print $1\" \"$2}'",
                 node->id.c_str(), host.c_str(), port.c_str());

        if(!run_shell(cmd, "^[a-z0-9]+ [0-9.]+:[0-9]+$", output)) { /* check string like 'b854c21f83132f6f1b6266f4ef1443bfcd04bb42 192.168.102.43:7008' */
            clear_servers();
            return;
        }
        for(size_t idx = 0; idx < output.size(); idx++) {
            size_t pos_addr = output[idx].find(' ');
            size_t pos_port = output[idx].find(':');

            Node *node = new Node;
            assert(node);
            node->id = output[idx].substr(0, pos_addr);
            node->host = output[idx].substr(pos_addr + 1, (pos_port - pos_addr) - 1);
            node->port = output[idx].substr(pos_port + 1);
            //DBG_INFO("get slave node [%s] [%s]:[%s]", node->id.c_str(), node->host.c_str(), node->port.c_str() );

            grp->nodes.push_back(node);
        }
    }

    std::ostringstream ss;
    ss << "load_servers got servers:\r\n";
    itg = g_cluster.begin();
    for(int seq = 1; itg != g_cluster.end(); itg++, seq++) {
        const Group &grp = **itg;
        ss << "Group "<<seq << "/"<<g_cluster.size()<<" has "<< grp.slots <<" slots, with nodes:\r\n";
        for(NodeSet::const_iterator itn = grp.nodes.begin(); itn != grp.nodes.end(); itn++) {
            const Node &node = **itn;
            if(itn == grp.nodes.begin()) {
                ss << node.id << " " << node.host <<":"<< node.port << " - M\r\n"; //master
            } else {
                ss <<"    "<< node.id << " " << node.host <<":"<< node.port << " - S\r\n"; //slave
            }
        }
    }
    DBG_INFO("%s", ss.str().c_str());
}

void fix_cluster(std::string &host, std::string &port) {
#define CLUSTER_FIX_ROUNDS 5

    char cmd[MAX_LINE_LEN];

    int retry = 0;
    for(; retry < CLUSTER_FIX_ROUNDS; retry++,sleep(5)) {
        DBG_INFO("fix_cluster: check and fix(if necessary) round %d/%d...", retry + 1, CLUSTER_FIX_ROUNDS);

        snprintf(cmd, sizeof(cmd), "./redis-trib.rb fix %s:%s 2>&1 >/dev/null", host.c_str(), port.c_str());
        DBG_TRACE_CMD(cmd);
        system(cmd);

        snprintf(cmd, sizeof(cmd), "./redis-trib.rb check %s:%s", host.c_str(), port.c_str());
        if(!run_shell(cmd, "[WARNING]") && !run_shell(cmd, "[ERR]")) {
            DBG_INFO("fix_cluster: check/fix successful");
            break;
        }
    }
    if(retry >= CLUSTER_FIX_ROUNDS) {
        DBG_ERR("cluster in abnormal status, please fix it manually");
    }
#undef CLUSTER_FIX_ROUNDS
}

void failover(std::string &host, std::string &port) {
    char cmd[MAX_LINE_LEN];

    load_servers(host, port);

    if(g_cluster.size() < 1) {
        DBG_ERR("failover: no group in cluster");
        return;
    }

    GroupSet::iterator itg = g_cluster.begin();
    Group *grp = *itg;

    if(grp->nodes.size() < 2) {
        DBG_ERR("failover: no slave node in group");
        return;
    }
    Node *node = *(grp->nodes.begin());
    grp->nodes.pop_front();       // move master node to tail
    grp->nodes.push_back(node);
    node =  *(grp->nodes.begin());// failover
    snprintf(cmd, sizeof(cmd), "echo \"cluster failover\" | ./redis-cli -h %s -p %s" DBG_SHELL_QUIET,
             node->host.c_str(), node->port.c_str());
    DBG_INFO("failover %s:%s begin...",node->host.c_str(), node->port.c_str());

    DBG_TRACE_CMD(cmd);
    int ret = system(cmd);
    if(WIFEXITED(ret) && WEXITSTATUS(ret) != 0) {
        DBG_ERR("failover %s:%s fail",node->host.c_str(), node->port.c_str());
        return;
    }

    g_cluster.pop_front();
    g_cluster.push_back(grp); // move this group to tail

}
void reshard(std::string &host, std::string &port) {
    Group *grp_from;
    Group *grp_to;
    char cmd[MAX_LINE_LEN];

    load_servers(host, port);

    if(g_cluster.size() < 2) {
        DBG_ERR("reshard: number of groups less than 2");
        return;
    }
    GroupSet::iterator itg = g_cluster.begin();
    grp_from = *itg;
    itg++;
    for(; itg != g_cluster.end(); itg++) {
        grp_to = *itg;
        if(grp_from->slots < grp_to->slots) {
            grp_from = grp_to;
        }
    }

    uint32_t left_count = (uint32_t)(100000*(rand()/(RAND_MAX+0.1))) % (grp_from->slots/2);
    while(left_count == 0) {
        left_count = (uint32_t)(100000*(rand()/(RAND_MAX+0.1))) % (grp_from->slots/2);
    }
    uint32_t every_count = left_count / (g_cluster.size() - 1);
    Node    *snode;
    Node    *dnode;
    uint32_t count;
    NodeSet::iterator itn = grp_from->nodes.begin();
    if(itn == grp_from->nodes.end()) {
        DBG_ERR("reshard: no node exists in source group");
        return;
    }
    snode = *itn;

    itg = g_cluster.begin();
    for(; itg != g_cluster.end(); itg++) {
        if(*itg == grp_from) {
            continue;
        }

        grp_to = *itg;
        itn = grp_to->nodes.begin();
        if(itn == grp_to->nodes.end()) {
            DBG_ERR("reshard: no node exists in destination group");
            return;
        }
        dnode  = *itn;

        if(left_count - every_count >= every_count) {
            count = every_count; // not last destination node
        } else {
            count = left_count; // last destination node
        }
        assert(count > 0);
        snprintf(cmd, sizeof(cmd), "./redis-trib.rb reshard "
                 "--from %s --to %s --slots %u --yes %s:%s " DBG_SHELL_QUIET,
                 snode->id.c_str(), dnode->id.c_str(), count,
                 snode->host.c_str(), snode->port.c_str());
        DBG_INFO("reshard %u slots from %s to %s begin...", count, snode->id.c_str(), dnode->id.c_str());
        DBG_TRACE_CMD(cmd);
        int ret = system(cmd);
        if(WIFEXITED(ret) && WEXITSTATUS(ret) != 0) {
            DBG_ERR("reshard %u slots from %s to %s fail", count, snode->id.c_str(), dnode->id.c_str());
            return;
        }
    }
}

int main(int argc, char **argv) {
    unsigned int interval;
    if( argc < 4 ) {
        DBG_ERR("usage: %s interval host port\r\n interval: seconds waiting for to start next operations", argv[0]);
        return 1;
    }
    interval = (unsigned int)atoi(argv[1]);
    std::string host(argv[2]);
    std::string port(argv[3]);

    for(;;) {
        fix_cluster(host, port);
        sleep(10);
        reshard(host, port);
        sleep(10);
        failover(host, port);
        sleep(10);
        DBG_INFO("waiting %d seconds to start next round...", interval);
        sleep(interval);
    }
}

#undef DBG_TRACE_CMD
#undef MAX_LINE_LEN

