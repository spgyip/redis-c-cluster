#include "../redis_cluster.hpp"
#include <unistd.h>
#include <stdio.h>
#include <ncurses.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <iostream>
#include <map>
#include <hiredis/hiredis.h>

typedef struct {
    uint64_t read;
    uint64_t write;
    uint64_t read_t;
    uint64_t write_t;
    uint64_t read_ttl;
    uint64_t write_ttl;
    uint64_t read_error;
    uint64_t write_error;
    uint64_t unmatch;
} stat_item_t;

static const stat_item_t stat_item_initial = {0,0,0,0,0,0,0};
#define DUMP_STAT_ITEM(it) " r: "<< (it).read << " w: " << (it).write << \
                           " rt: " << (it).read_t << " wt: " << (it).write_t << \
                           " re: " << (it).read_error << " we: " << (it).write_error << \
                           " um: " << (it).unmatch << " "

typedef struct {
    int now_sec;
    int now_us;
    int last_us;
} time_period_t;

typedef struct {
    bool        is_running; // set by main thread , read by work thread
    pthread_t   tid;
    stat_item_t stat;       // set by work thread , read by main thread
} work_thread_data_t;

typedef std::map<std::string, std::string> CacheType;

volatile int  conf_load_of_thread = 6;
#define MAX_LOAD_OF_THREAD 10

volatile int  conf_threads_num = 4;
#define MAX_THREADS_NUM 20

volatile bool conf_is_running = true;

redis::cluster::Cluster *cluster_ = NULL;
CacheType          cache;
pthread_spinlock_t c_lock;

std::string get_random_key() {
    int n = 10000*(rand()/(RAND_MAX+0.1));
    std::ostringstream ss;
    ss << "key_" << n;
    return ss.str();
}

std::string get_random_value() {
    int n = 100000*(rand()/(RAND_MAX+0.1));
    std::ostringstream ss;
    ss << "value_" << n;
    return ss.str();
}

int check_point(int &now_us, int &now_sec, int &last_us) {
    struct timeval tv;
    gettimeofday( &tv, NULL );
    now_sec = tv.tv_sec;
    now_us = tv.tv_sec*1000000 + tv.tv_usec;

    int ret = now_us - last_us;
    last_us = now_us;
    return ret;
}

int redis_set(const std::string &key, const std::string& value) {
    int ret = 0;
    std::vector<std::string> commands;
    commands.push_back("SET");
    commands.push_back(key);
    commands.push_back(value);
    redisReply *reply = cluster_->run(commands);
    if( !reply ) {
        ret = -1;
    } else if( reply->type==REDIS_REPLY_STATUS && !strcmp(reply->str, "OK") ) {
        ret = 0;
    } else if( reply->type==REDIS_REPLY_ERROR ) {
        //std::cout << "redis_set error " << reply->str << std::endl;
        ret = -1;
    } else {
        ret = -1;
    }

    if( reply )
        freeReplyObject( reply );

    return ret;
}

int redis_get(const std::string &key, std::string& value) {
    int ret = 0;
    std::vector<std::string> commands;
    commands.push_back("GET");
    commands.push_back(key);
    redisReply *reply = cluster_->run(commands);
    if( !reply ) {
        ret = -1;
    } else if( reply->type==REDIS_REPLY_NIL ) {
        ret = 1; //not found
    } else if( reply->type==REDIS_REPLY_STRING ) {
        value = reply->str;
        ret = 0;
    } else if( reply->type==REDIS_REPLY_ERROR ) {
        //std::cout << "redis_get error " << reply->str << std::endl;
        ret = -1;
    } else {
        ret = -1;
    }

    if( reply )
        freeReplyObject( reply );

    return ret;
}

void* work_thread(void* para) {
    int now_us = 0;
    int now_sec = 0;
    int last_us = 0;
    volatile work_thread_data_t *pmydata = (work_thread_data_t *)para;
    volatile stat_item_t        *pstat   = &pmydata->stat;

    check_point(now_us, now_sec, last_us);
    srand( now_us );
    while( pmydata->is_running) {

        std::string key = get_random_key();
        std::string value_read;
        std::string value_write = get_random_value();

        /* check  */
        {
            redis::cluster::LockGuard lockguard(c_lock);
            CacheType::iterator iter = cache.find( key );
            if( iter != cache.end() )  {
                value_write = iter->second;
            } else {
                cache[key] = value_write;
            }
        }

        /* set */
        check_point(now_us, now_sec, last_us);
        int rv = redis_set(key, value_write);
        pstat->write_ttl += cluster_->ttls();
        pstat->write ++;
        if( rv<0 ) {
            pstat->write_error ++;
        }
        pstat->write_t += check_point(now_us, now_sec, last_us);

        /* read */
        rv = redis_get(key, value_read);
        pstat->read_ttl += cluster_->ttls();
        pstat->read ++;
        if( rv<0 ) {
            pstat->read_error ++;
        } else {
            if( value_read != value_write )  {
                pstat->unmatch ++;
            }
        }
        pstat->read_t += check_point(now_us, now_sec, last_us);

        /* load control */
        while(conf_load_of_thread == 0 && pmydata->is_running) {
            usleep(50 * 1000);
        }
        usleep( (10 - conf_load_of_thread) * 0.5 * (pstat->read_t/pstat->read) );

    }

    return NULL;
}

void* conf_thread(void* para) {

    int ch;
    initscr();
    cbreak();                 // no input buf
    noecho();                 // no echo
    keypad(stdscr, TRUE);     // enable function key
    refresh();                // clear screan
    //nl();

    while(true) {
        ch = getch();

        if (ch == 'q' or ch == 'Q') {
            endwin();
            conf_is_running = false;
            break;
        }

        if (ch == KEY_LEFT) {
            if(conf_threads_num > 0)
                conf_threads_num--;
        } else if (ch == KEY_RIGHT) {
            if(conf_threads_num < MAX_THREADS_NUM)
                conf_threads_num++;
        } else if (ch == KEY_UP) {
            if(conf_load_of_thread < MAX_LOAD_OF_THREAD)
                conf_load_of_thread++;
        } else if (ch == KEY_DOWN) {
            if(conf_load_of_thread > 0)
                conf_load_of_thread--;
        }
    }
    return NULL;
}

int main(int argc, char *argv[]) {

    bool interactively = true;

    /* init cache */

    int ret = pthread_spin_init(&c_lock, PTHREAD_PROCESS_PRIVATE);
    if(ret != 0) {
        std::cerr << "pthread_spin_init fail" << std::endl;
        return 1;
    }

    /* init cluster */

    std::string startup = "127.0.0.1:7000,127.0.0.1:7001";
    char *arg_startup   = NULL;

    if( argc > 1 ) {
        for(int i = 1; i < argc; i++) {
            if(strncmp(argv[i], "-n", 2) == 0) {
                if(interactively) {
                    interactively = false;
                    int n = atoi(argv[i] + 2);
                    if(n > 0) {
                        conf_threads_num = n > MAX_THREADS_NUM? MAX_THREADS_NUM: n;
                    }
                }
            } else if(!arg_startup)
                arg_startup = argv[i];
        }
    }
    if(arg_startup)
        startup = arg_startup;

    std::cout << "cluster startup with " << startup << std::endl;
    cluster_ = new redis::cluster::Cluster(1);

    if( cluster_->setup(startup.c_str(), true)!=0 ) {
        std::cerr << "cluster setup fail" << std::endl;
        return 1;
    }

    /* create config thread */

    if(interactively) {
        std::cout << "Hotkeys: \r\n"
                  "'LEFT'/'RIGHT' key to decrease/increase number of running threads.\r\n"
                  "'UP'/'DOWN' key to turn up/down the Read and Write speeds per thread.\r\n"
                  "'q' to quit.\r\n"
                  "press ENTER to continue...";
        getchar();

        pthread_t thconf;
        if(pthread_create(&thconf, NULL, conf_thread, NULL) != 0) {
            std::cerr << "create config thread fail" << std::endl;
            return 1;
        }
    }

    /* control work threads and do statistics */

    std::vector<work_thread_data_t> threads_data;
    std::vector<stat_item_t>        thread_stats_last;
    int                 workers_num_now;
    int                 workers_num_to;
    work_thread_data_t *pdata;
    int                 last_sec = 0;
    struct              timeval tv;

    uint64_t total_read  = 0;
    uint64_t total_write = 0;

    threads_data.reserve(MAX_THREADS_NUM);

    while(true) {
        workers_num_now = threads_data.size();
        if(!conf_is_running)
            workers_num_to = 0;
        else
            workers_num_to  = conf_threads_num;

        if(workers_num_to > workers_num_now) {
            /* create more work threads */

            threads_data.resize(workers_num_to);
            thread_stats_last.resize(workers_num_to, stat_item_initial);


            for(int idx = workers_num_now; idx < workers_num_to; idx++) {
                pdata = &(threads_data[idx]);

                pdata->is_running = true;
                pdata->stat = stat_item_initial;
                thread_stats_last[idx] = stat_item_initial;
                if(pthread_create(&pdata->tid, NULL, work_thread, pdata) != 0) {
                    std::cerr << "create worker thread "<<idx<<" fail" << std::endl;
                    exit(0);
                }
            }
        } else if (workers_num_to < workers_num_now) {
            /* end some work threads */

            for(int idx = workers_num_to; idx < workers_num_now; idx++) {
                pdata = &(threads_data[idx]);
                pdata->is_running = false;
                if(pthread_join(pdata->tid, NULL) != 0) {
                    std::cerr << "join worker thread "<<idx<<" fail" << std::endl;
                }
            }
            threads_data.resize(workers_num_to);
            thread_stats_last.resize(workers_num_to);
        }

        if(!conf_is_running)
            break;

        /* statistic */

        gettimeofday( &tv, NULL );
        int now_sec = tv.tv_sec;
        if( last_sec != now_sec ) {

            last_sec = now_sec;

            uint64_t read        = 0;
            uint64_t write       = 0;
            uint64_t read_t      = 0;
            uint64_t write_t     = 0;
            uint64_t read_ttl    = 0;
            uint64_t write_ttl   = 0;
            uint64_t read_error  = 0;
            uint64_t write_error = 0;
            uint64_t unmatch     = 0;

            stat_item_t stat_now;

            for(int idx = 0; idx < workers_num_to; idx++) {
                stat_now = threads_data[idx].stat;

                read        += (stat_now.read        - thread_stats_last[idx].read);
                write       += (stat_now.write       - thread_stats_last[idx].write);
                read_t      += (stat_now.read_t      - thread_stats_last[idx].read_t);
                write_t     += (stat_now.write_t     - thread_stats_last[idx].write_t);
                read_ttl    += (stat_now.read_ttl    - thread_stats_last[idx].read_ttl);
                write_ttl   += (stat_now.write_ttl   - thread_stats_last[idx].write_ttl);
                read_error  += (stat_now.read_error  - thread_stats_last[idx].read_error);
                write_error += (stat_now.write_error - thread_stats_last[idx].write_error);
                unmatch     += (stat_now.unmatch     - thread_stats_last[idx].unmatch);

                thread_stats_last[idx] = stat_now;
            }
            total_read  += read;
            total_write += write;

            std::cout << conf_threads_num <<" threads " << conf_load_of_thread << " loads "
                      << total_read << " R("<<read<<" read, " << read_error << " err, " <<  unmatch << " unmatch, "
                      << (read?(read_t/read):0)<<" usec per op, "
                      << (read?(read_ttl/read):0)<<" ttls) | "

                      << total_write << " W(" << write << " write, " << write_error << " err, "
                      << (write?(write_t/write):0)<<" usec per op, "
                      << (write?(write_ttl/write):0)<<" ttls) " << cluster_->stat_dump() << "\r\n";

            fflush(stdout);

        }
        usleep( 1000*10 );
    }

    return 0;
}


