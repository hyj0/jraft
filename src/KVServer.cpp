//
// Created by hyj on 2020-01-27.
//

#include <stack>
#include <unistd.h>
#include "KVServer.h"
#include "Common.h"
#include "Log.h"
#include "Utils.h"

using namespace std;

int g_log_id = 0;
pthread_mutex_t g_log_id_lock = PTHREAD_MUTEX_INITIALIZER;

ThreadGroupLogList *g_threadGroupLogList;

struct task_t
{
    stCoRoutine_t *co;
    int fd;
    int threadIndex;
};

static stack<task_t*> *g_readwrite_task;

static int g_tcpServFd = -1;
static map<string, Common*> *g_groupIdCommonMap;

int co_accept(int fd, struct sockaddr *addr, socklen_t *len);

void *KVWriteLogCoroutine(void *args) {
    co_enable_hook_sys();
    int threadIndex = *(int *)args;

    map<string, vector<LogData *>> &groupLogList = g_threadGroupLogList->getGroupList(threadIndex);
    while (1) {
        int ret = co_cond_timedwait(g_threadGroupLogList->getThreadCond(threadIndex), 1000*80);
        for (auto it = groupLogList.begin(); it != groupLogList.end(); ++it) {
            vector<LogData *> &logDataV = it->second;
            if (logDataV.size() <= 0) {
                continue;
            }
            auto commonIt = g_groupIdCommonMap->find(it->first);
            //预写
            int startLogid = commonIt->second->getRaftMachine()->preWriteLog(logDataV);
            if (startLogid < 0) {
                LOG_COUT << "preWriteLog err ret=" << startLogid << LOG_ENDL;
                assert(0);//todo:preWriteLog err
            }
            //:batch write
            LOG_COUT << "size=" << logDataV.size() << LOG_ENDL;
            vector<jraft::Storage::Log> batchLog;
            for (int i = 0; i < logDataV.size(); ++i) {
                batchLog.push_back(logDataV[i]->log);
            }
            //写入
            commonIt->second->getStorage()->setRaftLog(batchLog);
            for (int j = 0; j < logDataV.size(); ++j) {
                logDataV[j]->state = 1;
            }
            //通知处理
            commonIt->second->getRaftMachine()->notifyLeaderSendLog();
            logDataV.clear();
        }
#if 0
        vector<LogData*> kvDataVect;
        do {
            LogData *pkvData = coBuff.popOne();
            if (pkvData == NULL) {
                break;
            }
            kvDataVect.push_back(pkvData);
        } while (1);
        if (kvDataVect.size() > 0) {
//            kvDataQueque->putData(kvDataVect);
            for (int i = 0; i < kvDataVect.size(); ++i) {
                auto it = g_groupIdCommonMap->find(kvDataVect[i]->groupId);
                if (it != g_groupIdCommonMap->end()) {
                    jraft::Storage::RaftConfig *raftConfig = it->second->getRaftMachine()->getRaftConfig();
                    long logid = it->second->getRaftMachine()->makePreLogid(1);
                    kvDataVect[i]->log.set_term(raftConfig->current_term());
                    kvDataVect[i]->log.set_log_index(logid);
                    if (i == 0) {
                        it->second->getStorage()->setRaftLog(kvDataVect[i]->log, logid);
                    }
                }
            }

            LOG_COUT << "size=" << kvDataVect.size() << LOG_ENDL;
        }
        for (int i = 0; i < kvDataVect.size(); ++i) {
            kvDataVect[i]->state = 1;
            co_cond_signal(kvDataVect[i]->cond);
        }
#endif
    }
    return 0;
}

void *KVWorkerCoroutine(void *args) {
    task_t *co = (task_t*)args;
    int threadIndex = co->threadIndex;
    stack<task_t*> &g_readwrite = g_readwrite_task[threadIndex];
    co_enable_hook_sys();

    stCoCond_t *cond = co_cond_alloc();
    string retStr = "HTTP/1.1 200 OK\r\n"
                    "Content-Length: 2\r\n"
                    "Connection: Keep-Alive\r\n"
                    "Content-Type: text/html\r\n\r\nab";
    string retStr1 = "HTTP/1.1 200 OK\r\n"
                     "Content-Length: 8\r\n"
                     "Connection: Keep-Alive\r\n"
                     "Content-Type: text/html\r\n\r\n%08d";
    char buf[1024];
    for(;;) {
        if (-1 == co->fd) {
            g_readwrite.push(co);
            co_yield_ct();
            continue;
        }
        int fd = co->fd;
        co->fd = -1;
        while (1) {
            struct pollfd pf = { 0 };
            pf.fd = fd;
            pf.events = (POLLIN|POLLERR|POLLHUP);
            int ret = co_poll( co_get_epoll_ct(),&pf,1,5*1000);
            if (ret == 0) {
                continue;
            }

            ret = read( fd,buf,sizeof(buf) );
            if (ret <= 0) {
                break;
            }
#if 0
            if (1) {
                pthread_mutex_lock(&g_log_id_lock);
                g_log_id +=1;
                int nLen = sprintf(buf, retStr1.c_str(), g_log_id);
                pthread_mutex_unlock(&g_log_id_lock);

#if 0
                while (1) {
                    pf.fd = fd;
                    pf.events = (POLLOUT|POLLERR|POLLHUP);
                    int ret = co_poll( co_get_epoll_ct(),&pf,1,5*1000);
                    if (ret == 0) {
                        continue;
                    }
                    ret = write( fd, buf, nLen );
//                LOG_COUT << "write ret=" << ret << " errno=" << errno << LOG_ENDL;
                    if (ret != nLen) {
                        assert(0);
                    }
                    break;
                }
#else
                ret = write( fd, buf, nLen );
//                LOG_COUT << "write ret=" << ret << " errno=" << errno << LOG_ENDL;
                if (ret != nLen) {
                    assert(0);
                }
#endif
                continue;
            }
#endif

            shared_ptr<jraft::Network::Msg> recvMsg = make_shared<jraft::Network::Msg>();
            //test data
            recvMsg->set_msg_type(jraft::Network::MSG_Type_KVCli_Request);
            recvMsg->set_group_id("1");
            jraft::Network::KVCliReq *kvCliReq = recvMsg->mutable_kvcli_request();
            //test data
            kvCliReq->set_request_type(1);
            jraft::Network::LogEntry *logEntry = kvCliReq->mutable_log_entry();
            //test data
            logEntry->set_action("change");
            logEntry->set_key("key");
            logEntry->set_value("vvv");

            LogData logData;
            logData.log.mutable_log_entry()->set_action(logEntry->action());
            logData.log.mutable_log_entry()->set_key(logEntry->key());
            logData.log.mutable_log_entry()->set_value(logEntry->value());
            logData.cond = cond;
            long tid = GetThreadId();
            logData.tid = tid;
            logData.state = 0;
            logData.groupId = recvMsg->group_id();
//            LOG_COUT << "worker tid=" << tid << LOG_ENDL;
            jraft::Network::Msg resMsg;
            resMsg.set_msg_type(jraft::Network::MSG_Type_KVCli_Response);
            jraft::Network::KVCliRes *kvCliRes = resMsg.mutable_kvcli_response();
            kvCliRes->set_result(0);

            if (recvMsg->msg_type() == jraft::Network::MSG_Type_KVCli_Request) {
                if (kvCliReq->request_type() == 1) {
                    ret  = g_threadGroupLogList->addLogData(threadIndex, recvMsg->group_id(), &logData);
                    if (ret != 0) {
                        LOG_COUT << "addLogData err!!" << LOG_ENDL;
                        assert(0);
                    }
                    //通知写入协程
                    co_cond_signal(g_threadGroupLogList->getThreadCond(threadIndex));
                    //处理返回
                    while (logData.state != 2) {
                        ret = co_cond_timedwait(cond, 1000*5);
                    }
                } else if (kvCliReq->request_type() == 2){

                }
            } else {
                kvCliRes->set_result(1);
                kvCliRes->set_err_msg("msg type err");
            }
            //send
            if( ret >= 0 )
            {
                string retStr = "HTTP/1.1 200 OK\r\n"
                                "Content-Length: 2\r\n"
                                "Connection: Keep-Alive\r\n"
                                "Content-Type: text/html\r\n\r\nab";
                int nLen = sprintf(buf, retStr1.c_str(), logData.log.log_index());
                ret = write( fd, buf, nLen );
                if (ret != nLen) {
                    LOG_COUT << "write ret=" << ret << " errno=" << errno << LOG_ENDL;
                    assert(0);
                }
            }
            if (ret <= 0) {
                if (errno == EAGAIN || errno == 0) {
                    continue;
                } else {
                    break;
                }
            }
        }
        close(fd);
    }
}

void *KVAcceptCoroutine(void *args) {
    co_enable_hook_sys();
    int threadIndex = *(int *)args;
    stack<task_t*> &g_readwrite = g_readwrite_task[threadIndex];

    for(;;)
    {
        if( g_readwrite.empty() )
        {
//            LOG_COUT << "g_readwrite.empty !! " << LOG_ENDL;
            task_t * task = (task_t*)calloc( 1,sizeof(task_t) );
            task->fd = -1;
            task->threadIndex = threadIndex;
            co_create(&(task->co), NULL, KVWorkerCoroutine, task);
            co_resume(task->co);
        }
        struct sockaddr_in addr; //maybe sockaddr_un;
        memset( &addr,0,sizeof(addr) );
        socklen_t len = sizeof(addr);

        struct pollfd pf = { 0 };
        pf.fd = g_tcpServFd;
        pf.events = (POLLIN | POLLERR | POLLHUP);
        int ret = co_poll(co_get_epoll_ct(), &pf, 1, 10000 );
        if (ret == 0) {
            continue;
        }
        int fd = co_accept(g_tcpServFd, (struct sockaddr *)&addr, &len);
        if( fd < 0 )
        {
            continue;
        }
        if( g_readwrite.empty() )
        {
            close( fd );
            continue;
        }
        SetNonBlock( fd );
        task_t *co = g_readwrite.top();
        co->fd = fd;
        co->threadIndex = threadIndex;
        g_readwrite.pop();
        co_resume(co->co);
    }
    return 0;
}

void evloopFun(void *arg) {
    CoroutineSignalOverThread *cor = CoroutineSignalOverThread::getInstance();
    cor->loopSelfTid();
}

void *KVServerThread(void *args) {
    int threadIndex = (int)args;
    LOG_COUT << "threadIndex=" << threadIndex << LOG_ENDL;
    for (int i = 0; i < 100; ++i) {
        task_t * task = (task_t*)calloc( 1,sizeof(task_t) );
        task->fd = -1;
        task->threadIndex = threadIndex;
        co_create(&(task->co), NULL, KVWorkerCoroutine, task);
        co_resume(task->co);
    }

    stCoRoutine_t *ctx = NULL;
    co_create(&ctx, NULL, KVAcceptCoroutine, &threadIndex);
    co_resume(ctx);

    co_create(&ctx, NULL, KVWriteLogCoroutine, &threadIndex);
    co_resume(ctx);

    co_eventloop(co_get_epoll_ct(), evloopFun, NULL);
}

int StartKVServer(map<string, Common *> &groupIdCommonMap, int tcpPort) {
    g_groupIdCommonMap = &groupIdCommonMap;
    g_tcpServFd = CreateTcpSocket(tcpPort, "*", true);
    if (g_tcpServFd < 0) {
        LOG_COUT << "CreateTcpSocket err ret=" << g_tcpServFd << LOG_ENDL_ERR;
        return g_tcpServFd;
    }
    SetNonBlock(g_tcpServFd);
    //处理KV请求的线程
    int nThreadCount = 8;
    g_threadGroupLogList = new ThreadGroupLogList(nThreadCount, groupIdCommonMap);
    g_readwrite_task = new stack<task_t*>[nThreadCount];
    pthread_t *tinfo = static_cast<pthread_t *>(calloc(nThreadCount, sizeof(pthread_t)));
    for (int j = 0; j < nThreadCount; ++j) {
        LOG_COUT << "j=" << j << LOG_ENDL;
#if 1
        int ret = pthread_create(&tinfo[j], NULL, KVServerThread, (void *)j);
#else
        KVServerThread(j);
#endif
        if (ret != 0) {
            LOG_COUT << " pthread_create err ret=" << ret << LOG_ENDL_ERR;
            return ret;
        }
    }
    return 0;
}
