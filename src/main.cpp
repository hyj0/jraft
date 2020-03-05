//
// Created by dell-pc on 2018/5/6.
//
#include <stdio.h>
#include <malloc.h>
#include <iostream>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include "json11.hpp"
#include "Log.h"
#include "Config.h"
#include "Network.h"
#include "Common.h"
#include "RaftMachine.h"
#include "pb2json.h"
#include "Utils.h"
#include "Storage_leveldb.h"
#include "Storage_rocksdb.h"
#include "KVServer.h"
#include "sys/sysinfo.h"
using namespace std;

//clock_gettime
#if 1
//
extern "C"{
int __clock_gettime_glibc_2_2_5(clockid_t clk_id, struct timespec *tp);
}
__asm__(".symver __clock_gettime_glibc_2_2_5,  clock_gettime@GLIBC_2.2.5");
extern "C" {
int __wrap_clock_gettime(clockid_t clk_id, struct timespec *tp)
{
    return __clock_gettime_glibc_2_2_5(clk_id, tp);
}
}
#endif

#if 1
extern "C"{
void *__aligned_alloc_glibc_2_2_5(size_t alignment, size_t size);
}
__asm__(".symver __aligned_alloc_glibc_2_2_5,  aligned_alloc@GLIBC_2.16");
extern "C" {
void *__wrap_aligned_alloc(size_t alignment, size_t size)
{
#if 1
//    return __aligned_alloc_glibc_2_2_5(alignment, size);
    return memalign(alignment, size);
#else
    printf("coredump %s:%d\n", __FILE__, __LINE__);
    1/0;
    return 0;
#endif
}
}
#endif

#if 1
// memcpy
extern "C"{
void *__memcpy_glibc_2_2_5(void *, const void *, size_t);
}

asm(".symver __memcpy_glibc_2_2_5, memcpy@GLIBC_2.2.5");

extern "C" {
void *__wrap_memcpy(void *dest, const void *src, size_t n)
{
    return __memcpy_glibc_2_2_5(dest, src, n);
}
}
#endif

int co_accept(int fd, struct sockaddr *addr, socklen_t *len);

void *leaderSendLogCoroutine(void *arg) {
    RaftMachine *raftMachine = (RaftMachine *)arg;
    LOG_COUT << "start leaderSendLogCoroutine" << LOG_ENDL;
    raftMachine->leaderSendLogCoroutine();
}

void *leaderWriteLogThread(void *arg) {
    RaftMachine *raftMachine = (RaftMachine *)arg;
    LOG_COUT << "start leaderSendLogCoroutine" << LOG_ENDL;
    Utils::bindThreadCpu(1);
    raftMachine->leaderWriteLogThread();
}

void *startRaftMachine(void *arg)
{
    co_enable_hook_sys();
    Common *common = static_cast<Common *>(arg);
    Network *network = common->getNetwork();
    Storage *storage = common->getStorage();
    shared_ptr<pair<string, int>> selfNode = common->getSelfnode();

    RaftMachine *pRaftMachine = new RaftMachine(storage, network, common->getGroupCfg(), selfNode, common->getBusinessThreads());
    common->setRaftMachine(pRaftMachine);

    pthread_t tid;
    int ret = pthread_create(&tid, NULL, leaderWriteLogThread, pRaftMachine);

    //新建leader发送log协程
    stCoRoutine_t *ctx = NULL;
    co_create(&ctx, NULL, leaderSendLogCoroutine, pRaftMachine);
    co_resume(ctx);

    pRaftMachine->start();
}

void *mainCoroutine(void *arg)
{
    co_enable_hook_sys();
    map<string, Common*> *groupIdCommonMap = static_cast<map<string, Common *> *>(arg);
    int servFd = groupIdCommonMap->begin()->second->getNetwork()->getSelfnodeFd();

    while (1) {
        struct pollfd pf = {0};
        pf.fd = servFd;
        pf.events = (POLLIN | POLLERR | POLLHUP);
        int ret = co_poll(co_get_epoll_ct(), &pf, 1, 1000*50);
        if (ret == 0) {
            LOG_COUT << "no data fd=" << servFd << LOG_ENDL;
            continue;
        }

        char buff[1000*4];
        memset(buff, 0, sizeof(buff));
        struct sockaddr_in cliAddr;
        socklen_t cliAddrLen;
        cliAddrLen = sizeof(cliAddr);
        ret = recvfrom(servFd, buff, sizeof(buff), 0, (struct sockaddr *) (&cliAddr), &cliAddrLen);
        if (ret < 0) {
            LOG_COUT << " recvfrom fd=" << servFd << " ret=" << ret << LOG_ENDL;
            continue;
        }

        int dataLen = ((MsgHead*)buff)->datalen;
        char *data = buff+ sizeof(MsgHead);
        if (dataLen+ sizeof(MsgHead) != ret) {
            LOG_COUT << "Msg len err ! " << LOG_ENDL_ERR;
            continue;
        }
        shared_ptr<jraft::Network::Msg> recvMsg = make_shared<jraft::Network::Msg>();
        if (!recvMsg->ParseFromArray(data, dataLen)) {
            LOG_COUT << "ParseFromArray err ! " << LOG_ENDL_ERR;
            continue;
        }


        sockaddr_in *cliAddr1 = new sockaddr_in();
        memcpy(cliAddr1, &cliAddr, sizeof(sockaddr_in));
        auto it = groupIdCommonMap->find(recvMsg->group_id());
        if (it != groupIdCommonMap->end()) {
            struct sockaddr_in *addr = static_cast<sockaddr_in *>(malloc(sizeof(struct sockaddr_in)));
            memcpy(addr, &cliAddr, sizeof(struct sockaddr_in));
            shared_ptr<struct sockaddr_in> sharedAddr(addr);
            it->second->getNetwork()->pushMsg(
                    pair<shared_ptr<struct sockaddr_in>, shared_ptr<jraft::Network::Msg>>(sharedAddr, recvMsg));
        } else {
            LOG_COUT <<"group_id not found recv:group_id=" << recvMsg->group_id() << LOG_ENDL;
            continue;
        }
    }
}

void signalHand(int sig)
{
    LOG_COUT << "sig=" << sig << LOG_ENDL;
    exit(0);
}

void loopFun(void *arg) {
    map<string, Common*> *groupIdCommonMap = (map<string, Common*> *)arg;
    auto it = groupIdCommonMap->begin();
    for (; it!= groupIdCommonMap->end(); ++it) {
       it->second->getRaftMachine()->eventLoop();
    }
}

int main(int argc, char **argv)
{
    if (argc < 2) {
        LOG_COUT << "usage:" << argv[0] << " configfile [mem|leveldb|rocksdb]" << LOG_ENDL;
        return 1;
    }
    Config config(argv[1]);
    int ret = config.parseConfig();
    if (ret != 0) {
        LOG_COUT <<  "read config err ret=" << ret << LOG_ENDL;
        return ret;
    }

    string storageType = "mem";
    if (argc >= 3) {
        storageType = argv[2];
        if (storageType == "mem"
            || storageType == "leveldb"
            || storageType == "rocksdb") {
        } else {
            LOG_COUT << "storageType err  [mem|leveldb|rocksdb] " << LOG_ENDL;
            return -1;
        }
    }

    int nCpu = sysconf(_SC_NPROCESSORS_CONF);
    int nCpuOn = sysconf(_SC_NPROCESSORS_ONLN);
    int nCore = get_nprocs();
    LOG_COUT << "cpu:" << nCpu << " " << nCpuOn << " " << nCore << LOG_ENDL;
    int nThreadCount = nCpuOn*2;
    LOG_COUT << "nThreadCount=" << nThreadCount << LOG_ENDL;

    signal(SIGTERM|SIGINT | SIGQUIT | SIGKILL, signalHand);

    auto configJson = config.getConfigJson();
    LOG_COUT << "config:" << configJson.dump() << LOG_ENDL;

    auto selfnode = config.getSelfnode();

    int servFd = CreateUdpSecket(const_cast<char *>(selfnode->first.c_str()), selfnode->second, true);
    if (servFd < 0) {
        LOG_COUT <<"CreateUdpSecket err ret="<<servFd << LOG_ENDL_ERR;
        return servFd;
    }
    map<string, Common*> groupIdCommonMap;
    
    vector<shared_ptr<GroupCfg>> groups = config.getGroups();
    for (int i = 0; i < groups.size(); ++i) {
        Common *common = new Common();
        common->setBusinessThreads(nThreadCount);
        shared_ptr<GroupCfg> &oneGroup = groups[i];
        stringstream strNodeSpecBuf;
        strNodeSpecBuf << oneGroup->getGroupId() << "_" << selfnode->first << "_" << selfnode->second;
        Storage *pStorage = NULL;;
        if (storageType == "mem") {
            pStorage = new Storage(const_cast<string &>(groups[i]->getStorage()),
                                           strNodeSpecBuf.str());
        } else if (storageType == "leveldb"){
            pStorage = new Storage_leveldb(const_cast<string &>(groups[i]->getStorage()),
                                   strNodeSpecBuf.str());
        } else if (storageType == "rocksdb") {
            pStorage = new Storage_rocksdb(const_cast<string &>(groups[i]->getStorage()),
                                           strNodeSpecBuf.str());
        }
        LOG_COUT << "storageType=" << pStorage->getStorageType() << LOG_ENDL;
        common->setStorage(pStorage);
        common->setNetwork(new Network(servFd));
        common->setGroupCfg(groups[i].get());
        common->setSelfnode(selfnode);
        groupIdCommonMap.insert(make_pair<string, Common*>((string)groups[i]->getGroupId(),
                                                           reinterpret_cast<Common *&&>(common)));
        stCoRoutine_t *ctx = NULL;
        co_create(&ctx, NULL, startRaftMachine, common);
        co_resume(ctx);
    }

    stCoRoutine_t *ctx = NULL;
    co_create(&ctx, NULL, mainCoroutine, &groupIdCommonMap);
    co_resume(ctx);

    StartKVServer(groupIdCommonMap, selfnode->second, nThreadCount);
    Utils::bindThreadCpu(0);
    co_eventloop(co_get_epoll_ct(), loopFun, &groupIdCommonMap);
}

