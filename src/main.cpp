//
// Created by dell-pc on 2018/5/6.
//
#include <iostream>
#include <netinet/in.h>
#include <arpa/inet.h>
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

using namespace std;

void *startRaftMachine(void *arg)
{
    co_enable_hook_sys();
    Common *common = static_cast<Common *>(arg);
    Network *network = common->getNetwork();
    Storage *storage = common->getStorage();
    shared_ptr<pair<string, int>> selfNode = common->getSelfnode();

    RaftMachine raftMachine(storage, network, common->getGroupCfg(), selfNode);

    raftMachine.start();
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
        int ret = co_poll(co_get_epoll_ct(), &pf, 1, 1000*10);
        if (ret == 0) {
            LOG_COUT << "no data fd=" << servFd << LOG_ENDL;
            continue;
        }

        char buff[1000];
        memset(buff, 0, sizeof(buff));
        struct sockaddr_in cliAddr;
        socklen_t cliAddrLen;
        cliAddrLen = sizeof(cliAddr);
        ret = recvfrom(servFd, buff, 1000, 0, (struct sockaddr *) (&cliAddr), &cliAddrLen);
        if (ret < 0) {
            LOG_COUT << " recvfrom fd=" << servFd << " ret=" << ret << LOG_ENDL;
            continue;
        }
        LOG_COUT << " recvfrom fd=" << servFd << " buff=" << buff << LOG_ENDL;

        Pb2Json::Json json = Pb2Json::Json::parse(string(buff, ret));
        shared_ptr<jraft::Network::Msg> recvMsg = make_shared<jraft::Network::Msg>();
        Pb2Json::Json2Message(json, *recvMsg.get(), true);

        sockaddr_in *cliAddr1 = new sockaddr_in();
        memcpy(cliAddr1, &cliAddr, sizeof(sockaddr_in));
        LOG_COUT << "recvfrom " << RaftMachine::pair2NodeId(Network::address2pair(shared_ptr<sockaddr_in>(cliAddr1)).get())
                 << " groupId=" << recvMsg->group_id()
                 << " msg=" << json.dump() << LOG_ENDL;
        if (recvMsg->msg_type() == jraft::Network::MsgType::MSG_Type_Rpc_Request) {
            if (!json["rpc_request"]["log_entrys"].is_null()) {
                if (recvMsg->mutable_rpc_request()->log_entrys_size() == 0) {
                    LOG_COUT << "Json2Message lose repeated data! fix now" << LOG_ENDL;
                    Pb2Json::Json logJson = json["rpc_request"]["log_entrys"];
                    for (int i = 0; i < logJson.size(); ++i) {
                        jraft::Network::LogEntry *logEntry = recvMsg->mutable_rpc_request()->add_log_entrys();
                        Pb2Json::Json2Message(logJson[i], *logEntry);
                    }
                }
            }
        }

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

int main(int argc, char **argv)
{
    if (argc != 2) {
        LOG_COUT << "usage:" << argv[0] << " configfile" << LOG_ENDL;
        return 1;
    }
    Config config(argv[1]);
    int ret = config.parseConfig();
    if (ret != 0) {
        LOG_COUT <<  "read config err ret=" << ret << LOG_ENDL;
        return ret;
    }

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
        shared_ptr<GroupCfg> &oneGroup = groups[i];
        stringstream strNodeSpecBuf;
        strNodeSpecBuf << oneGroup->getGroupId() << "_" << selfnode->first << "_" << selfnode->second;
        common->setStorage(new Storage_rocksdb(const_cast<string &>(groups[i]->getStorage()),
                                               strNodeSpecBuf.str()));
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

    co_eventloop(co_get_epoll_ct(), NULL, NULL);
}

