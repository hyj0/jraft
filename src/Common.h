//
// Created by dell-pc on 2018/5/6.
//

#ifndef PROJECT_COMMON_H
#define PROJECT_COMMON_H

#include "Log.h"
#include "Config.h"
#include "Network.h"
#include "Storage.h"
#include "Utils.h"
#include "RaftMachine.h"
#include "Common.h"

class RaftMachine;
class Common {
public:
    Network *getNetwork() const {
        return network;
    }

    void setNetwork(Network *network) {
        Common::network = network;
    }

    Storage *getStorage() const {
        return storage;
    }

    void setStorage(Storage *storage) {
        Common::storage = storage;
    }

    GroupCfg *getGroupCfg() const {
        return groupCfg;
    }

    void setGroupCfg(GroupCfg *groupCfg) {
        Common::groupCfg = groupCfg;
    }

    RaftMachine *getRaftMachine() const {
        return raftMachine;
    }

    void setRaftMachine(RaftMachine *raftMachine) {
        Common::raftMachine = raftMachine;
    }

    int getBusinessThreads() const {
        return businessThreads;
    }

    void setBusinessThreads(int businessThreads) {
        Common::businessThreads = businessThreads;
    }

private:
    GroupCfg *groupCfg;
    Network *network;
    Storage *storage;
    shared_ptr<pair<string, int>> selfnode;
    RaftMachine *raftMachine;
    int businessThreads;//业务线程数
public:
    const shared_ptr<pair<string, int>> &getSelfnode() const {
        return selfnode;
    }

    void setSelfnode(const shared_ptr<pair<string, int>> &selfnode) {
        Common::selfnode = selfnode;
    }
};


#endif //PROJECT_COMMON_H
