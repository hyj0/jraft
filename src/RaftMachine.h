//
// Created by dell-pc on 2018/5/6.
//

#ifndef PROJECT_RAFTMACHINE_H
#define PROJECT_RAFTMACHINE_H

#include "Common.h"
#define VOTEFOR_NULL string("NULL")


enum RaftStatus {
    RAFT_STAT_LEADER = 1,
    RAFT_STAT_FOLLOWER = 2,
    RAFT_STAT_CANDIDATER = 3
};

static map<RaftStatus , string> g_raftStatusNameMap = {
        {RAFT_STAT_LEADER, "Leader"},
        {RAFT_STAT_FOLLOWER, "Follower"},
        {RAFT_STAT_CANDIDATER, "Candidater"}
};

class NodesLogInfo;

class RaftMachine {
private:
    Storage *storage;
    Network *network;
    GroupCfg *groupCfg;
    shared_ptr<pair<string, int>> selfNode;
    RaftStatus raftStatus;
    jraft::Storage::RaftConfig raftConfig;
    shared_ptr<NodesLogInfo> nodesLogInfo;
    string leader_id;
public:
    RaftMachine(Storage *storage, Network *network, GroupCfg *groupCfg, const shared_ptr<pair<string, int>> &selfNode)
            : storage(storage), network(network), groupCfg(groupCfg), selfNode(selfNode) {
        /*load group config*/
        const GroupCfg &storageGroupCfg = storage->getGroupCfg();
        if (!storageGroupCfg.getNodes().empty()) {
            groupCfg->setNodes(storageGroupCfg.getNodes());
        }

        /**/
        raftConfig = storage->getRaftConfig();
        this->raftStatus = RAFT_STAT_FOLLOWER;
    }

    void start();

    int followerProcess();

    int candidaterProcess();

    int leaderProcess();

    void changeRaftStat(RaftStatus to);

    static string pair2NodeId(pair<string, int> *pPair);

    static string pair2NodeId(pair<string, int> &pair);

    int getLastLogTerm();

};

class NodesLogInfo{
private:
    map<string, int> matchIndex;
    map<string, int> nextIndex;
    int maxCommitedId;
public:
    NodesLogInfo(const vector<string> &nodes, int index=1) {
        for (const auto &node : nodes) {
            matchIndex[node] = 0;
            nextIndex[node] = index;
            maxCommitedId = 0;
        }
    }

    int getMatchIndex(string &node) {
        return matchIndex[node];
    }

    void setMatchIndex(string &nodeId, int index) {
        if (matchIndex.find(nodeId) == matchIndex.end()) {
            LOG_COUT << "can not find nodeId! nodeId=" << nodeId << LOG_ENDL;
            return;
        }
        int count = 1;
        matchIndex[nodeId] = index;
        for (auto &it : matchIndex) {
            if (it.second == index) {
                count += 1;
                if (count > (matchIndex.size()+1)/2) {
                    if (maxCommitedId < index) {
                        LOG_COUT << "change max commit:" << maxCommitedId << "-->" << index << LOG_ENDL;
                        maxCommitedId = index;
                    }
                    break;
                }
            }
        }
    }

    int getNextIndex(string node) {
        return nextIndex[node];
    }
    void setNextIndex(string &nodeId, int index) {
        nextIndex[nodeId] = index;
    }

    int getMaxCommitedId() {
        return maxCommitedId;
    }
};

#endif //PROJECT_RAFTMACHINE_H
