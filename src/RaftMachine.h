//
// Created by dell-pc on 2018/5/6.
//

#ifndef PROJECT_RAFTMACHINE_H
#define PROJECT_RAFTMACHINE_H
#include <semaphore.h>
#include <algorithm>
#include <storage.pb.h>
#include "Utils.h"
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

    RingBuff *preWriteBuffArray;
    int businessThreads;//业务线程数, 对应的preWriteBuff_大小
    struct stCoCond_t *preWriteCond;
    long tid; //RaftMachine的tid, 目前不用了
    int notify_events; //业务通知RaftMachine::leaderSendLogCoroutine
    queue<LogData *> readyLogQue; //已经持久化存储的日志
    RingBuff writeLogThreadBuff_; //写入线程队列
    pthread_mutex_t writeLogThreadLock; //写入线程锁
    pthread_cond_t writeLogThreadCond;//写入线程信号
    long hasWriteLogIndex;//已持久化的logindex

    sem_t sendLogThreadSem;
    RingBuff sendLogThreadBuff;

public:
    RaftMachine(Storage *storage, Network *network, GroupCfg *groupCfg, const shared_ptr<pair<string, int>> &selfNode, int businessThreads)
            : storage(storage), network(network), groupCfg(groupCfg), selfNode(selfNode), businessThreads(businessThreads) {
        /*load group config*/
        const GroupCfg &storageGroupCfg = storage->getGroupCfg();
        if (!storageGroupCfg.getNodes().empty()) {
            groupCfg->setNodes(storageGroupCfg.getNodes());
        }

        /**/
        raftConfig = storage->getRaftConfig();
        this->raftStatus = RAFT_STAT_FOLLOWER;
        preWriteCond = co_cond_alloc();
        tid = GetThreadId();
        preWriteBuffArray = new RingBuff[businessThreads];
        sem_init(&sendLogThreadSem, 0, 0);
    }

    void start();

    int followerProcess();

    int candidaterProcess();

    int leaderProcess();

    void changeRaftStat(RaftStatus to);

    static string pair2NodeId(pair<string, int> *pPair);

    static string pair2NodeId(pair<string, int> &pair);

    int getLastLogTerm();

    jraft::Storage::RaftConfig *getRaftConfig() {
        return &raftConfig;
    }

//    //各个应用线程注册线程对应的预写的logid的列表
//    int registerThreadLogidQueque(long threadId, vector<void *> preLogidList);

    //业务预写log, 只是写入内存队列,
    //返回值>0 则返回值为开始的logid, 可使用的范围[logid, logList.size]
    //<0 则出错
    int preWriteLog(vector<LogData *> &logList, int threadIndex);

    //业务通知leader预写完成, 触发leader发送log给follower
    int notifyLeaderSendLog();

    leaderSendLogCoroutine();

    //写log线程
    leaderWriteLogThread();

    //发送log线程
    leaderSendLogThread();

    eventLoop();
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

    void setMatchIndex(string &nodeId, int index, string leaderNode, int leaderLogIndex) {
        if (matchIndex.find(nodeId) == matchIndex.end()) {
            LOG_COUT << "can not find nodeId! nodeId=" << nodeId << LOG_ENDL;
            return;
        }
        int count = 1;
        matchIndex[nodeId] = index;
        matchIndex[leaderNode] = leaderLogIndex;
        /*如果存在一个满足N > commitIndex的 N，并且大多数的matchIndex[i] ≥ N成立，
         * 并且log[N].term == currentTerm成立，那么令 commitIndex 等于这个 N （5.3 和 5.4 节）
         * 实现:对matchIndex排序(包含leader)倒排, 从大到小, 然后取matchIndex[size/2+1]的数据就是maxCommitedId,
         * 比如 [6 5 4], maxCommitedId=5
         */
        vector<int> matchIndexVect;
        for (auto it = matchIndex.begin();it != matchIndex.end(); ++it) {
            matchIndexVect.push_back(it->second);
//            LOG_COUT << "matchIndex " << it->first << "-->" << it->second << LOG_ENDL;
        }
        sort(matchIndexVect.begin(), matchIndexVect.end(), std::greater<>());
#if 0
        for (int i = 0; i < matchIndexVect.size(); ++i) {
            LOG_COUT << "matchIndexVect i=" <<i << " index=" << matchIndexVect[i] << LOG_ENDL;
        }
        LOG_COUT << "change max commit:" << maxCommitedId << "-->" << matchIndexVect[matchIndex.size()/2] << LOG_ENDL;
#endif
        maxCommitedId = matchIndexVect[matchIndex.size()/2];
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
