//
// Created by dell-pc on 2018/5/6.
//

#ifndef PROJECT_RAFTMACHINE_H
#define PROJECT_RAFTMACHINE_H

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

    RingBuff preWriteBuff_;
    struct stCoCond_t *preWriteCond;
    long tid;
    int notify_events;
    queue<LogData *> readyLogQue; //�Ѿ��־û��洢����־
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
        preWriteCond = co_cond_alloc();
        tid = GetThreadId();
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

//    //����Ӧ���߳�ע���̶߳�Ӧ��Ԥд��logid���б�
//    int registerThreadLogidQueque(long threadId, vector<void *> preLogidList);

    //ҵ��Ԥдlog, ֻ��д���ڴ����,
    //����ֵ>0 �򷵻�ֵΪ��ʼ��logid, ��ʹ�õķ�Χ[logid, logList.size]
    //<0 �����
    int preWriteLog(vector<LogData *> &logList);

    //ҵ��֪ͨleaderԤд���, ����leader����log��follower
    int notifyLeaderSendLog();

    leaderSendLogProccess();

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
        /*�������һ������N > commitIndex�� N�����Ҵ������matchIndex[i] �� N������
         * ����log[N].term == currentTerm��������ô�� commitIndex ������� N ��5.3 �� 5.4 �ڣ�
         * ʵ��:��matchIndex����(����leader)����, �Ӵ�С, Ȼ��ȡmatchIndex[size/2+1]�����ݾ���maxCommitedId,
         * ���� [6 5 4], maxCommitedId=5
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
