//
// Created by hyj on 2020-01-27.
//

#ifndef PROJECT_KVSERVER_H
#define PROJECT_KVSERVER_H

#include "Common.h"
#include <queue>
#include <vector>
#include <co_routine.h>
#include "raft.pb.h"
#include "Utils.h"

using namespace std;

class KVServer {

};

//业务工作线程对应的协程 写入的分组日志
class ThreadGroupLogList {
private:
    vector<map<string, vector<LogData *>>> threadGroupLogList; // 访问threadGroupLogList[threadIndex][groupId]-->vector<logData*>
    vector<stCoCond_t *> thread_cond; //通知写入线程的信号
    vector<int > sumArray; //每个线程的sum
public:
    ThreadGroupLogList(int threadCount, map<string, Common *> &groupIdCommonMap) {
        for (int i = 0; i < threadCount; ++i) {
            map<string, vector<LogData *>> gMap;
            auto it = groupIdCommonMap.begin();
            for (; it != groupIdCommonMap.end(); ++it) {
                vector<LogData *> logDataList;
                gMap[it->first] = logDataList;
            }
            threadGroupLogList.push_back(gMap);
            struct stCoCond_t *cond = co_cond_alloc();
            thread_cond.push_back(cond);
            sumArray.push_back(0);
        }
    }

    int addLogData(int threadIndex, string groupId,  LogData *logData) {
        auto it = threadGroupLogList[threadIndex].find(groupId);
        if (it == threadGroupLogList[threadIndex].end()) {
            LOG_COUT << "group id err!!! " << groupId << endl;
            return -1;
        }
        threadGroupLogList[threadIndex][groupId].push_back(logData);
        sumArray[threadIndex] += 1;
        return 0;
    }

     stCoCond_t * getThreadCond(int threadIndex) {
        return thread_cond[threadIndex];
    }

    map<string, vector<LogData *>> &getGroupList(int threadIndex) {
        return threadGroupLogList[threadIndex];
    }

    vector<int> *getSumArray() {
        return &sumArray;
    }
};


void *KVWriteLogCoroutine(void *args);
void *KVWorkerCoroutine(void *args);

void *KVAcceptCoroutine(void *args);

void *KVServerThread(void *args);

int StartKVServer(map<string, Common *> &groupIdCommonMap, int tcpPort, int nThreadCount);


#endif //PROJECT_KVSERVER_H
