//
// Created by dell-pc on 2018/5/6.
//

#ifndef PROJECT_STORAGE_H
#define PROJECT_STORAGE_H

#include <iostream>
#include "storage.pb.h"
#include "Config.h"
#include "Log.h"

class Storage {
protected:
    jraft::Storage::RaftConfig raftConfig;
    GroupCfg groupCfg;
    vector<jraft::Storage::Log> raftLogArray;
    pthread_mutex_t raftLogArrayLock;
public:
    Storage (string & storageName, string nodeId) {
        jraft::Storage::Log log;
        raftLogArray.push_back(log);
        raftLogArrayLock = PTHREAD_MUTEX_INITIALIZER;
    }

    virtual ~Storage() {}

    virtual const GroupCfg &getGroupCfg() const {
        return groupCfg;
    }

    virtual void setGroupCfg(const GroupCfg &groupCfg) {
        Storage::groupCfg = groupCfg;
    }

    virtual const jraft::Storage::RaftConfig &getRaftConfig() const {
        return raftConfig;
    }

    virtual void setRaftConfig(const jraft::Storage::RaftConfig &raftConfig) {
        Storage::raftConfig = raftConfig;
    }

    virtual shared_ptr<jraft::Storage::Log> getRaftLog(int logIndex) {
        if (logIndex > raftConfig.max_log_index()) {
            return nullptr;
        }
        if (logIndex <= 0) {
            return NULL;
        }
        if (logIndex >= 0 && logIndex < raftLogArray.size()) {
            jraft::Storage::Log *log = new jraft::Storage::Log(raftLogArray[logIndex]);
            return shared_ptr<jraft::Storage::Log>(log);
        } else {
            LOG_COUT << "logIndex err logIndex=" << logIndex << LOG_ENDL;
        }
        return NULL;
    }

    virtual int setRaftLog(jraft::Storage::Log &log, int index) {
        pthread_mutex_lock(&raftLogArrayLock);
        if (index < raftLogArray.size()) {
            raftLogArray[index] = log;
        } else {
            if (index == raftLogArray.size()) {
                raftLogArray.push_back(log);
            } else {
                raftLogArray.resize(index+1);
                raftLogArray[index] = log;
//                LOG_COUT << "logIndex err logIndex=" << index << LOG_ENDL;
//                return -1;
            }
        }
        pthread_mutex_unlock(&raftLogArrayLock);
        return 0;
    }

    virtual int setRaftLog(vector<jraft::Storage::Log> &logVect) {
        for (int i = 0; i < logVect.size(); ++i) {
            setRaftLog(logVect[i], logVect[i].log_index());
        }
    }

    virtual int setRaftLog(vector<jraft::Storage::Log> &logVect, const jraft::Storage::RaftConfig &raftConfig) {
        setRaftLog(logVect);
        setRaftConfig(raftConfig);
    }
};


#endif //PROJECT_STORAGE_H
