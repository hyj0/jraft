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
    string storageType = "mem";
    jraft::Storage::RaftConfig raftConfig;
    GroupCfg groupCfg;
    vector<jraft::Storage::Log> raftLogArray;
    map<int, jraft::Storage::Log*> noWriteBuffMap;//cache
public:
    Storage (string & storageName, string nodeId) {
        //todo:多线程读写有问题,先临时分配, 大于这么多的还是有多线程问题
        raftLogArray.reserve(10000*200);
        jraft::Storage::Log log;
        setRaftLog(log, 0);
        storageType = "mem";
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
        if (index < raftLogArray.size()) {
            raftLogArray[index] = log;
        } else {
            if (index == raftLogArray.size()) {
                raftLogArray.push_back(log);
            } else {
                raftLogArray.resize(index+1);
                raftLogArray[index] = log;
                LOG_COUT << "logIndex err logIndex=" << index << LOG_ENDL;
                assert(0);
                return -1;
            }
        }
        return 0;
    }

    virtual int setRaftLogNoWrite(jraft::Storage::Log &log, int index) {
        noWriteBuffMap[index] = &log;;
        return 0;
    }

    virtual int deleleRaftLogNoWriteCache(int index) {
        noWriteBuffMap.erase(index);
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

    virtual const string &getStorageType() const {
        return storageType;
    }
};


#endif //PROJECT_STORAGE_H
