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
private:
    jraft::Storage::RaftConfig raftConfig;
    GroupCfg groupCfg;
    vector<jraft::Storage::Log> raftLogArray;
public:
    Storage () {
        jraft::Storage::Log log;
        raftLogArray.push_back(log);
    }

    const GroupCfg &getGroupCfg() const {
        return groupCfg;
    }

    void setGroupCfg(const GroupCfg &groupCfg) {
        Storage::groupCfg = groupCfg;
    }

    const jraft::Storage::RaftConfig &getRaftConfig() const {
        return raftConfig;
    }

    void setRaftConfig(const jraft::Storage::RaftConfig &raftConfig) {
        Storage::raftConfig = raftConfig;
    }

    shared_ptr<jraft::Storage::Log> getRaftLog(int logIndex) {
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

    int setRaftLog(jraft::Storage::Log &log, int index) {
        if (index < raftLogArray.size()) {
            raftLogArray[index] = log;
        } else {
            if (index == raftLogArray.size()) {
                raftLogArray.push_back(log);
            } else {
                LOG_COUT << "logIndex err logIndex=" << index << LOG_ENDL;
                return -1;
            }
        }
        return 0;
    }
};


#endif //PROJECT_STORAGE_H
