//
// Created by hyj on 2020-01-20.
//

#ifndef PROJECT_STORAGE_ROCKSDB_H
#define PROJECT_STORAGE_ROCKSDB_H


#include "Storage.h"
#include <rocksdb/db.h>
#include "pb2json.h"
#include "RaftMachine.h"

class Storage_rocksdb:public Storage {
private:
    rocksdb::DB *db;
    string dbName;
    int dbStatus;
    string key_GroupCfg = "groupCfg";
    string key_RaftConfig = "RaftConfig";
    string key_RaftLog = "RaftLog_";
public:
    Storage_rocksdb(string & storageName, string nodeId):Storage(storageName, nodeId) {
        dbName = storageName + "_" + nodeId + ".db";
        rocksdb::Options options;
        options.create_if_missing = true;
        rocksdb::Status status = rocksdb::DB::Open(options, dbName, &db);
        if (!status.ok()) {
            LOG_COUT << "open err:" << status.ToString() << LOG_ENDL;
            dbStatus = 1;
        } else {
            dbStatus = 0;
        }
        /*init raft config*/
        raftConfig.set_current_term(0);
        raftConfig.set_votefor(VOTEFOR_NULL);
        raftConfig.set_commit_index(0);
        raftConfig.set_last_applied(0);
        raftConfig.set_last_log_term(0);
    }
    ~Storage_rocksdb() {
    }

    const GroupCfg &getGroupCfg() const override {
        return Storage::getGroupCfg();
    }

    void setGroupCfg(const GroupCfg &groupCfg) override {
        string value = "keke";
        rocksdb::Status status;
        rocksdb::WriteOptions writeOptions = rocksdb::WriteOptions();
        writeOptions.sync = true;
        status = db->Put(writeOptions, key_GroupCfg, value);
        Storage::setGroupCfg(groupCfg);
    }

    const jraft::Storage::RaftConfig &getRaftConfig() const override {
        string strJson;
        rocksdb::Status status = db->Get(rocksdb::ReadOptions(), key_RaftConfig, &strJson);
        if (status.ok()) {
            jraft::Storage::RaftConfig msg;
            Pb2Json::Json  json = Pb2Json::Json::parse(strJson);
            Pb2Json::Json2Message(json, msg, true);
            Storage::setRaftConfig(msg);
            LOG_COUT << "raftConfig=" << strJson << LOG_ENDL;
        } else {
            LOG_COUT << "get err:" << status.ToString() << LOG_ENDL;
        }
        return Storage::getRaftConfig();
    }

    void setRaftConfig(const jraft::Storage::RaftConfig &raftConfig) override {
        Pb2Json::Json json;
        Pb2Json::Message2Json(raftConfig, json, true);
        rocksdb::WriteOptions writeOptions = rocksdb::WriteOptions();
        writeOptions.sync = true;
        rocksdb::Status status = db->Put(writeOptions, key_RaftConfig, json.dump());
        if (!status.ok()) {
            LOG_COUT << "put err:" << LOG_ENDL;
        }
        Storage::setRaftConfig(raftConfig);
    }

    shared_ptr<jraft::Storage::Log> getRaftLog(int logIndex) override {
        shared_ptr<jraft::Storage::Log>  log = make_shared<jraft::Storage::Log>();
        string strJson;
        rocksdb::Status status = db->Get(rocksdb::ReadOptions(), getRaftLogKey(logIndex), &strJson);
        if (status.ok()) {
            Pb2Json::Json json = Pb2Json::Json::parse(strJson);
            Pb2Json::Json2Message(json, *log.get(), true);
            return log;
        } else {
            LOG_COUT << "get log index=" << logIndex << " err:" << status.ToString() << LOG_ENDL;
            return shared_ptr<jraft::Storage::Log>();
        }
    }

    int setRaftLog(jraft::Storage::Log &log, int index) override {
        Pb2Json::Json json;
        Pb2Json::Message2Json(log, json, true);
        rocksdb::WriteOptions writeOptions = rocksdb::WriteOptions();
        writeOptions.sync = true;
        rocksdb::Status status = db->Put(writeOptions, getRaftLogKey(index), json.dump());
        if (!status.ok()) {
            LOG_COUT << "put err:" << LOG_ENDL;
            return -1;
        }
        return 0;
    }

    string getRaftLogKey(int index) {
        stringstream strBuff;
        strBuff << key_RaftLog << index;
        return strBuff.str();
    }
};


#endif //PROJECT_STORAGE_ROCKSDB_H
