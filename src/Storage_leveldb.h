//
// Created by dell-pc on 2018/5/14.
//

#ifndef PROJECT_STORAGE_LEVELDB_H
#define PROJECT_STORAGE_LEVELDB_H

#include <iostream>
#include <sstream>
#include "Storage.h"
#include "leveldb/db.h"
#include "Log.h"
#include "pb2json.h"
#include "RaftMachine.h"

using namespace std;
using namespace leveldb;

class Storage_leveldb:public Storage {
private:
    DB *db;
    string dbName;
    int dbStatus;
    string key_GroupCfg = "groupCfg";
    string key_RaftConfig = "RaftConfig";
    string key_RaftLog = "RaftLog_";
public:
    Storage_leveldb(string & storageName, string nodeId):Storage(storageName, nodeId) {
        dbName = storageName + "_" + nodeId + ".db";
        Options options;
        options.create_if_missing = true;
        Status status = DB::Open(options, dbName, &db);
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
        raftConfig.set_max_log_index(0);
//        raftConfig.set_last_log_term(0);
        storageType = "leveldb";
    }
    ~Storage_leveldb() {
    }

    const GroupCfg &getGroupCfg() const override {
        return Storage::getGroupCfg();
    }

    void setGroupCfg(const GroupCfg &groupCfg) override {
        string value = "keke";
        Status status;
        WriteOptions writeOptions = leveldb::WriteOptions();
        writeOptions.sync = true;
        status = db->Put(writeOptions, key_GroupCfg, value);
        Storage::setGroupCfg(groupCfg);
    }

    const jraft::Storage::RaftConfig &getRaftConfig() const override {
        string strJson;
        leveldb::Status status = db->Get(leveldb::ReadOptions(), key_RaftConfig, &strJson);
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
        WriteOptions writeOptions = leveldb::WriteOptions();
        writeOptions.sync = true;
        Status status = db->Put(writeOptions, key_RaftConfig, json.dump());
        if (!status.ok()) {
            LOG_COUT << "put err:" << LOG_ENDL;
        }
        Storage::setRaftConfig(raftConfig);
    }

    shared_ptr<jraft::Storage::Log> getRaftLog(int logIndex) override {
        if (logIndex > raftConfig.max_log_index()) {
            return nullptr;
        }
//        if (noWriteBuffMap.find(logIndex) != noWriteBuffMap.end()) {
//            jraft::Storage::Log *plog =  new jraft::Storage::Log(*noWriteBuffMap[logIndex]);
//            return shared_ptr<jraft::Storage::Log>(plog);
//        }
        shared_ptr<jraft::Storage::Log>  log = make_shared<jraft::Storage::Log>();
        string strJson;
        leveldb::Status status = db->Get(leveldb::ReadOptions(), getRaftLogKey(logIndex), &strJson);
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
        WriteOptions writeOptions = leveldb::WriteOptions();
        writeOptions.sync = true;
        Status status = db->Put(writeOptions, getRaftLogKey(index), json.dump());
        if (!status.ok()) {
            LOG_COUT << "put err:" << LOG_ENDL;
            return -1;
        }
        return 0;
    }
#if 0
    int setRaftLog(vector<jraft::Storage::Log> &logVect) override {
        leveldb::WriteBatch writeBatch;
        for (int i = 0; i < logVect.size(); ++i) {
            Pb2Json::Json json;
            Pb2Json::Message2Json(logVect[i], json, true);
            leveldb::WriteOptions writeOptions = leveldb::WriteOptions();
            writeBatch.Put(getRaftLogKey(logVect[i].log_index()), json.dump());
        }
        leveldb::WriteOptions writeOptions = leveldb::WriteOptions();
        writeOptions.sync = true;
        leveldb::Status status = db->Write(writeOptions, &writeBatch);
        if (!status.ok()) {
            LOG_COUT << "put err:" << LOG_ENDL;
            return -1;
        }
        return 0;
    }

    int setRaftLog(vector<jraft::Storage::Log> &logVect, const jraft::Storage::RaftConfig &raftConfig) override {
        leveldb::WriteBatch writeBatch;
        for (int i = 0; i < logVect.size(); ++i) {
            Pb2Json::Json json;
            Pb2Json::Message2Json(logVect[i], json, true);
            leveldb::WriteOptions writeOptions = leveldb::WriteOptions();
            writeBatch.Put(getRaftLogKey(logVect[i].log_index()), json.dump());
        }

        Pb2Json::Json json;
        Pb2Json::Message2Json(raftConfig, json, true);
        writeBatch.Put(key_RaftConfig, json.dump());

        leveldb::WriteOptions writeOptions = leveldb::WriteOptions();
        writeOptions.sync = true;
        leveldb::Status status = db->Write(writeOptions, &writeBatch);
        if (!status.ok()) {
            LOG_COUT << "put err:" << LOG_ENDL;
            return -1;
        }
        Storage::setRaftConfig(raftConfig);
        return 0;
    }
#endif
    string getRaftLogKey(int index) {
        stringstream strBuff;
        strBuff << key_RaftLog << index;
        return strBuff.str();
    }
};


#endif //PROJECT_STORAGE_LEVELDB_H
