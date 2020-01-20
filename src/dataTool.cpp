//
// Created by hyj on 2020-01-20.
//

#include "Log.h"
#include <rocksdb/db.h>
#include<fstream>

int main(int argc, char **argv) {
    if (argc != 4) {
        LOG_COUT << "usage:" << argv[0] << " load data.json rocksdbDir" << LOG_ENDL;
        return -1;        
    }
    string dataJson(argv[2]);
    string dbDir(argv[3]);

    rocksdb::DB *db;
    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::Status status = rocksdb::DB::Open(options, dbDir, &db);
    if (!status.ok()) {
        LOG_COUT << "open err:" << status.ToString() << LOG_ENDL;
        return -1;
    } else {
    }

    ifstream infile(dataJson);
    if (!infile) {
        LOG_COUT << "open config file" << LOG_ENDL_ERR;
        return -1;
    }
    string jsonStr;
    while (!infile.eof()) {
        char buff[1000];
        memset(buff, 0, sizeof(buff));
        infile.getline(buff, sizeof(buff));
        string line(buff);
        if (line.length() == 0) {
            continue;
        }
        cout << line << endl;
        long index = line.find(" ");
        string name(line.substr(0, index));
        index = line.find(":");
        index = line.find("{", index);
        string json = line.substr(index);
        cout << "name=" << name << endl;
        cout << "json=" << json << endl;
        rocksdb::WriteOptions writeOptions = rocksdb::WriteOptions();
        writeOptions.sync = true;
        status = db->Put(writeOptions, name, json);
        if (!status.ok()) {
            LOG_COUT << "put err!" << status.ToString() << LOG_ENDL_ERR;
            return status.code();
        }
    }
}
