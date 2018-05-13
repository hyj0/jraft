//
// Created by dell-pc on 2018/5/6.
//

#ifndef PROJECT_CONFIG_H
#define PROJECT_CONFIG_H

#include <iostream>
#include <string>
#include "json11.hpp"

using namespace std;

class GroupCfg {
public:
    const string &getGroupId() const {
        return groupId;
    }

    void setGroupId(const string &groupId) {
        GroupCfg::groupId = groupId;
    }

    const vector<pair<string, int>> &getNodes() const {
        return nodes;
    }

    void setNodes(const vector<pair<string, int>> &nodes) {
        GroupCfg::nodes = nodes;
    }

    const string &getStorage() const {
        return storage;
    }

    void setStorage(const string &storage) {
        GroupCfg::storage = storage;
    }

private:
    string groupId;
    vector<pair<string, int>> nodes;
    string storage;
};

class Config {
public:
    Config() {}
    Config(const string &configFile) : configFile(configFile) {}
    ~Config() {}

    int parseConfig();

    const shared_ptr<pair<string, int>> &getSelfnode() const {
        return selfnode;
    }

    const vector<shared_ptr<GroupCfg>> &getGroups() const {
        return groups;
    }

    const json11::Json &getConfigJson() const {
        return configJson;
    }

private:
    string configFile;
    json11::Json configJson;
    shared_ptr<pair<string, int>> selfnode;
    vector<shared_ptr<GroupCfg>> groups;
};


#endif //PROJECT_CONFIG_H
