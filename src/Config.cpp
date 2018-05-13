//
// Created by dell-pc on 2018/5/6.
//

#include "Config.h"
#include "Log.h"
#include<fstream>
#include <sstream>

int Config::parseConfig() {
    ifstream infile(this->configFile);
    if (!infile) {
        LOG_COUT << "open config file" << LOG_ENDL_ERR;
        return -1;
    }
    string jsonStr;
    while (!infile.eof()) {
        char buff[1000];
        memset(buff, 0, sizeof(buff));
        infile.read(buff, 1000);
        jsonStr += buff;
    }

    string errs;
    this->configJson  =  json11::Json::parse(jsonStr, errs, json11::JsonParse::COMMENTS);
    if (this->configJson.is_null()) {
        LOG_COUT << "json parse err="<<errs << LOG_ENDL;
        return -1;
    }
    string selfHost = configJson["selfnode"]["host"].string_value();
    int selfPort = configJson["selfnode"]["port"].int_value();
    this->selfnode = make_shared<pair<string, int>>(selfHost, selfPort);

    json11::Json::array groupsArr = configJson["groups"].array_items();
    for (int i = 0; i < groupsArr.size(); ++i) {
        shared_ptr<GroupCfg> oneGroup = make_shared<GroupCfg>();
        oneGroup->setGroupId(groupsArr[i]["groupId"].string_value());
        oneGroup->setStorage(groupsArr[i]["storage"].string_value());

        json11::Json::array groupNodes = groupsArr[i]["nodes"].array_items();
        vector<pair<string, int>> nodes;
        for (int j = 0; j < groupNodes.size(); ++j) {
            string host = groupNodes[j]["host"].string_value();
            int port =  groupNodes[j]["port"].int_value();
            if (host == selfnode.get()->first
                    && port == selfnode.get()->second) {
                continue;
            }
            nodes.push_back(make_pair<string, int>(static_cast<string &&>(host), reinterpret_cast<int &&>(port)));
        }
        oneGroup.get()->setNodes(nodes);
        this->groups.push_back(oneGroup);
    }
    return 0;
}
