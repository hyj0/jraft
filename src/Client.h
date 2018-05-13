//
// Created by dell-pc on 2018/5/13.
//

#ifndef PROJECT_CLIENT_H
#define PROJECT_CLIENT_H

#include <raft.pb.h>

class Client {

public:
    int sendMsg(int fd, std::string host, int port, jraft::Network::Msg msg);
};

#endif //PROJECT_CLIENT_H
