//
// Created by dell-pc on 2018/5/6.
//

#ifndef PROJECT_NETWORK_H
#define PROJECT_NETWORK_H

#include <queue>
#include <memory>
#include <netinet/in.h>
#include "raft.pb.h"
#include <arpa/inet.h>
#include "co_routine.h"
#include "Log.h"

using namespace std;

int SetNonBlock(int iSock);
int CreateUdpSecket(char *host, int port, int reuse);

struct MsgHead {
    int datalen; //data len
    int magic;
};


class Network {
public:
    Network(int selfnodeFd) : selfnodeFd(selfnodeFd) {
        cond = co_cond_alloc();
    }

    void pushMsg(pair<shared_ptr<struct sockaddr_in>, shared_ptr<jraft::Network::Msg>> msg) {
        queueMsg.push(msg);
        co_cond_signal(cond);
    }

    int getSelfnodeFd() const {
        return selfnodeFd;
    }

    const queue<pair<shared_ptr<sockaddr_in>, shared_ptr<jraft::Network::Msg>>> &getQueueMsg() const {
        return queueMsg;
    }

    stCoCond_t *getCond() const {
        return cond;
    }

    void sendMsg(const pair<string, int> &pair, jraft::Network::Msg &msg);

    shared_ptr<pair<shared_ptr<pair<string, int>>, shared_ptr<jraft::Network::Msg>>> waitMsgTimeOut(int m_sec);

    static shared_ptr<pair<string, int>> address2pair(shared_ptr<sockaddr_in>);

    static shared_ptr<sockaddr_in> host2address(string &host, int port);

private:
    int selfnodeFd;
    queue<pair<shared_ptr<struct sockaddr_in>, shared_ptr<jraft::Network::Msg>>> queueMsg;
    stCoCond_t* cond;
};

int CreateTcpSocket(const unsigned short shPort /* = 0 */,const char *pszIP /* = "*" */,bool bReuse /* = false */);

#endif //PROJECT_NETWORK_H
