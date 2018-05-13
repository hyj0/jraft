//
// Created by dell-pc on 2018/5/13.
//

#include <iostream>
#include <string>
#include <co_routine.h>
#include "Client.h"
#include "Log.h"
#include "raft.pb.h"
#include "Network.h"
#include <unistd.h>
#include "pb2json.h"

using namespace std;

int Client::sendMsg(int fd, std::string host, int port, jraft::Network::Msg msg) {
    Pb2Json::Json json;
    Pb2Json::Message2Json(msg, json, true);
    const shared_ptr<sockaddr_in> &addresss = Network::host2address(host, port);
    string str = json.dump();
    int ret = sendto(fd, str.c_str(), str.length(), 0,
                     reinterpret_cast<const sockaddr *>(addresss.get()), sizeof(struct sockaddr_in));
    return ret;
}


void printUsage()
{
    cout << "usage:" << endl;
    cout << "ls" << endl;
    cout << "get index" << endl;
    cout << "set index key data" << endl;
}

void *mainCoroutine(void *arg)
{
//    co_enable_hook_sys();
    
    char **argv = static_cast<char **>(arg);
    string host = argv[1];
    int port = atoi(argv[2]);
    string groupId = argv[3];

    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) {
        LOG_COUT << "socket" << LOG_ENDL_ERR;
        return reinterpret_cast<void *>(fd);
    }

    Client client;
    
    while (true) {
        int ret;
        char buff[1000];
        memset(buff, 0, 1000);
        int n = read(0, buff, 1000);
        if (n < 0) {
            LOG_COUT << " read err fd=" << 0 << LOG_ENDL_ERR;
            continue;
        }
        char args[4][100];
        n = sscanf(buff, "%s %s %s %s", args[0], args[1], args[2], args[3]);
        if (n <= 0) {
            printUsage();
            continue;
        }
        if (n == 1) {
            if (string(args[0]) != "ls") {
                printUsage();
                continue;
            }
            jraft::Network::Msg msg;
            msg.set_group_id(groupId);
            msg.set_msg_type(jraft::Network::MsgType::MSG_Type_Cli_Request);
            jraft::Network::CliReq *cliReq = msg.mutable_cli_request();
            cliReq->set_request_type(3);
            ret = client.sendMsg(fd, host, port, msg);
            if (ret < 0) {
                LOG_COUT << "sendMsg err ret=" << ret << LOG_ENDL_ERR;
            }

            char buff[1000];
            ret = recvfrom(fd, buff, sizeof(buff), 0, 0, 0);
            if (ret < 0) {
                LOG_COUT << "recvfrom err ret=" << ret << LOG_ENDL_ERR;
                continue;
            }
            string recvmsg = string(buff, ret);
            cout << recvmsg << endl;

            continue;
        }
        if (n == 2) {
            if (string(args[0]) != "get") {
                printUsage();
                continue;
            }
            int index = atoi(args[1]);
            jraft::Network::Msg msg;
            msg.set_group_id(groupId);
            msg.set_msg_type(jraft::Network::MsgType::MSG_Type_Cli_Request);
            jraft::Network::CliReq *cliReq = msg.mutable_cli_request();
            cliReq->set_request_type(2);
            cliReq->set_log_index(index);
            ret = client.sendMsg(fd, host, port, msg);
            if (ret < 0) {
                LOG_COUT << "sendMsg err ret=" << ret << LOG_ENDL_ERR;
            }
            char buff[1000];
            ret = recvfrom(fd, buff, sizeof(buff), 0, 0, 0);
            if (ret < 0) {
                LOG_COUT << "recvfrom err ret=" << ret << LOG_ENDL_ERR;
                continue;
            }
            string recvmsg = string(buff, ret);
            cout << recvmsg << endl;
            continue;
        }
        if (n == 4) {
            if (string(args[0]) != "set") {
                printUsage();
                continue;
            }
            int index = atoi(args[1]);
            string key = args[2];
            string value = args[3];
            jraft::Network::Msg msg;
            msg.set_group_id(groupId);
            msg.set_msg_type(jraft::Network::MsgType::MSG_Type_Cli_Request);
            jraft::Network::CliReq *cliReq = msg.mutable_cli_request();
            cliReq->set_request_type(1);
            cliReq->set_log_index(index);
            cliReq->mutable_log_entry()->set_action("change");
            cliReq->mutable_log_entry()->set_key(key);
            cliReq->mutable_log_entry()->set_value(value);

            ret = client.sendMsg(fd, host, port, msg);
            if (ret < 0) {
                LOG_COUT << "sendMsg err ret=" << ret << LOG_ENDL_ERR;
            }

            char buff[1000];
            ret = recvfrom(fd, buff, sizeof(buff), 0, 0, 0);
            if (ret < 0) {
                LOG_COUT << "recvfrom err ret=" << ret << LOG_ENDL_ERR;
                continue;
            }
            string recvmsg = string(buff, ret);
            cout << recvmsg << endl;

            continue;
        }
    }
}

int main(int argc, char **argv) {
    if (argc != 4) {
        LOG_COUT << "usage:" << argv[0] << " ip port groupId" << LOG_ENDL;
        return -1;
    }

    stCoRoutine_t *ctx = NULL;
    co_create(&ctx, NULL, mainCoroutine, argv);
    co_resume(ctx);

    co_eventloop(co_get_epoll_ct(), NULL, NULL);
}

