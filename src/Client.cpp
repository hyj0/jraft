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
#include <sys/time.h>

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
    cout << "perf count" << endl;
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
        if (n == 2 && string(args[0]) == "get") {
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
        if (n == 2 && string(args[0]) == "perf") {
            int index;
            int count = atoi(args[1]);
            string key = "k";
            string value = "v";
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
            Pb2Json::Json json = Pb2Json::Json::parse(recvmsg);
            shared_ptr<jraft::Network::Msg> recvMsg = make_shared<jraft::Network::Msg>();
            Pb2Json::Json2Message(json, *recvMsg.get(), true);
            if (recvMsg->msg_type() == jraft::Network::MsgType::MSG_Type_Cli_Response) {
                jraft::Network::CliRes *cliRes = recvMsg->mutable_cli_response();
                if (cliRes->commit_index() == cliRes->last_log_index()) {
                    if (cliRes->commit_index() == -1) {
                        cout << "err ! cliRes->commit_index() != index-1  " << cliRes->commit_index() << " " << index-1 << endl;
                        break;//
                    }
                }
                index= cliRes->last_log_index()+1;
            }
            struct timeval tStart;
            gettimeofday(&tStart, NULL);
            for (int i = 0; i < count; ++i, ++index) {
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
            }
            {
                struct timeval tEnd;
                gettimeofday(&tEnd, NULL);
                double speed = count*1000.0/((tEnd.tv_sec*1000 + tEnd.tv_usec/1000) - (tStart.tv_sec*1000 + tStart.tv_usec/1000));
                cout << "put speed " << speed << "qps"<< endl;
            }
            while (1) {
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

                Pb2Json::Json json = Pb2Json::Json::parse(recvmsg);
                shared_ptr<jraft::Network::Msg> recvMsg = make_shared<jraft::Network::Msg>();
                Pb2Json::Json2Message(json, *recvMsg.get(), true);
                if (recvMsg->msg_type() == jraft::Network::MsgType::MSG_Type_Cli_Response) {
                    jraft::Network::CliRes *cliRes = recvMsg->mutable_cli_response();
                    if (cliRes->commit_index() == cliRes->last_log_index()) {
                        if (cliRes->commit_index() != index-1) {
                            cout << "err ! cliRes->commit_index() != index-1  " << cliRes->commit_index() << " " << index-1 << endl;
                        }
                        break;
                    }
                }
                usleep(1000*200);
            }
            struct timeval tEnd;
            gettimeofday(&tEnd, NULL);
            double speed = count * 1000.0 / ((tEnd.tv_sec * 1000 + tEnd.tv_usec / 1000) - (tStart.tv_sec * 1000 + tStart.tv_usec / 1000));
            cout << "ok speed " << speed << "qps" << endl;
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

