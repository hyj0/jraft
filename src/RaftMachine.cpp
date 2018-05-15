//
// Created by dell-pc on 2018/5/6.
//

#include "RaftMachine.h"
#include "Log.h"
#include "Utils.h"
#include <sstream>
#include "pb2json.h"


void RaftMachine::changeRaftStat(RaftStatus to) {

    LOG_COUT << "groupId="<<groupCfg->getGroupId()
             << " ratfStatus:" << g_raftStatusNameMap[raftStatus]
             << "-->" << g_raftStatusNameMap[to] << LOG_ENDL;
    this->raftStatus = to;
}

string RaftMachine::pair2NodeId(pair<string, int> *pPair) {
    stringstream nodeIdBuf;
    nodeIdBuf << pPair->first << ":" << pPair->second;
    return nodeIdBuf.str();
}

string RaftMachine::pair2NodeId(pair<string, int> &pair1) {
    stringstream nodeIdBuf;
    nodeIdBuf << pair1.first << ":" << pair1.second;
    return nodeIdBuf.str();
}

int RaftMachine::followerProcess() {
    while (true)
    {
        cout << endl;
        LOG_COUT << "groupId=" << groupCfg->getGroupId()
                 << " status:" << g_raftStatusNameMap[raftStatus]
                 << " pid=" << Utils::getPid()  << LOG_ENDL;

        shared_ptr<pair<shared_ptr<pair<string, int>>, shared_ptr<jraft::Network::Msg>>> addressMsgPair;
        addressMsgPair = network->waitMsgTimeOut(Utils::randint(1000*6, 1000*9));
        if (addressMsgPair == NULL) {
            /*time out*/
            LOG_COUT << "groupId=" << groupCfg->getGroupId() << " wait time out!" << LOG_ENDL;
            changeRaftStat(RAFT_STAT_CANDIDATER);
            return 0;
        }
        shared_ptr<jraft::Network::Msg> &recvMsg = addressMsgPair->second;
        switch (recvMsg->msg_type()) {
            case jraft::Network::MsgType::MSG_Type_Vote_Request:
            {
                bool result = false;
                jraft::Network::VoteReq *voteReqMsg = recvMsg->mutable_vote_request();
                if (voteReqMsg->term() < raftConfig.current_term()) {
                    result = false;
                } else if ((raftConfig.votefor() == VOTEFOR_NULL
                            || raftConfig.votefor() == voteReqMsg->candidate_id())
                        && raftConfig.last_applied() <= voteReqMsg->last_log_index()) {
                    raftConfig.set_current_term(voteReqMsg->term());
                    raftConfig.set_votefor(voteReqMsg->candidate_id());
                    storage->setRaftConfig(raftConfig);
                    result = true;
                }

                jraft::Network::Msg msg;
                msg.set_group_id(groupCfg->getGroupId());
                msg.set_msg_type(jraft::Network::MsgType::MSG_Type_Vote_Response);
                jraft::Network::VoteRes *voteResMsg = msg.mutable_vote_response();
                voteResMsg->set_term(raftConfig.current_term());
                voteResMsg->set_granted(result);

                network->sendMsg(*addressMsgPair->first.get(), msg);
                if (result) {
                    //restart timer
                    continue;
                }
                break;
            }
            case jraft::Network::MsgType::MSG_Type_Rpc_Request:
            {
                /*todo;follower rpc request*/
                bool result = true;
                jraft::Network::RpcReq *rpcReq = recvMsg->mutable_rpc_request();
                if (rpcReq->term() < raftConfig.current_term()
                        /*todo:compare prelogIndex and term 5.3*/) {
                    result = false;
                } else {
                    Pb2Json::Json json;
                    Pb2Json::Message2Json(*recvMsg.get(), json);
                    LOG_COUT << "rpcReq:" << json.dump() << LOG_ENDL;
                    if (rpcReq->log_entrys_size() != 0) {
                        jraft::Storage::Log log;
                        log.set_log_index(rpcReq->prev_log_index()+1);
                        log.set_term(rpcReq->term());
                        jraft::Storage::LogEntry *entry = log.mutable_log_entry();
                        entry->set_action(rpcReq->log_entrys(0).action());
                        entry->set_key(rpcReq->log_entrys(0).key());
                        entry->set_key(rpcReq->log_entrys(0).value());
                        storage->setRaftLog(log, rpcReq->prev_log_index() + 1);
                        raftConfig.set_last_applied(rpcReq->prev_log_index()+1);
                        LOG_COUT << g_raftStatusNameMap[raftStatus]
                                 << " apply log !"
                                 <<" lastApplyId=" << raftConfig.last_applied() << LOG_ENDL;
                    }

                    if (rpcReq->leader_commit() > raftConfig.commit_index()) {
                        raftConfig.set_commit_index(
                                rpcReq->prev_log_index() > rpcReq->leader_commit() ? rpcReq->leader_commit()
                                                                                   : rpcReq->prev_log_index());
                    }
                    result = true;
                    this->leader_id = rpcReq->leader_id();
                }

                if (rpcReq->term() > raftConfig.current_term()) {
                    raftConfig.set_current_term(rpcReq->term());
                }
                storage->setRaftConfig(raftConfig);

                jraft::Network::Msg msg;
                msg.set_group_id(groupCfg->getGroupId());
                msg.set_msg_type(jraft::Network::MsgType::MSG_Type_Rpc_Response);
                jraft::Network::RpcRes *rpcRes = msg.mutable_rpc_response();
                rpcRes->set_term(raftConfig.current_term());
                rpcRes->set_success(result);
                rpcRes->set_match_index(raftConfig.last_applied());
                network->sendMsg(*addressMsgPair->first.get(), msg);
                break;
            }
            case jraft::Network::MsgType::MSG_Type_Cli_Request:
            {
                jraft::Network::Msg msg;
                msg.set_group_id(groupCfg->getGroupId());
                msg.set_msg_type(jraft::Network::MsgType::MSG_Type_Cli_Response);
                jraft::Network::CliRes *cliRes = msg.mutable_cli_response();
                cliRes->set_result(1);
                cliRes->set_raft_state(raftStatus);
                cliRes->set_leader_id(leader_id);
                cliRes->set_commit_index(raftConfig.commit_index());
                cliRes->set_last_log_index(raftConfig.last_applied());
                network->sendMsg(*addressMsgPair->first.get(), msg);
                break;
            }
            default:
                LOG_COUT << "groupId=" << groupCfg->getGroupId() << " " << g_raftStatusNameMap[raftStatus]
                         << " recv MsgType err msgType=" << recvMsg->msg_type() << LOG_ENDL;
                break;
        }
    }
}

int RaftMachine::candidaterProcess() {
    while (true)
    {
        cout << endl;
        LOG_COUT << "groupId=" << groupCfg->getGroupId()
                 << " status:" << g_raftStatusNameMap[raftStatus]
                 << " pid=" << Utils::getPid()  << LOG_ENDL;

        int voteCount = 0;
        this->raftConfig.set_current_term(raftConfig.current_term()+1);
        this->raftConfig.set_votefor(this->pair2NodeId(selfNode.get()));
        voteCount += 1;//self vote
        storage->setRaftConfig(raftConfig);

        for (int i = 0; i < groupCfg->getNodes().size(); ++i) {
            jraft::Network::Msg msg;
            msg.set_group_id(groupCfg->getGroupId());
            msg.set_msg_type(jraft::Network::MsgType::MSG_Type_Vote_Request);
            jraft::Network::VoteReq *voteReq = msg.mutable_vote_request();
            voteReq->set_term(raftConfig.current_term());
            voteReq->set_candidate_id(this->pair2NodeId(selfNode.get()));
            voteReq->set_last_log_index(raftConfig.last_applied());
            voteReq->set_last_log_term(raftConfig.last_log_term());

            network->sendMsg(groupCfg->getNodes()[i], msg);
        }

        Timer timer(Utils::randint(1000*6, 1000*9));
        while (timer.hasRemainTime()) {
            int timeout_ms = timer.getRemainTime();
            if (timeout_ms < 0) {
                LOG_COUT << "Timer bug here" << LOG_ENDL;
                break;
            }
            shared_ptr<pair<shared_ptr<pair<string, int>>, shared_ptr<jraft::Network::Msg>>> addressMsgPair;
            addressMsgPair = network->waitMsgTimeOut(timeout_ms);
            if (addressMsgPair.get() == NULL) {
                LOG_COUT << g_raftStatusNameMap[raftStatus] << " wait time out !" << LOG_ENDL;
                break;
            }
            shared_ptr<jraft::Network::Msg> &recvMsg = addressMsgPair->second;
            switch (recvMsg->msg_type()) {
                case jraft::Network::MsgType::MSG_Type_Vote_Response: {
                    jraft::Network::VoteRes *voteResponse = recvMsg->mutable_vote_response();
                    if (voteResponse->term() > raftConfig.current_term()) {
                        raftConfig.set_current_term(voteResponse->term());
                        raftConfig.set_votefor(VOTEFOR_NULL);
                        storage->setRaftConfig(raftConfig);

                        /*todo: reset vote??? */
                        voteCount = 0;
                        changeRaftStat(RAFT_STAT_FOLLOWER);
                        break;
                    }

                    if (voteResponse->granted()) {
                        voteCount += 1;
                        if (voteCount >= ((groupCfg->getNodes().size() + 1)/2+1)) {
                            LOG_COUT << "groupId=" << groupCfg->getGroupId() <<  " I am Leader !!!" << LOG_ENDL;
                            changeRaftStat(RAFT_STAT_LEADER);
                            vector<string> nodeIds;
                            for (int i = 0; i < groupCfg->getNodes().size(); ++i) {
                                nodeIds.push_back(pair2NodeId(
                                        const_cast<pair<string, int> *>(&groupCfg->getNodes()[i])));
                            }
                            nodesLogInfo = make_shared<NodesLogInfo>(nodeIds, raftConfig.last_applied() + 1);
                            return 0;
                        }
                    }
                    break;
                }
                case jraft::Network::MsgType::MSG_Type_Vote_Request:
                {
                    bool result = false;
                    jraft::Network::VoteReq *voteReqMsg = recvMsg->mutable_vote_request();
                    if (voteReqMsg->term() <= raftConfig.current_term()) {
                        result = false;
                    } else {
                        raftConfig.set_current_term(voteReqMsg->term());
                        raftConfig.set_votefor(VOTEFOR_NULL);
                        if (voteReqMsg->last_log_index() >= raftConfig.last_applied()) {
                            raftConfig.set_votefor(voteReqMsg->candidate_id());
                            result = true;
                        }
                    }
                    storage->setRaftConfig(raftConfig);

                    jraft::Network::Msg msg;
                    msg.set_group_id(groupCfg->getGroupId());
                    msg.set_msg_type(jraft::Network::MsgType::MSG_Type_Vote_Response);
                    jraft::Network::VoteRes *voteResMsg = msg.mutable_vote_response();
                    voteResMsg->set_term(raftConfig.current_term());
                    voteResMsg->set_granted(result);

                    network->sendMsg(*addressMsgPair->first.get(), msg);
                    if (result) {
                        changeRaftStat(RAFT_STAT_FOLLOWER);
                        return 0;
                    }
                    break;
                }
                case jraft::Network::MsgType::MSG_Type_Rpc_Request:
                {
                    //todo:Candidater rpc request
                    changeRaftStat(RAFT_STAT_FOLLOWER);
                    jraft::Network::RpcReq *rpcReq = recvMsg->mutable_rpc_request();
                    if (rpcReq->term() > raftConfig.current_term()) {
                        raftConfig.set_current_term(rpcReq->term());
                        storage->setRaftConfig(raftConfig);
                    }
                    this->leader_id = rpcReq->leader_id();
                    changeRaftStat(RAFT_STAT_FOLLOWER);
                    return 0;
                    break;
                }
                case jraft::Network::MsgType::MSG_Type_Cli_Request:
                {
                    jraft::Network::Msg msg;
                    msg.set_group_id(groupCfg->getGroupId());
                    msg.set_msg_type(jraft::Network::MsgType::MSG_Type_Cli_Response);
                    jraft::Network::CliRes *cliRes = msg.mutable_cli_response();
                    cliRes->set_result(1);
                    cliRes->set_raft_state(raftStatus);
                    cliRes->set_commit_index(raftConfig.commit_index());
                    cliRes->set_last_log_index(raftConfig.last_applied());
                    network->sendMsg(*addressMsgPair->first.get(), msg);
                    break;
                }
                default:
                    LOG_COUT << "groupId=" << groupCfg->getGroupId() << " " << g_raftStatusNameMap[raftStatus]
                             << " recv MsgType err msgType=" << recvMsg->msg_type() << LOG_ENDL;
                    break;
            }
        }
    }
    return 0;
}

int RaftMachine::leaderProcess() {
    int initFlag = false;
    while (true)
    {
        cout << endl;
        LOG_COUT << "groupId=" << groupCfg->getGroupId()
                 << " status:" << g_raftStatusNameMap[raftStatus]
                 << " pid=" << Utils::getPid()  << LOG_ENDL;

        int waitTime_ms = 1000*3;
        if (!initFlag) {
            /*begin become Leader do not wait*/
            initFlag = true;
            waitTime_ms = -100;
        }
        Timer timer(waitTime_ms);
        while (timer.hasRemainTime()) {
            int timeout_ms = timer.getRemainTime();
            if (timeout_ms < 0) {
                LOG_COUT << "Timer bug here" << LOG_ENDL;
                break;
            }
            shared_ptr<pair<shared_ptr<pair<string, int>>, shared_ptr<jraft::Network::Msg>>> addressMsgPair;
            addressMsgPair = network->waitMsgTimeOut(timeout_ms);
            if (addressMsgPair.get() != NULL) {
                shared_ptr<jraft::Network::Msg> &recvMsg = addressMsgPair->second;
                switch (recvMsg->msg_type()) {
                    case jraft::Network::MsgType::MSG_Type_Vote_Request:
                    {
                        bool need2Follower = false;
                        bool result = false;
                        jraft::Network::VoteReq *voteReqMsg = recvMsg->mutable_vote_request();
                        if (voteReqMsg->term() <= raftConfig.current_term()) {
                            result = false;
                        } else {
                            raftConfig.set_current_term(voteReqMsg->term());
                            raftConfig.set_votefor(voteReqMsg->candidate_id());
                            if (voteReqMsg->last_log_index() >= raftConfig.last_applied()) {
                                result = true;
                            }
                            need2Follower = true;
                        }
                        storage->setRaftConfig(raftConfig);

                        jraft::Network::Msg msg;
                        msg.set_group_id(groupCfg->getGroupId());
                        msg.set_msg_type(jraft::Network::MsgType::MSG_Type_Rpc_Request);
                        jraft::Network::VoteRes *voteResMsg = msg.mutable_vote_response();
                        voteResMsg->set_term(raftConfig.current_term());
                        voteResMsg->set_granted(result);

                        network->sendMsg(*addressMsgPair->first.get(), msg);
                        if (need2Follower) {
                            changeRaftStat(RAFT_STAT_FOLLOWER);
                            return 0;
                        }
                        break;
                    }
                    case jraft::Network::MsgType::MSG_Type_Rpc_Response:
                    {
                        //todo:leader rpc response
                        jraft::Network::RpcRes *rpcRes = recvMsg->mutable_rpc_response();
                        string nodeId = pair2NodeId(*addressMsgPair->first);
                        if (rpcRes->term() > raftConfig.current_term()) {
                            raftConfig.set_current_term(rpcRes->term());
                            raftConfig.set_votefor(VOTEFOR_NULL);
                            storage->setRaftConfig(raftConfig);
                            changeRaftStat(RAFT_STAT_FOLLOWER);
                            return 0;
                        }
                        nodesLogInfo->setNextIndex(nodeId, rpcRes->match_index()+1);

                        nodesLogInfo->setMatchIndex(nodeId, rpcRes->match_index());
                        raftConfig.set_commit_index(nodesLogInfo->getMaxCommitedId());
                        storage->setRaftConfig(raftConfig);
                        break;
                    }
                    case jraft::Network::MsgType::MSG_Type_Cli_Request:
                    {
                        jraft::Network::Msg msg;
                        msg.set_group_id(groupCfg->getGroupId());
                        msg.set_msg_type(jraft::Network::MsgType::MSG_Type_Cli_Response);
                        jraft::Network::CliRes *cliRes = msg.mutable_cli_response();
                        cliRes->set_raft_state(raftStatus);
                        cliRes->set_leader_id(pair2NodeId(*selfNode));
                        cliRes->set_result(0);//default

                        jraft::Network::CliReq *cliReq = recvMsg->mutable_cli_request();
                        switch (cliReq->request_type()) {
                            case 1:
                            {
                                if (cliReq->log_index() != raftConfig.last_applied() + 1) {
                                    cliRes->set_result(3);
                                    cliRes->set_err_msg("logIndex err");
                                    break;
                                }

                                if (!cliReq->has_log_entry()) {
                                    cliRes->set_result(3);
                                    cliRes->set_err_msg("no entry");
                                    break;
                                }

                                jraft::Storage::Log configLog;
                                configLog.set_log_index(raftConfig.last_applied() + 1);
                                configLog.set_term(raftConfig.current_term());
                                jraft::Storage::LogEntry *entry = configLog.mutable_log_entry();
                                entry->set_action(cliReq->mutable_log_entry()->action());
                                entry->set_key(cliReq->mutable_log_entry()->key());
                                entry->set_value(cliReq->mutable_log_entry()->value());
                                raftConfig.set_last_applied(raftConfig.last_applied() + 1);
                                storage->setRaftConfig(raftConfig);
                                storage->setRaftLog(configLog, raftConfig.last_applied());
                                break;
                            }
                            case 2:
                            {
                                if (cliReq->log_index() < 1 || cliReq->log_index() > raftConfig.last_applied()) {
                                    cliRes->set_result(3);
                                    cliRes->set_err_msg("log_index err");
                                    break;
                                }
                                shared_ptr<jraft::Storage::Log> entryLog = storage->getRaftLog(cliReq->log_index());
                                cliRes->mutable_log_entry()->set_action(entryLog->mutable_log_entry()->action());
                                cliRes->mutable_log_entry()->set_key(entryLog->mutable_log_entry()->key());
                                cliRes->mutable_log_entry()->set_value(entryLog->mutable_log_entry()->value());

                                if (cliReq->log_index() <= raftConfig.commit_index()) {
                                    cliRes->set_key_state(1);
                                } else {
                                    cliRes->set_key_state(2);
                                }
                                break;
                            }
                            case 3:
                            {
                                break;
                            }
                            default:
                                cliRes->set_result(4);
                                cliRes->set_err_msg("request_type err");
                                break;
                        }
                        cliRes->set_commit_index(raftConfig.commit_index());
                        cliRes->set_last_log_index(raftConfig.last_applied());
                        network->sendMsg(*addressMsgPair->first.get(), msg);
                        break;
                    }
                    default:
                        LOG_COUT << "groupId=" << groupCfg->getGroupId() << " " << g_raftStatusNameMap[raftStatus]
                                 << " recv MsgType err msgType=" << recvMsg->msg_type() << LOG_ENDL;
                        break;
                }
            }
        }

        /*add group config to log*/
        if (raftConfig.last_applied() == 0) {
            jraft::Storage::Log configLog;
            configLog.set_log_index(0);
            configLog.set_term(raftConfig.current_term());
            jraft::Storage::LogEntry *entry = configLog.mutable_log_entry();
            entry->set_action("change");
            entry->set_key("__raft_group_nodes__");
            vector<string> nodesArray;
            for (int i = 0; i < groupCfg->getNodes().size(); ++i) {
                nodesArray.push_back(pair2NodeId((pair<string, int>&)groupCfg->getNodes()[i]));
            }
            nodesArray.push_back(pair2NodeId(this->selfNode.get()));
            Pb2Json::Json json = nodesArray;
            LOG_COUT << "nodesJon=" << json.dump() << LOG_ENDL;
            entry->set_value(json.dump());
            raftConfig.set_last_applied(raftConfig.last_applied() + 1);
            storage->setRaftConfig(raftConfig);
            storage->setRaftLog(configLog, raftConfig.last_applied());
        }

        for (int i = 0; i < groupCfg->getNodes().size(); ++i) {
            jraft::Network::Msg msg;
            msg.set_group_id(groupCfg->getGroupId());
            msg.set_msg_type(jraft::Network::MsgType::MSG_Type_Rpc_Request);
            jraft::Network::RpcReq *rpcReq = msg.mutable_rpc_request();
            rpcReq->set_term(raftConfig.current_term());
            rpcReq->set_leader_id(pair2NodeId(selfNode.get()));
            pair<string, int> nodeId = groupCfg->getNodes()[i];
            int nodeNextId = nodesLogInfo->getNextIndex(pair2NodeId(nodeId));
            rpcReq->set_prev_log_index(nodeNextId-1);
            rpcReq->set_prev_log_term(raftConfig.last_log_term());
            rpcReq->set_leader_commit(raftConfig.commit_index());

            shared_ptr<jraft::Storage::Log> log = storage->getRaftLog(nodeNextId);
            if (log != NULL) {
                Pb2Json::Json json;
                Pb2Json::Message2Json(*log.get(), json);
                LOG_COUT << "index=" << nodeNextId << " log=" << json.dump() << LOG_ENDL;
                jraft::Network::LogEntry *logEntry = rpcReq->add_log_entrys();
                logEntry->set_action(log->mutable_log_entry()->action());
                logEntry->set_key(log->mutable_log_entry()->key());
                logEntry->set_value(log->mutable_log_entry()->value());
                rpcReq->set_prev_log_term(log->term());
            }
            network->sendMsg(groupCfg->getNodes()[i], msg);
        }
    }
    return 0;
}

void RaftMachine::start() {
    while (true) {
        cout << endl;
        LOG_COUT << "groupId=" << groupCfg->getGroupId()
                 << " status:" << g_raftStatusNameMap[raftStatus]
                 << " pid=" << Utils::getPid()  << LOG_ENDL;
        switch (this->raftStatus) {
            case RAFT_STAT_CANDIDATER:
                candidaterProcess();
                break;
            case RAFT_STAT_FOLLOWER:
                followerProcess();
                break;
            case RAFT_STAT_LEADER:
                leaderProcess();
                break;
            default:
                LOG_COUT << " err status!! raftStatus=" << raftStatus << LOG_ENDL;
                break;
        }
    }
}