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

        Timer timer(Utils::randint(1000*4, 1000*6));
        while (timer.hasRemainTime()) {
            int timeout_ms = timer.getRemainTime();
            if (timeout_ms < 0) {
                LOG_COUT << "Timer bug here" << LOG_ENDL;
                break;
            }
            shared_ptr<pair<shared_ptr<pair<string, int>>, shared_ptr<jraft::Network::Msg>>> addressMsgPair;
            addressMsgPair = network->waitMsgTimeOut(timeout_ms);
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
                    if (voteReqMsg->term() <= raftConfig.current_term()) {
                        result = false;
                    } else  {
                        raftConfig.set_current_term(voteReqMsg->term());
                        raftConfig.set_votefor(VOTEFOR_NULL);
                        if (voteReqMsg->last_log_term() != getLastLogTerm()) {
                            if (voteReqMsg->last_log_term() > getLastLogTerm()) {
                                result = true;
                            } else {
                                result = false;
                            }
                        } else {
                            //last term相同
                            if (voteReqMsg->last_log_index() >= raftConfig.max_log_index()) {
                                result = true;
                            } else {
                                result = false;
                            }
                        }
                        if (result == true) {
                            raftConfig.set_votefor(voteReqMsg->candidate_id());
                            result = true;
                        }
                        storage->setRaftConfig(raftConfig);
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
                        timer.resetTime(Utils::randint(1000*4, 1000*6));
                        continue;
                    }
                    break;
                }
                case jraft::Network::MsgType::MSG_Type_Rpc_Request:
                {
                    /*todo;follower rpc request*/
                    bool reset_timer = true;
                    bool result = true;
                    jraft::Network::RpcReq *rpcReq = recvMsg->mutable_rpc_request();
                    if (rpcReq->term() < raftConfig.current_term()
                        /*todo:compare prelogIndex and term 5.3*/) {
                        result = false;
                        reset_timer = false;
                    } else {
//                        Pb2Json::Json json;
//                        Pb2Json::Message2Json(*recvMsg.get(), json);
//                        LOG_COUT << "rpcReq:" << json.dump() << LOG_ENDL;
                        const shared_ptr<jraft::Storage::Log> &localPreLog = storage->getRaftLog(rpcReq->prev_log_index());
                        if ((localPreLog == nullptr
                             || (localPreLog != nullptr && localPreLog->term() != rpcReq->prev_log_term()))
                             && rpcReq->prev_log_index() != 0) {
                            //rpcReq->prev_log_index() == 0 强行附加!!!
                            LOG_COUT << "log not match index=" << rpcReq->prev_log_index() <<" "
                            << (localPreLog != nullptr ? localPreLog->term():-00) << " != " << rpcReq->prev_log_term() << LOG_ENDL;
                            //如果不一致的位置prev_log_index < raftConfig.commit_index, 说明有bug
                            if (rpcReq->prev_log_index() < raftConfig.commit_index()) {
                                LOG_COUT << "prev_log_index < raftConfig.commit_index !!! " << rpcReq->prev_log_index() << " < " << raftConfig.commit_index() << LOG_ENDL;
                                assert(0);
                            }
                            //todo:如果已经存在的日志条目和新的产生冲突（索引值相同但是任期号不同），删除这一条和之后所有的 （5.3 节）
                            if (raftConfig.max_log_index() > rpcReq->prev_log_index()) {
                                //删除就简单的设置max_log_index即可
                                raftConfig.set_max_log_index(rpcReq->prev_log_index()-1);
                            }
                            result = false;
                        } else {
                            if (rpcReq->term() > raftConfig.current_term()) {
                                raftConfig.set_current_term(rpcReq->term());
                            }

                            vector<jraft::Storage::Log> batchVect;
                            for (int i = 0; i < rpcReq->log_entrys_size(); ++i) {
                                //:支持多个log_entrys
                                jraft::Storage::Log log;
                                log.set_log_index(rpcReq->log_entrys(i).index());
                                log.set_term(rpcReq->log_entrys(i).term());
                                jraft::Storage::LogEntry *entry = log.mutable_log_entry();
                                entry->set_action(rpcReq->log_entrys(i).action());
                                entry->set_key(rpcReq->log_entrys(i).key());
                                entry->set_value(rpcReq->log_entrys(i).value());
                                batchVect.push_back(log);
//                                storage->setRaftLog(log, rpcReq->log_entrys(i).index());
                                if (rpcReq->log_entrys(i).index() > raftConfig.max_log_index()) {
                                    raftConfig.set_max_log_index(rpcReq->log_entrys(i).index());
                                }
//                                LOG_COUT << g_raftStatusNameMap[raftStatus]
//                                         << " append log !"
//                                         <<" max_log_index=" << raftConfig.max_log_index() << LOG_ENDL;
                            }

                            if (rpcReq->leader_commit() < raftConfig.commit_index()) {
                                LOG_COUT << "PS:leader_commit < follower_commit_index "
                                << rpcReq->leader_commit() << "<" << raftConfig.commit_index() << LOG_ENDL;
                                /*todo:这里不能assert, 因为原leader commit_index(11), follower都接收了日志max_log_index(11), 但是commit_index(10)
                                          重新选举, 原来的follower成为leader, 然后会进入这个case*/
//                                assert(0);
                            }
                            if (rpcReq->leader_commit() > raftConfig.commit_index()) {
                                /*如果 leaderCommit > commitIndex，令 commitIndex 等于 leaderCommit 和 新日志条目索引值中较小的一个
                                 */
                                int new_commit_index = raftConfig.max_log_index() > rpcReq->leader_commit() ? rpcReq->leader_commit()
                                                                                     : raftConfig.max_log_index();
                                if (new_commit_index < raftConfig.commit_index()) {
                                    LOG_COUT << "new_commit_index < raftConfig.commit_index() "
                                             << new_commit_index << "<" << raftConfig.commit_index() << LOG_ENDL;
                                    assert(0);
                                }
                                LOG_COUT << " update commit_index  "
                                         << raftConfig.commit_index() << "-->" << new_commit_index
                                         << " size=" << new_commit_index-raftConfig.commit_index() <<LOG_ENDL;
                                raftConfig.set_commit_index(new_commit_index);
                            }
                            storage->setRaftLog(batchVect, raftConfig);

                            result = true;
                            this->leader_id = rpcReq->leader_id();
                        }
                    }

                    if (rpcReq->term() > raftConfig.current_term()) {
                        raftConfig.set_current_term(rpcReq->term());
                        storage->setRaftConfig(raftConfig);
                    }

                    jraft::Network::Msg msg;
                    msg.set_group_id(groupCfg->getGroupId());
                    msg.set_msg_type(jraft::Network::MsgType::MSG_Type_Rpc_Response);
                    jraft::Network::RpcRes *rpcRes = msg.mutable_rpc_response();
                    rpcRes->set_term(raftConfig.current_term());
                    rpcRes->set_success(result);

                    if (result) {
                        if (rpcReq->log_entrys_size()) {
                            rpcRes->set_match_index(rpcReq->prev_log_index()+rpcReq->log_entrys_size());
                        } else {
                            rpcRes->set_match_index(rpcReq->prev_log_index());
                        }
                    } else {
                        rpcRes->set_match_index(rpcReq->prev_log_index()-1);
                    }
                    if (rpcRes->match_index() > raftConfig.max_log_index()) {
                        //直接设置max_log_index
                        LOG_COUT << "rpcRes->match_index() > raftConfig.max_log_index() " << rpcRes->match_index() << ">" << raftConfig.max_log_index() << LOG_ENDL;
                        rpcRes->set_match_index(raftConfig.max_log_index());
                    }
                    network->sendMsg(*addressMsgPair->first.get(), msg);
                    if (reset_timer) {
                        timer.resetTime(Utils::randint(1000*4, 1000*6));
                    }
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
                    cliRes->set_last_log_index(raftConfig.max_log_index());

                    jraft::Network::CliReq *cliReq = recvMsg->mutable_cli_request();
                    switch (cliReq->request_type()) {
                        case 2:
                        {
                            if (cliReq->log_index() < 1 || cliReq->log_index() > raftConfig.max_log_index()) {
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
                        default:
                            cliRes->set_result(4);
                            cliRes->set_err_msg("request_type err");
                            break;
                    }

                    network->sendMsg(*addressMsgPair->first.get(), msg);
                    break;
                }
                default:
                    LOG_COUT << "groupId=" << groupCfg->getGroupId() << " " << g_raftStatusNameMap[raftStatus]
                             << " recv MsgType err msgType=" << recvMsg->msg_type() << LOG_ENDL;
                    break;
            }
        }
        /*time out*/
        LOG_COUT << "groupId=" << groupCfg->getGroupId() << " wait time out!" << LOG_ENDL;
        changeRaftStat(RAFT_STAT_CANDIDATER);
        return 0;
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
            voteReq->set_last_log_index(raftConfig.max_log_index());
            voteReq->set_last_log_term(getLastLogTerm());

            network->sendMsg(groupCfg->getNodes()[i], msg);
        }

        Timer timer(Utils::randint(1000*4, 1000*6));
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
                        return 0;
                    }

                    if (voteResponse->granted()
                        && voteResponse->term() == raftConfig.current_term()) {
                        voteCount += 1;
                        if (voteCount >= ((groupCfg->getNodes().size() + 1)/2+1)) {
                            LOG_COUT << "groupId=" << groupCfg->getGroupId() <<  " I am Leader !!!" << LOG_ENDL;
                            changeRaftStat(RAFT_STAT_LEADER);
                            vector<string> nodeIds;
                            for (int i = 0; i < groupCfg->getNodes().size(); ++i) {
                                nodeIds.push_back(pair2NodeId(
                                        const_cast<pair<string, int> *>(&groupCfg->getNodes()[i])));
                            }
                            //nextIndex[] 	对于每一个服务器，需要发送给他的下一个日志条目的索引值（初始化为领导人最后索引值加一）
                            nodesLogInfo = make_shared<NodesLogInfo>(nodeIds, raftConfig.max_log_index() + 1);
                            return 0;
                        }
                    }
                    break;
                }
                case jraft::Network::MsgType::MSG_Type_Vote_Request:
                {
                    bool result = false;
                    jraft::Network::VoteReq *voteReqMsg = recvMsg->mutable_vote_request();
                    /*
                     * 接收者实现：
                        如果term < currentTerm返回 false （5.2 节）
                        如果 votedFor 为空或者为 candidateId，并且候选人的日志至少和自己一样新，那么就投票给他（5.2 节，5.4 节）
                        Raft 通过比较两份日志中最后一条日志条目的索引值和任期号定义谁的日志比较新。
                        如果两份日志最后的条目的任期号不同，那么任期号大的日志更加新。
                        如果两份日志最后的条目任期号相同，那么日志比较长的那个就更加新
                    */
                    //每一轮只能投给一个选举人, voteReqMsg->term() == raftConfig.current_term()的情况不投(和论文有差异), 因为到这里会先投给自己, 所以一定不可能投改其他人
                    //有一个场景:原先自己current_term=1, 先投给了term(2), set_current_term=2,  然后马上又有一个term(3), 也符合条件, 也投了, 这里应该是安全的, 因为term(3)返回去会迫使term(2)跟随
                    if (voteReqMsg->term() <= raftConfig.current_term()) {
                        result = false;
                    } else {
                        raftConfig.set_current_term(voteReqMsg->term());
                        raftConfig.set_votefor(VOTEFOR_NULL);
                        if (voteReqMsg->last_log_term() != getLastLogTerm()) {
                            if (voteReqMsg->last_log_term() > getLastLogTerm()) {
                                result = true;
                            } else {
                                result = false;
                            }
                        } else {
                            //last term相同
                            if (voteReqMsg->last_log_index() >= raftConfig.max_log_index()) {
                                result = true;
                            } else {
                                result = false;
                            }
                        }
                        if (result == true) {
                            raftConfig.set_votefor(voteReqMsg->candidate_id());
                            result = true;
                        }
                        storage->setRaftConfig(raftConfig);
                    }

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
                    cliRes->set_last_log_index(raftConfig.max_log_index());

                    jraft::Network::CliReq *cliReq = recvMsg->mutable_cli_request();
                    switch (cliReq->request_type()) {
                        case 2:
                        {
                            if (cliReq->log_index() < 1 || cliReq->log_index() > raftConfig.max_log_index()) {
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
                        default:
                            cliRes->set_result(4);
                            cliRes->set_err_msg("request_type err");
                            break;
                    }

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
                            raftConfig.set_votefor(VOTEFOR_NULL);
                            if (voteReqMsg->last_log_term() != getLastLogTerm()) {
                                if (voteReqMsg->last_log_term() > getLastLogTerm()) {
                                    result = true;
                                } else {
                                    result = false;
                                }
                            } else {
                                //last term相同
                                if (voteReqMsg->last_log_index() >= raftConfig.max_log_index()) {
                                    result = true;
                                } else {
                                    result = false;
                                }
                            }
                            if (result == true) {
                                raftConfig.set_votefor(voteReqMsg->candidate_id());
                                result = true;
                            }
                            storage->setRaftConfig(raftConfig);
                            need2Follower = true;
                        }

                        jraft::Network::Msg msg;
                        msg.set_group_id(groupCfg->getGroupId());
                        msg.set_msg_type(jraft::Network::MsgType::MSG_Type_Vote_Response);
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

                        if (rpcRes->success()) {
                            //返回成功才可以更新matchIndex
                            //nextIndex不更新
                            nodesLogInfo->setMatchIndex(nodeId, rpcRes->match_index(), this->pair2NodeId(*selfNode),
                                                        raftConfig.max_log_index());
                            if (nodesLogInfo->getMaxCommitedId() > raftConfig.commit_index()) {
                                LOG_COUT << "update commit_index " << raftConfig.commit_index() << "-->" << nodesLogInfo->getMaxCommitedId()
                                << " size=" << nodesLogInfo->getMaxCommitedId()-raftConfig.commit_index() << LOG_ENDL;
                                raftConfig.set_commit_index(nodesLogInfo->getMaxCommitedId());
                                //todo:apply commited log

                                while (!readyLogQue.empty()) {
                                    LogData *logData = readyLogQue.front();
                                    if (logData == NULL) {
                                        break;
                                    }
                                    if (logData->log.log_index() <= raftConfig.commit_index()) {
                                        //通知业务完成
                                        while (logData->writeState == 0) {
                                            LOG_COUT << "logData has no Write!!! logid=" << logData->log.log_index() << LOG_ENDL;
                                            co_poll(co_get_epoll_ct(), NULL, 0, 20);
                                        }
                                        logData->state = 2;
                                        CoroutineSignalOverThread::getInstance()->addSig(logData->cond, logData->tid);
                                        readyLogQue.pop();
                                        //:删除storage的缓存
                                        storage->deleleRaftLogNoWriteCache(logData->log.log_index());
                                    } else {
//                                        LOG_COUT << "no biger!!!  logData->log.log_index()  : raftConfig.commit_index() "
//                                        << logData->log.log_index() << ":" <<  raftConfig.commit_index() << LOG_ENDL;
                                        break;
                                    }
                                }
                            }
                            storage->setRaftConfig(raftConfig);
                        } else {
                            //false
                            //pipeline? 连续发,失败才更新
                            nodesLogInfo->setNextIndex(nodeId, rpcRes->match_index()+1);
                        }
                        if (nodesLogInfo->getNextIndex(nodeId) != raftConfig.max_log_index()+1) {
                            //加速日志复制
                            LOG_COUT << " 加速日志复制  " << nodeId  <<  " " << nodesLogInfo->getNextIndex(nodeId) << " " << raftConfig.max_log_index() << LOG_ENDL;
                            if (timer.getRemainTime() > 10) {
                                timer.resetTime(10);
                            }
//                            co_cond_signal(preWriteCond);
                        }
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
                            case 1: //set index key value
                            {
#if 1
                                cliReq->set_log_index(raftConfig.max_log_index()+1);
#endif
                                if (cliReq->log_index() != raftConfig.max_log_index() + 1) {
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
                                configLog.set_log_index(raftConfig.max_log_index() + 1);
                                configLog.set_term(raftConfig.current_term());
                                jraft::Storage::LogEntry *entry = configLog.mutable_log_entry();
                                entry->set_action(cliReq->mutable_log_entry()->action());
                                entry->set_key(cliReq->mutable_log_entry()->key());
                                entry->set_value(cliReq->mutable_log_entry()->value());
                                raftConfig.set_max_log_index(raftConfig.max_log_index() + 1);
                                storage->setRaftConfig(raftConfig);
                                storage->setRaftLog(configLog, raftConfig.max_log_index());
                                //立即触发发送日志
//                                co_cond_signal(preWriteCond);
                                if (timer.getRemainTime() > 10) {
                                    timer.resetTime(10);
                                }
                                break;
                            }
                            case 2:
                            {
                                if (cliReq->log_index() < 1 || cliReq->log_index() > raftConfig.max_log_index()) {
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
                        cliRes->set_last_log_index(raftConfig.max_log_index());
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
        //由leaderSendLogProccess发送
        co_cond_signal(preWriteCond);
        continue;
#if 0
        /*add group config to log*/
        if (raftConfig.max_log_index() == 0) {
            jraft::Storage::Log configLog;
            configLog.set_log_index(1);
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
            raftConfig.set_max_log_index(raftConfig.max_log_index() + 1);
            storage->setRaftConfig(raftConfig);
            storage->setRaftLog(configLog, raftConfig.max_log_index());
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
            const shared_ptr<jraft::Storage::Log> &raftLog = storage->getRaftLog(nodeNextId - 1);
            if (raftLog == nullptr) {
                rpcReq->set_prev_log_term(0);
            } else {
                rpcReq->set_prev_log_term(raftLog->term());
            }
            
            rpcReq->set_leader_commit(raftConfig.commit_index());

            if (nodeNextId <= raftConfig.max_log_index()) {
                if (raftConfig.max_log_index() - nodeNextId > 10) {
                    LOG_COUT << "less log n=" << raftConfig.max_log_index() - nodeNextId << LOG_ENDL;
                }
                string nodeIdStr = pair2NodeId(nodeId);
                for (int j = nodeNextId; j <= nodeNextId + 80 && j <= raftConfig.max_log_index(); ++j) {
                    shared_ptr<jraft::Storage::Log> log = storage->getRaftLog(j);
                    if (log != NULL) {
                        Pb2Json::Json json;
                        Pb2Json::Message2Json(*log.get(), json);
//                        LOG_COUT << "index=" << nodeNextId << " log=" << json.dump() << LOG_ENDL;
                        jraft::Network::LogEntry *logEntry = rpcReq->add_log_entrys();
                        logEntry->set_action(log->mutable_log_entry()->action());
                        logEntry->set_key(log->mutable_log_entry()->key());
                        logEntry->set_value(log->mutable_log_entry()->value());
                        logEntry->set_term(log->term());
                        logEntry->set_index(log->log_index());
                        //pipeline? 直接更新NextIndex
                        nodesLogInfo->setNextIndex(nodeIdStr, log->log_index()+1);
                    }
                }

            }
            network->sendMsg(groupCfg->getNodes()[i], msg);
        }
#endif
    }
    return 0;
}




void RaftMachine::start() {
    //主流程
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

int RaftMachine::getLastLogTerm() {
    int max_log_index = raftConfig.max_log_index();
    const shared_ptr<jraft::Storage::Log> &raftLog = storage->getRaftLog(max_log_index);
    if (raftLog == nullptr) {
        return 0;
    }
    return raftLog->term();
}

int RaftMachine::preWriteLog(vector<LogData *> &logList, int threadIndex) {
    if (raftStatus != RAFT_STAT_LEADER) {
        return -1;
    }

    if (preWriteBuffArray[threadIndex].IsFull()) {
        return -2;
    }

    for (int i = 0; i < logList.size(); ++i) {
        long retIndex = preWriteBuffArray[threadIndex].addBuffNoLock(logList[i]);
        if (retIndex < 0) {
            assert(0);
        }
    }
    return 0;
}

int RaftMachine::leaderSendLogCoroutine() {
    while (1) {
        int waitTime_ms = 1000*3;
        Timer timer(waitTime_ms);
        while (timer.hasRemainTime()) {
            int timeout_ms = timer.getRemainTime();
            if (timeout_ms < 0) {
                LOG_COUT << "Timer bug here" << LOG_ENDL;
                break;
            }
            co_cond_timedwait(preWriteCond, timeout_ms);
            if (timer.getRemainTime() > 10) {
                timer.resetTime(10);
            }
        }

        if (raftStatus != RAFT_STAT_LEADER) {
            continue;
        }
        LOG_COUT << "start send log ..." << LOG_ENDL;
        /*add group config to log*/
        if (raftConfig.max_log_index() == 0) {
            jraft::Storage::Log configLog;
            configLog.set_log_index(1);
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
            raftConfig.set_max_log_index(raftConfig.max_log_index() + 1);
            storage->setRaftConfig(raftConfig);
            storage->setRaftLog(configLog, raftConfig.max_log_index());
        }

        //todo:检查preWriteLog
        int old_log_id = raftConfig.max_log_index();
        for (int k = 0; k < businessThreads; ++k) {
            int index = 0;
            for (; index < 1000; index++) {
                if (!preWriteBuffArray[k].IsIn(preWriteBuffArray[k].getStartIndex())) {
                    break;
                }
                LogData *logData =preWriteBuffArray[k].getData(preWriteBuffArray[k].getStartIndex());
                if (logData == NULL) {
                    break;
                }

                if (1) {
                    logData->log.set_log_index(raftConfig.max_log_index()+1);
                    logData->log.set_term(raftConfig.current_term());
                    raftConfig.set_max_log_index(raftConfig.max_log_index()+1);
                    preWriteBuffArray[k].popOne();
//                LOG_COUT << "readyLogQue.push logid=" << logData->log.log_index() << LOG_ENDL;
                    readyLogQue.push(logData);
                    if (storage->getStorageType() == "mem") {
                        //mem直接写
                        storage->setRaftLog(logData->log, logData->log.log_index());
                        logData->writeState = 1;
                    } else {
                        writeLogThreadBuff_.addBuffNoLock(logData);
                        storage->setRaftLogNoWrite(logData->log, logData->log.log_index());
                    }
                } else {
                    LOG_COUT << "ERR!!  logData->log.log_index() != raftConfig.max_log_index()+1 "
                             << logData->log.log_index() <<" "<< raftConfig.max_log_index()<< LOG_ENDL;
                    break;
                }
            }
        }

        if (raftConfig.max_log_index() != old_log_id) {
            LOG_COUT << "log_index " << old_log_id << "-->" << raftConfig.max_log_index()
            << " size=" << raftConfig.max_log_index()-old_log_id << LOG_ENDL;
            //todo:这里直接更新max_log_index不妥, 因为log还没有写
            storage->setRaftConfig(raftConfig);
            //通知写log线程
            pthread_cond_signal(&writeLogThreadCond);
        }

        int maxNextId = 0;
        do {
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
                const shared_ptr<jraft::Storage::Log> &raftLog = storage->getRaftLog(nodeNextId - 1);
                if (raftLog == nullptr) {
                    rpcReq->set_prev_log_term(0);
                } else {
                    rpcReq->set_prev_log_term(raftLog->term());
                }

                rpcReq->set_leader_commit(raftConfig.commit_index());

                if (nodeNextId <= raftConfig.max_log_index()) {
                    string nodeIdStr = pair2NodeId(nodeId);
                    if (raftConfig.max_log_index() - nodeNextId > 10) {
                        LOG_COUT << "less log n=" << raftConfig.max_log_index() - nodeNextId
                                 << " nodeIdStr=" << nodeIdStr <<LOG_ENDL;
                    }

                    for (int j = nodeNextId; j <= nodeNextId + 80 && j <= raftConfig.max_log_index(); ++j) {
                        shared_ptr<jraft::Storage::Log> log = storage->getRaftLog(j);
                        if (log != NULL) {
//                        Pb2Json::Json json;
//                        Pb2Json::Message2Json(*log.get(), json);
//                        LOG_COUT << "index=" << nodeNextId << " log=" << json.dump() << LOG_ENDL;
                            jraft::Network::LogEntry *logEntry = rpcReq->add_log_entrys();
                            logEntry->set_action(log->mutable_log_entry()->action());
                            logEntry->set_key(log->mutable_log_entry()->key());
                            logEntry->set_value(log->mutable_log_entry()->value());
                            logEntry->set_term(log->term());
                            logEntry->set_index(log->log_index());
                            //pipeline? 直接更新NextIndex
                            nodesLogInfo->setNextIndex(nodeIdStr, log->log_index()+1);
                        } else {
                            LOG_COUT << "get log err !!! index=" << j << LOG_ENDL;
                            break;
                        }
                    }

                }
                maxNextId = max<int>(maxNextId, nodesLogInfo->getNextIndex(pair2NodeId(nodeId)));
                network->sendMsg(groupCfg->getNodes()[i], msg);
            }
        } while (maxNextId < raftConfig.max_log_index());
    }
    return 0;
}

int RaftMachine::notifyLeaderSendLog() {
    notify_events++;
    return 0;
}

int RaftMachine::eventLoop() {
    if (notify_events > 0) {
        LOG_COUT << "notify_events=" << notify_events << LOG_ENDL;
        notify_events = notify_events/2;
        co_cond_signal(preWriteCond);
    }
    return 0;
}

int RaftMachine::leaderWriteLogThread() {
    timespec stTime;
    int needLock = 1;
    while (1) {
        if (needLock == 1) {
            pthread_mutex_lock(&writeLogThreadLock);
            needLock = 0;
        }
        stTime.tv_sec = 0;
        stTime.tv_nsec = 20 * 1000; //50 ms
        int ret = pthread_cond_timedwait(&writeLogThreadCond, &writeLogThreadLock, &stTime);
        if (ret == ETIMEDOUT) {
            if (writeLogThreadBuff_.getEndIndex()-writeLogThreadBuff_.getStartIndex() <= 0) {
                continue;
            }
        } else {
            if (writeLogThreadBuff_.getEndIndex()-writeLogThreadBuff_.getStartIndex() < 3) {
                continue;
            }
        }

        if (writeLogThreadBuff_.getEndIndex()-writeLogThreadBuff_.getStartIndex() <= 0) {
            continue;
        }
        vector<jraft::Storage::Log> logVect;
        vector<LogData *> logDataVect;
        while (1) {
            LogData *logData = writeLogThreadBuff_.popOne();
            if (logData == NULL) {
                break;
            }
            logVect.push_back(logData->log);
            logDataVect.push_back(logData);
        }
        if (logVect.size() > 0) {
            int ret = storage->setRaftLog(logVect);
            if (ret != 0) {
                LOG_COUT << "write log err!!! ret=" << ret << " pid=" << getpid() << LOG_ENDL;
                assert(0);
            }
        }
        for (int i = 0; i < logDataVect.size(); ++i) {
            logDataVect[i]->writeState = 1;
        }
    }
    return 0;
}

