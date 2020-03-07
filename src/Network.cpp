//
// Created by dell-pc on 2018/5/6.
//
#include <iostream>
#include <sys/socket.h>
#include <asm/byteorder.h>
#ifdef __CYGWIN__
#include <cygwin/in.h>
#else
#endif
#include <unistd.h>
#include <fcntl.h>
#include "Network.h"
#include "Log.h"
#include "pb2json.h"

using namespace std;

int SetNonBlock(int iSock)
{
    int iFlags;

    iFlags = fcntl(iSock, F_GETFL, 0);
    iFlags |= O_NONBLOCK;
    iFlags |= O_NDELAY;
    int ret = fcntl(iSock, F_SETFL, iFlags);
    return ret;
}


int CreateUdpSecket(char *host, int port, int reuse)
{
    co_enable_hook_sys();
    int sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (sockfd < 0) {
        LOG_COUT << "create socket err ret=" << sockfd << LOG_ENDL_ERR;
        return sockfd;
    }
    if (reuse) {
        int nResuseAddr = 1;
        int ret = setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &nResuseAddr, sizeof(nResuseAddr));
    }
    struct sockaddr_in addr;
    bzero(&addr, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons((uint16_t)port);
    int ret = bind(sockfd, (const sockaddr *)&addr, sizeof(addr));
    if (ret < 0) {
        LOG_COUT << "bind ret=" << ret << LOG_ENDL_ERR;
        close(sockfd);
        return ret;
    }
    unsigned int nLen = 8*1024*1024;
    setsockopt(sockfd, SOL_SOCKET, SO_RCVBUF, &nLen, sizeof(nLen));
    setsockopt(sockfd, SOL_SOCKET, SO_SNDBUF, &nLen, sizeof(nLen));
    return sockfd;
}


void Network::sendMsg(const pair<string, int> &pair, jraft::Network::Msg &msg) {
    struct sockaddr_in address;
    memset(&address, 0, sizeof(struct sockaddr_in));
    address.sin_family=AF_INET;
    inet_pton(AF_INET, pair.first.c_str(), &address.sin_addr);
    address.sin_port=htons(static_cast<uint16_t>(pair.second));

//    shared_ptr<unsigned char > buff = make_shared<unsigned char>(msg.ByteSize());
//    if (!msg.SerializePartialToArray(buff.get(), msg.ByteSize())) {
//        LOG_COUT << "msg serial err" << LOG_ENDL_ERR;
//        return;
//    }
//

    string str;
    bool ret1 = msg.SerializeToString(&str);
    if (!ret1) {
        LOG_COUT << "SerializeToString err ret=" << ret1 << LOG_ENDL_ERR;
        return ;
    }
    char buff[sizeof(MsgHead)+str.length()];
    ((MsgHead*)buff)->datalen = str.length();
    memcpy(buff+ sizeof(MsgHead), str.c_str(), str.length());
    ssize_t ret = sendto(selfnodeFd, buff, sizeof(MsgHead)+str.length(), 0,
                         (struct sockaddr *)&address,
                         sizeof(struct sockaddr_in));
    if (ret < 0) {
        LOG_COUT << "sendto err" << LOG_ENDL_ERR;
    }
}

shared_ptr<pair<shared_ptr<pair<string, int>>, shared_ptr<jraft::Network::Msg>>> Network::waitMsgTimeOut(int m_sec) {
    shared_ptr<pair<shared_ptr<pair<string, int>>, shared_ptr<jraft::Network::Msg>>> msg;
    if (queueMsg.empty()) {
        co_cond_timedwait(this->getCond(), m_sec);
    }
    if (!queueMsg.empty()) {
        pair<shared_ptr<sockaddr_in>, shared_ptr<jraft::Network::Msg>> &pMsg = queueMsg.front();
        msg = make_shared<pair<shared_ptr<pair<string, int>>, shared_ptr<jraft::Network::Msg>>>(
                address2pair(pMsg.first), pMsg.second);
        queueMsg.pop();

    }
    return msg;
}

shared_ptr<pair<string, int>> Network::address2pair(shared_ptr<sockaddr_in> address) {
    char str[17];
    inet_ntop(AF_INET, &address->sin_addr, str, sizeof(str));
    int port = ntohs(address->sin_port);
    shared_ptr<pair<string, int>> addressPair = make_shared<pair<string, int>>(str, port);
    return addressPair;
}

shared_ptr<sockaddr_in> Network::host2address(string &host, int port) {
    struct sockaddr_in *addr = static_cast<sockaddr_in *>(malloc(sizeof(struct sockaddr_in)));
    shared_ptr<struct sockaddr_in> address(addr);
    address->sin_family=AF_INET;
    inet_pton(AF_INET, host.c_str(), &address->sin_addr);
    address->sin_port=htons(port);
    return address;
}

void SetAddr(const char *pszIP,const unsigned short shPort,struct sockaddr_in &addr)
{
    bzero(&addr,sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(shPort);
    int nIP = 0;
    if( !pszIP || '\0' == *pszIP
        || 0 == strcmp(pszIP,"0") || 0 == strcmp(pszIP,"0.0.0.0")
        || 0 == strcmp(pszIP,"*")
            )
    {
        nIP = htonl(INADDR_ANY);
    }
    else
    {
        nIP = inet_addr(pszIP);
    }
    addr.sin_addr.s_addr = nIP;
}

int CreateTcpSocket(const unsigned short shPort, const char *pszIP, bool bReuse) {
    int fd = socket(AF_INET,SOCK_STREAM, IPPROTO_TCP);
    if( fd >= 0 )
    {
        if(shPort != 0)
        {
            if(bReuse)
            {
                int nReuseAddr = 1;
                setsockopt(fd,SOL_SOCKET,SO_REUSEADDR,&nReuseAddr,sizeof(nReuseAddr));
            }
            struct sockaddr_in addr ;
            SetAddr(pszIP,shPort,addr);
            int ret = bind(fd,(struct sockaddr*)&addr,sizeof(addr));
            if( ret != 0)
            {
                close(fd);
                return -1;
            }
        }
    }
    listen( fd,10240 );
    if(fd==-1){
        printf("Port %d is in use\n", shPort);
        close(fd);
        return -1;
    }
    return fd;
}
