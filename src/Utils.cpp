//
// Created by dell-pc on 2018/5/11.
//

#include <dlfcn.h>
#include <unistd.h>
#include "Utils.h"


int Utils::getThreadId() {
    //static int (*g_getThreadId)(void) = (int (*)(void)) dlsym(RTLD_DEFAULT, "GetCurrentThreadId");
//    int pid = g_getThreadId();
    return 0;
}

int Utils::getPid() {
    int pid;
#if defined(__CYGWIN__)
    static int (*GetCurrentProcessId)(void) = (int (*)(void)) dlsym(RTLD_DEFAULT, "GetCurrentProcessId");
    pid = GetCurrentProcessId();
#else
    pid = getpid();
#endif
    return pid;
}

int CoroutineSignalOverThread::loopSelfTid() {
    long tid = GetThreadId();
    if (sigDataArray[tid].condList.size() <= 0) {
        return 0;
    }
    pthread_mutex_lock(&sigDataArray[tid].lock);
    for (int i = 0; i < sigDataArray[tid].condList.size(); ++i) {
//        printf("loopSelfTid co_cond_signal tid=%ld\n", tid);
        co_cond_signal(sigDataArray[tid].condList[i]);
    }
    sigDataArray[tid].condList.clear();
    pthread_mutex_unlock(&sigDataArray[tid].lock);
    return 0;
}

int CoroutineSignalOverThread::addSig(stCoCond_t *sig, long tid) {
    pthread_mutex_lock(&sigDataArray[tid].lock);
    sigDataArray[tid].condList.push_back(sig);
    pthread_mutex_unlock(&sigDataArray[tid].lock);
    return 0;
}

long GetThreadId() {
    long tid = syscall(__NR_gettid );
    return tid;
}
