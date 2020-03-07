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
    if (sigRingBuff[tid] == NULL) {
        pthread_mutex_lock(&lock);
        if (sigRingBuff[tid] == NULL) {
            sigRingBuff[tid] = new RingBuff;
        }
        pthread_mutex_unlock(&lock);
    }
    while (1) {
        stCoCond_t *cond = (stCoCond_t *)sigRingBuff[tid]->popOne();
        if (cond == NULL) {
            break;
        }
        co_cond_signal(cond);
    }
    return 0;
}

int CoroutineSignalOverThread::addSig(stCoCond_t *sig, long tid) {
    if (sigRingBuff[tid] == NULL) {
        pthread_mutex_lock(&lock);
        if (sigRingBuff[tid] == NULL) {
            sigRingBuff[tid] = new RingBuff;
        }
        pthread_mutex_unlock(&lock);
    }
    sigRingBuff[tid]->addBuffNoLock((LogData*)sig);
    return 0;
}

long GetThreadId() {
    long tid = syscall(__NR_gettid );
    return tid;
}
