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
