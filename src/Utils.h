//
// Created by dell-pc on 2018/5/11.
//

#ifndef PROJECT_UTILS_H
#define PROJECT_UTILS_H

#include <cstdlib>
#include <random>
#include <sys/time.h>
#include <string>
#include <co_routine.h>
#include <storage.pb.h>
#include <unistd.h>
#include <sys/syscall.h>

using namespace std;

class Utils {
public:
    static int randint(int min, int max) {
        if (max == min) {
            return max;
        }
        int ret = rand() % (max - min);
        ret = ret + min;
        return ret;
    }

    static int getThreadId();

    static int getPid();
};

class Timer {
private:
    time_t endTime_ms;
    int delay_ms;
public:
    Timer(int delay_ms) : delay_ms(delay_ms) {
        timeval tv;
        gettimeofday(&tv, NULL);
        endTime_ms = tv.tv_sec*1000+tv.tv_usec/1000 + delay_ms;
    }

    int getRemainTime() {
        timeval tv;
        gettimeofday(&tv, NULL);
        time_t remain_ms = endTime_ms - (tv.tv_sec*1000 + tv.tv_usec/1000);
        return static_cast<int>(remain_ms);
    }

    bool hasRemainTime() {
        return getRemainTime() > 0;
    }

    int resetTime(int delay_ms) {
        timeval tv;
        gettimeofday(&tv, NULL);
        endTime_ms = tv.tv_sec*1000+tv.tv_usec/1000 + delay_ms;
    }
};


class LogData {
public:
    int state; //0--init 1--写入存储完成, 2--apply完成
    long tid;//
    string groupId;
    //data
    jraft::Storage::Log log;
    struct stCoCond_t *cond; //完成后通知业务
};

#define RING_BUFF_SIZE  (10000*10)

class RingBuff {
    unsigned long startIndex;
    unsigned long endIndex;
    LogData *kvData[RING_BUFF_SIZE];
    stCoCond_t *cond; //no use
    pthread_mutex_t lock;
public:
    RingBuff() {
        startIndex = 0;
        endIndex = 0;
        cond = co_cond_alloc();
        lock = PTHREAD_MUTEX_INITIALIZER;
    }

    unsigned long Index(unsigned long index) {
        return index % RING_BUFF_SIZE;
    }

    unsigned long getNextIndex(unsigned long index) {
        index = Index(index);
        if (index == RING_BUFF_SIZE-1) {
            index = 0;
        } else {
            index = index + 1;
        }
        return index;
    }

    bool IsFull() {
        if (Index(endIndex) == Index(startIndex)) {
            return true;
        } else {
            return false;
        }
    }

    long addBuff(LogData *inKvData) {
        pthread_mutex_lock(&lock);
//        if (IsFull()) {
//            pthread_mutex_unlock(&lock);
//            return -1;
//        }
        kvData[Index(endIndex)] = inKvData;
        long ret = endIndex;
        endIndex += 1;
        pthread_mutex_unlock(&lock);
        return ret;
    }

    bool IsIn(unsigned long index) {
        return startIndex <= index && index < endIndex;
    }

    LogData *popOne() {
        LogData *ret = NULL;
        if (startIndex < endIndex) {
            ret = kvData[Index(startIndex)];
            startIndex += 1;
        }
        return ret;
    }

    LogData *getData(long index) {
        return kvData[Index(index)];
    }

    unsigned long getStartIndex() const {
        return startIndex;
    }

    unsigned long getEndIndex() const {
        return endIndex;
    }

    stCoCond_t *getCond() const {
        return cond;
    }

    int retSetIndex(long index) {
        startIndex = index;
        endIndex = index;
    }
};

long GetThreadId();

class SigData {
public:
    vector<stCoCond_t *> condList;
    pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
};

class CoroutineSignalOverThread {
private:
     SigData *sigDataArray;
public:
    CoroutineSignalOverThread() {
        sigDataArray = new SigData[102400];
    }
    int addSig(stCoCond_t *sig, long tid);

    int loopSelfTid();

    static CoroutineSignalOverThread *getInstance() {
        static CoroutineSignalOverThread coroutineSignalOverThread;
        return &coroutineSignalOverThread;
    }
};

#endif //PROJECT_UTILS_H
