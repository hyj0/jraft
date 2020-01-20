//
// Created by dell-pc on 2018/5/11.
//

#ifndef PROJECT_UTILS_H
#define PROJECT_UTILS_H

#include <cstdlib>
#include <random>
#include <sys/time.h>

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


#endif //PROJECT_UTILS_H
