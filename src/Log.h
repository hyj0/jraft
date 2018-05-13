//
// Created by dell-pc on 2018/5/6.
//

#ifndef PROJECT_LOG_H
#define PROJECT_LOG_H
#include <cstring>

#define LOG_COUT cout << __FILE__ << ":" << __LINE__ <<  " at " << __FUNCTION__ << " "
#define LOG_ENDL_ERR        " err=" << strerror(errno) << endl
#define LOG_ENDL " " << endl;

class Log {

};


#endif //PROJECT_LOG_H
