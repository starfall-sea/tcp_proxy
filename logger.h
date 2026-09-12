#ifndef LOG_H
#define LOG_H

#include <syslog.h> // 提供 LOG_EMERG, LOG_ALERT等宏
#include <cstdarg>  // va_list可变参数

void set_loglevel( int log_level = LOG_DEBUG );
void log( int log_level, const char* file_name, int line_num, const char* format, ... );

#endif
