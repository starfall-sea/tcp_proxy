#include <stdio.h>
#include <time.h>
#include <string.h>
#include "logger.h"

static int level = LOG_INFO;    // 当前日志级别
static int LOG_BUFFER_SIZE = 2048;
// 用于输出匹配syslog中的日志级别的字符串
static const char* loglevels[] =
{
    "emerge!", "alert!", "critical!", "error!", "warn!", "notice:", "info:", "debug:"
};

void set_loglevel( int log_level )
{
    level = log_level;
}

void log( int log_level,  const char* file_name, int line_num, const char* format, ... )
{
    if ( log_level > level )
    {
        return;
    }

    // 获取当前时间
    time_t tmp = time( NULL );
    struct tm* cur_time = localtime( &tmp );
    if ( ! cur_time )
    {
        return;
    }

    char arg_buffer[ LOG_BUFFER_SIZE ];
    memset( arg_buffer, '\0', LOG_BUFFER_SIZE );
    // 格式化时间[ 日期 时间 ]
    strftime( arg_buffer, LOG_BUFFER_SIZE - 1, "[ %x %X ] ", cur_time );
    printf( "%s", arg_buffer );
    // 输出文件名和行号
    printf( "%s:%04d ", file_name, line_num );
    // 输出日志级别
    printf( "%s ", loglevels[ log_level - LOG_EMERG ] );

    // 输出用户内容（可变参数）
    va_list arg_list;
    va_start( arg_list, format );
    memset( arg_buffer, '\0', LOG_BUFFER_SIZE );
    vsnprintf( arg_buffer, LOG_BUFFER_SIZE - 1, format, arg_list );
    printf( "%s\n", arg_buffer );
    fflush( stdout );   // 立即刷新（牺牲性能来保证日志完整性和实时性，注重调试）
    va_end( arg_list );
}
