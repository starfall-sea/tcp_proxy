#ifndef FDWRAPPER_H
#define FDWRAPPER_H

// 错误码定义
enum RET_CODE { 
    OK = 0,             // 操作成功
    NOTHING = 1,        // 没有数据可以读写
    IOERR = -1,         // IO错误
    CLOSED = -2,        // 连接已关闭
    BUFFER_FULL = -3,   // 缓冲区满了
    BUFFER_EMPTY = -4,  // 缓冲区空了
    TRY_AGAIN           // 暂时无法读写，稍后重试
};

// 操作类型
enum OP_TYPE { 
    READ = 0, 
    WRITE, 
    ERROR 
};

// 函数声明
int setnonblocking( int fd );               // 设置非阻塞，返回原来的模式用于还原
void add_read_fd( int epollfd, int fd );    // 添加读事件
void add_write_fd( int epollfd, int fd );   // 添加写事件
void add_listen_fd( int epollfd, int fd );  // 添加监听事件
void removefd( int epollfd, int fd );       // 移除fd
void closefd( int epollfd, int fd );        // 移除并关闭fd
void modfd( int epollfd, int fd, int ev );  // 修改事件


#endif
