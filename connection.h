#ifndef CONN_H
#define CONN_H

#include <arpa/inet.h>
#include "fd_wrap.h"

class conn
{
public:
    conn();
    ~conn();

    // 初始化客户端连接
    void init_clt( int sockfd, const sockaddr_in& client_addr );
    // 初始化服务器连接
    void init_srv( int sockfd, const sockaddr_in& server_addr );
    // 重置连接（清空缓冲区，准备复用）
    void reset();

    // IO操作
    RET_CODE read_clt();    // 从客户端读数据
    RET_CODE write_clt();   // 向客户端写数据
    RET_CODE read_srv();    // 从服务器读数据
    RET_CODE write_srv();   // 向服务器写数据

public:
    static const int BUF_SIZE = 2048;

    // 客户端相关
    char* m_clt_buf;            // 从客户端读到的数据
    int m_clt_read_idx;         // 已经读到哪个位置
    int m_clt_write_idx;        // 已经发送到哪个位置
    sockaddr_in m_clt_address;  // 客户端地址
    int m_cltfd;                // 客户端socket

    // 服务器相关
    char* m_srv_buf;            // 从服务器读到的数据
    int m_srv_read_idx;         // 已经读到哪个位置
    int m_srv_write_idx;        // 已经发送到哪个位置
    sockaddr_in m_srv_address;  // 服务器地址
    int m_srvfd;                // 服务器socket

    bool m_srv_closed;          // 服务器是否关闭
};

#endif
