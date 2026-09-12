#ifndef SRVMGR_H
#define SRVMGR_H

#include <map>
#include <arpa/inet.h>
#include "fd_wrap.h"
#include "connection.h"
#include "shared.h"

using std::map;

// 后端服务器信息
class host
{
public:
    char m_hostname[1024];  // IP或域名
    int m_port;             // 端口
    int m_conncnt;          // 连接池大小
};

class mgr
{
public:
    // mgr( int epollfd, const host& srv );    // 构造时建立连接池
    mgr( int epollfd, const host& srv, SharedData* shared, int idx );   // 接收共享内存指针
    ~mgr();

    
    int conn2srv( const sockaddr_in& address ); // 连接到后端服务器
    conn* pick_conn( int sockfd );              // 从连接池取一个连接给客户端
    void free_conn( conn* connection );         // 释放连接（放回待回收池）
    int get_used_conn_cnt();                    // 获取当前活跃连接数
    void recycle_conns();                       // 回收并重建失效连接
    RET_CODE process( int fd, OP_TYPE type );   // 处理IO事件

private:
    int m_idx;                  // worker 在数组中的索引
    static int m_epollfd;       // 所有实例共享epollfd
    map< int, conn* > m_conns;  // 空闲连接池（key：srvfd）
    map< int, conn* > m_used;   // 使用中的连接（key：cltfd或srvfd）
    map< int, conn* > m_freed;  // 待回收连接（需要重连）
    host m_logic_srv;           // 后端服务器信息
    SharedData* m_shared;       // 共享内存指针           
};

#endif
