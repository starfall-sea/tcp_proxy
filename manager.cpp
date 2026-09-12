#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <assert.h>
#include <stdio.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/epoll.h>
#include <signal.h>
#include <sys/wait.h>
#include <sys/stat.h>

#include <exception>
#include "logger.h"
#include "manager.h"

using std::pair;

// 静态成员初始化
int mgr::m_epollfd = -1;

// 连接到后端服务器
int mgr::conn2srv( const sockaddr_in& address )
{
    int sockfd = socket( PF_INET, SOCK_STREAM, 0 );
    if( sockfd < 0 )
    {
        return -1;
    }

    // 关键：在 connect 之前设置 TCP_NODELAY(关闭Nagle算法)
    int flag = 1;
    if( setsockopt( sockfd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof( flag ) ) < 0 )
    {
        log( LOG_ERR, __FILE__, __LINE__, "setsockopt TCP_NODELAY failed: %s", strerror( errno ) );
        close( sockfd );
        return -1;
    }

    // 连接后端服务器
    if ( connect( sockfd, ( struct sockaddr* )&address, sizeof( address ) ) != 0  )
    {
        close( sockfd );
        return -1;
    }
    return sockfd;
}


mgr::mgr( int epollfd, const host& srv, SharedData* shared, int idx )
    : m_idx( idx ), m_logic_srv( srv ), m_shared( shared )
{
    m_epollfd = epollfd;

    struct sockaddr_in address;
    bzero( &address, sizeof( address ) );
    address.sin_family = AF_INET;
    inet_pton( AF_INET, srv.m_hostname, &address.sin_addr );
    address.sin_port = htons( srv.m_port );

    log( LOG_INFO, __FILE__, __LINE__, "logcial srv host info: (%s, %d)", srv.m_hostname, srv.m_port );

    for( int i = 0; i < srv.m_conncnt; ++i )
    {
        usleep( 100000 );   // ✅ 从 sleep(1) 改为 usleep(0.1s)
        int sockfd = conn2srv( address );
        if( sockfd < 0 )
        {
            log( LOG_ERR, __FILE__, __LINE__, "build connection %d failed", i );
        }
        else
        {
            log( LOG_INFO, __FILE__, __LINE__, "build connection %d to server success", i );
            conn* tmp = new conn;
            tmp->init_srv( sockfd, address );
            m_conns.insert( pair< int, conn* >( sockfd, tmp ) );
        }
    }
}


mgr::~mgr()
{
}

int mgr::get_used_conn_cnt()
{
    return m_used.size();
}

// 从空闲池取一个连接
conn* mgr::pick_conn( int cltfd  )
{
    if( m_conns.empty() )
    {
        log( LOG_ERR, __FILE__, __LINE__, "%s", "not enough srv connections to server" );
        return NULL;
    }

    // 取第一个空闲连接
    map< int, conn* >::iterator iter =  m_conns.begin();
    int srvfd = iter->first;
    conn* tmp = iter->second;
    if( !tmp )
    {
        log( LOG_ERR, __FILE__, __LINE__, "%s", "empty server connection object" );
        return NULL;
    }

    // 从空闲池移除
    m_conns.erase( iter );
    
    // 加入使用中（用两个key索引同一个conn）
    m_used.insert( pair< int, conn* >( cltfd, tmp ) );
    m_used.insert( pair< int, conn* >( srvfd, tmp ) );

    // 监听客户端和服务器socket(先移除再添加，避免EEXIST)
    removefd( m_epollfd, cltfd );
    removefd( m_epollfd, srvfd );
    add_read_fd( m_epollfd, cltfd );
    add_read_fd( m_epollfd, srvfd );

    // 实时更新共享内存负载
    m_shared->busy_ratio[m_idx] = m_used.size();

    log( LOG_INFO, __FILE__, __LINE__, "bind client sock %d with server sock %d", cltfd, srvfd );
    return tmp;
}

// 释放连接
void mgr::free_conn( conn* connection )
{
    int cltfd = connection->m_cltfd;
    int srvfd = connection->m_srvfd;

    closefd( m_epollfd, cltfd );   // 只关闭客户端
    m_used.erase( cltfd );
    m_used.erase( srvfd );

    if( connection->m_srv_closed )
    {
        // 后端确实断了，需要修复
        closefd( m_epollfd, srvfd );
        connection->reset();
        // 放入待回收池（需要重新连接后端）
        m_freed.insert( pair< int, conn* >( srvfd, connection ) );
    }
    else
    {
        // 从 epoll 中移除 srvfd，避免它在空闲池中触发野事件
        removefd( m_epollfd, srvfd );

        // 后端还活着，直接放回空闲池
        connection->reset();
        m_conns.insert( pair< int, conn* >( srvfd, connection ) );
    }
    // 实时更新共享内存负载
    m_shared->busy_ratio[m_idx] = m_used.size();
}

// 回收并重建连接
// void mgr::recycle_conns()
// {
//     if( m_freed.empty() ) 
//     {
//         return;
//     }
//     for( map< int, conn* >::iterator iter = m_freed.begin(); iter != m_freed.end(); iter++ )
//     {
//         sleep( 1 );
//         int srvfd = iter->first;
//         conn* tmp = iter->second;
//         // 重新连接后端
//         srvfd = conn2srv( tmp->m_srv_address );
//         if( srvfd < 0 )
//         {
//             log( LOG_ERR, __FILE__, __LINE__, "%s", "fix connection failed");
//         }
//         else
//         {
//             log( LOG_INFO, __FILE__, __LINE__, "%s", "fix connection success" );
//             tmp->init_srv( srvfd, tmp->m_srv_address );
//             m_conns.insert( pair< int, conn* >( srvfd, tmp ) );
//         }
//     }
//     m_freed.clear();
// }
void mgr::recycle_conns()
{
    if( m_freed.empty() ) return;

    for( map< int, conn* >::iterator iter = m_freed.begin(); iter != m_freed.end(); )
    {
        // int srvfd = iter->first;
        conn* tmp = iter->second;

        int newfd = conn2srv( tmp->m_srv_address );
        if( newfd < 0 )
        {
            ++iter;   // 失败保留，下次重试
        }
        else
        {
            log( LOG_INFO, __FILE__, __LINE__, "%s", "fix connection success" );
            tmp->init_srv( newfd, tmp->m_srv_address );
            m_conns.insert( pair< int, conn* >( newfd, tmp ) );
            iter = m_freed.erase( iter );
        }
    }
}

// 核心事件处理（状态机）
RET_CODE mgr::process( int fd, OP_TYPE type )
{
    // 查找fd对应的连接对象
    // conn* connection = m_used[ fd ];
    map< int, conn* >::iterator iter = m_used.find( fd );
    if( iter == m_used.end() || !iter->second )
    {
        return NOTHING;
    }
    conn* connection = iter->second;

    // 分支1：处理客户端fd的事件
    if( connection->m_cltfd == fd )
    {
        int srvfd = connection->m_srvfd;
        switch( type )
        {
            case READ:  // 客户端有数据可读
            {
                RET_CODE res = connection->read_clt();
                switch( res )
                {
                    case OK:
                    {
                        log( LOG_DEBUG, __FILE__, __LINE__, "content read from client: %s", connection->m_clt_buf );
                    }
                    case BUFFER_FULL:
                    {
                        // 读完立即尝试写后端
                        // return try_write_srv( connection );
                        // 缓冲区满了，修改服务器fd为可写
                        modfd( m_epollfd, srvfd, EPOLLOUT );
                        break;
                    }
                    case IOERR:
                    case CLOSED:
                    {
                        free_conn( connection );
                        return CLOSED;
                    }
                    default:
                        break;
                }
                if( connection->m_srv_closed )
                {
                    free_conn( connection );
                    return CLOSED;
                }
                break;
            }
            case WRITE: // 可以向客户端写数据
            {
                // 可写时直接调用 try_write_clt
                // return try_write_clt( connection );
                RET_CODE res = connection->write_clt();
                switch( res )
                {
                    case TRY_AGAIN:
                    {
                        // 发送缓冲区满，继续监听写事件
                        modfd( m_epollfd, fd, EPOLLOUT );
                        break;
                    }
                    case BUFFER_EMPTY:
                    {
                        // 数据发送完毕，改回读事件
                        modfd( m_epollfd, srvfd, EPOLLIN );
                        modfd( m_epollfd, fd, EPOLLIN );
                        break;
                    }
                    case IOERR:
                    case CLOSED:
                    {
                        free_conn( connection );
                        return CLOSED;
                    }
                    default:
                        break;
                }
                if( connection->m_srv_closed )
                {
                    free_conn( connection );
                    return CLOSED;
                }
                break;
            }
            default:
            {
                log( LOG_ERR, __FILE__, __LINE__, "%s", "other operation not support yet" );
                break;
            }
        }
    }
    // 分支2：处理服务器fd的事件
    else if( connection->m_srvfd == fd )
    {
        int cltfd = connection->m_cltfd;
        switch( type )
        {
            case READ:  // 服务器有数据可读
            {
                RET_CODE res = connection->read_srv();
                switch( res )
                {
                    case OK:
                    {
                        log( LOG_DEBUG, __FILE__, __LINE__, "content read from server: %s", connection->m_srv_buf );
                    }
                    case BUFFER_FULL:
                    {
                        // // 读完立即尝试写客户端
                        // return try_write_clt( connection );
                        // 缓冲区满了，修改客户端为可写
                        modfd( m_epollfd, cltfd, EPOLLOUT );
                        break;
                    }
                    case IOERR:
                    case CLOSED:
                    {
                        // 服务器关闭，标记但继续转发剩余数据
                        modfd( m_epollfd, cltfd, EPOLLOUT );
                        connection->m_srv_closed = true;
                        // return try_write_clt( connection );
                        break;
                    }
                    default:
                        break;
                }
                break;
            }
            case WRITE: // 可以向服务器写数据
            {
                // // 可写时直接调用 try_write_srv
                // return try_write_srv( connection );
                RET_CODE res = connection->write_srv();
                log(LOG_INFO, __FILE__, __LINE__, "write_srv returned %d", res);
                switch( res )
                {
                    case TRY_AGAIN:
                    {
                        modfd( m_epollfd, fd, EPOLLOUT );
                        break;
                    }
                    case BUFFER_EMPTY:
                    {
                        modfd( m_epollfd, cltfd, EPOLLIN );
                        modfd( m_epollfd, fd, EPOLLIN );
                        break;
                    }
                    case IOERR:
                    case CLOSED:
                    {
                        /*
                        if( connection->m_srv_write_idx == connection->m_srvread_idx )
                        {
                            free_conn( connection );
                        }
                        else
                        {
                            modfd( m_epollfd, cltfd, EPOLLOUT );
                        }
                        */
                        modfd( m_epollfd, cltfd, EPOLLOUT );
                        connection->m_srv_closed = true;
                        break;
                    }
                    default:
                        break;
                }
                break;
            }
            default:
            {
                log( LOG_ERR, __FILE__, __LINE__, "%s", "other operation not support yet" );
                break;
            }
        }
    }
    else
    {
        return NOTHING;
    }
    return OK;
}

// // try_write_srv: 尝试把客户端数据写给后端，写完立即读后端
// RET_CODE mgr::try_write_srv( conn* connection )
// {
//     int srvfd = connection->m_srvfd;
//     int cltfd = connection->m_cltfd;

//     RET_CODE res = connection->write_srv();
//     switch( res )
//     {
//         case TRY_AGAIN:
//         {
//             // 后端发送缓冲区满，等它可写
//             modfd( m_epollfd, srvfd, EPOLLOUT );
//             break;
//         }
//         case BUFFER_EMPTY:
//         {
//             // 请求全部写完后，恢复两端监听读事件
//             modfd( m_epollfd, cltfd, EPOLLIN );
//             modfd( m_epollfd, srvfd, EPOLLIN );

//             // 补刀：立即尝试读后端（响应可能已经到达）
//             RET_CODE rres = connection->read_srv();
//             if( rres == OK || rres == BUFFER_FULL )
//             {
//                 // 后端有响应，直接尝试写回客户端
//                 return try_write_clt( connection );
//             }
//             else if( rres == IOERR || rres == CLOSED )
//             {
//                 // 后端断了，尝试把已有数据写回客户端
//                 connection->m_srv_closed = true;
//                 return try_write_clt( connection );
//             }
//             // rres == NOTHING：暂时无响应，等 epoll 通知
//             break;
//         }
//         case IOERR:
//         case CLOSED:
//         {
//             // 后端断了，请求发不出去，直接释放
//             free_conn( connection );
//             return CLOSED;
//         }
//         default:
//             break;
//     }
//     return OK;
// }

// // try_write_clt: 尝试把后端响应写回客户端，写完立即读客户端
// RET_CODE mgr::try_write_clt( conn* connection )
// {
//     int srvfd = connection->m_srvfd;
//     int cltfd = connection->m_cltfd;

//     RET_CODE res = connection->write_clt();
//     switch( res )
//     {
//         case TRY_AGAIN:
//         {
//             // 客户端发送缓冲区满，等它可写
//             modfd( m_epollfd, cltfd, EPOLLOUT );
//             break;
//         }
//         case BUFFER_EMPTY:
//         {
//             // 如果后端已关闭，数据写完就释放连接
//             if( connection->m_srv_closed )
//             {
//                 free_conn( connection );
//                 return CLOSED;
//             }

//             // 恢复两端监听读事件
//             modfd( m_epollfd, cltfd, EPOLLIN );
//             modfd( m_epollfd, srvfd, EPOLLIN );

//             // 补刀：立即尝试读客户端（下一个请求可能已经到达）
//             RET_CODE rres = connection->read_clt();
//             if( rres == OK || rres == BUFFER_FULL )
//             {
//                 // 客户端有新请求，直接尝试写给后端
//                 return try_write_srv( connection );
//             }
//             else if( rres == IOERR || rres == CLOSED )
//             {
//                 free_conn( connection );
//                 return CLOSED;
//             }
//             // rres == NOTHING：暂时无新请求，等 epoll 通知
//             break;
//         }
//         case IOERR:
//         case CLOSED:
//         {
//             free_conn( connection );
//             return CLOSED;
//         }
//         default:
//             break;
//     }
//     return OK;
// }
