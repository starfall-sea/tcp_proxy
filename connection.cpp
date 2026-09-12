#include <netinet/tcp.h>
#include <exception>
#include <errno.h>
#include <string.h>
#include "connection.h"
#include "logger.h"
#include "fd_wrap.h"

/*这里使用的new逻辑是直接生搬malloc的逻辑，但是其实没有必要，
因为c++中new失败会直接报错，不需要手动抛出异常*/
conn::conn()
{
    m_srvfd = -1;
    // 分配客户端缓冲区
    m_clt_buf = new char[ BUF_SIZE ];
    if( !m_clt_buf )
    {
        throw std::exception();
    }
    // 分配服务端缓冲区
    m_srv_buf = new char[ BUF_SIZE ];
    if( !m_srv_buf )
    {
        throw std::exception();
    }
    // 初始化状态
    reset();
}

conn::~conn()
{
    delete [] m_clt_buf;
    delete [] m_srv_buf;
}

void conn::init_clt( int sockfd, const sockaddr_in& client_addr )
{
    m_cltfd = sockfd;
    m_clt_address = client_addr;
}

void conn::init_srv( int sockfd, const sockaddr_in& server_addr )
{
    m_srvfd = sockfd;
    m_srv_address = server_addr;
}

void conn::reset()
{
    m_clt_read_idx = 0;
    m_clt_write_idx = 0;
    m_srv_read_idx = 0;
    m_srv_write_idx = 0;
    m_srv_closed = false;
    m_cltfd = -1;
    memset( m_clt_buf, '\0', BUF_SIZE );
    memset( m_srv_buf, '\0', BUF_SIZE );
}

RET_CODE conn::read_clt()
{
    int bytes_read = 0;

    while( true )
    {
        // 如果缓冲区已满，删除已消费的数据（指针回滚机制，原地滑动窗口）
        if( m_clt_read_idx >= BUF_SIZE )
        {
            // 如果缓冲区全是未消费数据
            if( m_clt_write_idx == 0 )
            {
                log( LOG_ERR, __FILE__, __LINE__, "%s", "the client read buffer is full, let server write" );
                return BUFFER_FULL;
            }

            // 计算未消费数据
            int data_left = m_clt_read_idx - m_clt_write_idx;
            if( data_left == 0 )
            {
                // 数据全部被消费了，重置读写指针到开头
                m_clt_read_idx = 0;
                m_clt_write_idx = 0;
            }
            else
            {
                // 还有未消费的数据，将它们移动到缓冲区开头，腾出后面的空间
                memmove( m_clt_buf, m_clt_buf + m_clt_write_idx, data_left );
                m_clt_read_idx = data_left;
                m_clt_write_idx = 0;
            }
        }

        // 从客户端读数据
        bytes_read = recv( m_cltfd, 
            m_clt_buf + m_clt_read_idx,     // 写入位置
            BUF_SIZE - m_clt_read_idx,      // 剩余空间
            0 );

        if ( bytes_read == -1 )
        {
            if( errno == EAGAIN || errno == EWOULDBLOCK )
            {
                break;  // 没有数据了，正常退出
            }
            return IOERR;   // 真正的错误
        }
        else if ( bytes_read == 0 )
        {
            return CLOSED;  // 客户端关闭连接
        }

        m_clt_read_idx += bytes_read;   // 更新读取位置

        // 打开TCP_QUICKACK，降低延迟
        int flag = 1;
        if( setsockopt( m_cltfd, IPPROTO_TCP, TCP_QUICKACK, &flag, sizeof( flag ) ) < 0 )
        {
            log( LOG_ERR, __FILE__, __LINE__, "setsockopt TCP_QUICKACK failed: %s", strerror( errno ) );
        }
    }
    // 如果缓冲区有数据，返回OK
    return ( ( m_clt_read_idx - m_clt_write_idx ) > 0 ) ? OK : NOTHING;
}
// 函数逻辑同上
RET_CODE conn::read_srv()
{
    int bytes_read = 0;
    while( true )
    {
        if( m_srv_read_idx >= BUF_SIZE )
        {
            if( m_srv_write_idx == 0 )
            {
                log( LOG_ERR, __FILE__, __LINE__, "%s", "the server read buffer is full, let client write" );
                return BUFFER_FULL;
            }

            int data_left = m_srv_read_idx - m_srv_write_idx;
            if( data_left == 0 )
            {
                m_srv_read_idx = 0;
                m_srv_write_idx = 0;
            }
            else
            {
                memmove( m_srv_buf, m_srv_buf + m_srv_write_idx, data_left );
                m_srv_read_idx = data_left;
                m_srv_write_idx = 0;
            }
        }

        bytes_read = recv( m_srvfd, m_srv_buf + m_srv_read_idx, BUF_SIZE - m_srv_read_idx, 0 );
        if ( bytes_read == -1 )
        {
            if( errno == EAGAIN || errno == EWOULDBLOCK )
            {
                break;
            }
            return IOERR;
        }
        else if ( bytes_read == 0 )
        {
            log( LOG_ERR, __FILE__, __LINE__, "%s", "the server should not close the persist connection" );
            return CLOSED;
        }

        m_srv_read_idx += bytes_read;
        int flag = 1;
        if( setsockopt( m_srvfd, IPPROTO_TCP, TCP_QUICKACK, &flag, sizeof( flag ) ) < 0 )
        {
            log( LOG_ERR, __FILE__, __LINE__, "setsockopt TCP_QUICKACK failed: %s", strerror( errno ) );
        }
    }
    return ( ( m_srv_read_idx - m_srv_write_idx ) > 0 ) ? OK : NOTHING;
}

RET_CODE conn::write_srv()
{
    int bytes_write = 0;
    while( true )
    {
        // 检查是否所有数据都已发送
        if( m_clt_read_idx <= m_clt_write_idx )
        {
            // 重置缓冲区，准备接收新数据
            m_clt_read_idx = 0;
            m_clt_write_idx = 0;
            return BUFFER_EMPTY;    // 缓冲区已空
        }

        // 发送数据到服务器
        log(LOG_DEBUG, __FILE__, __LINE__, "write_srv: read_idx=%d, write_idx=%d", m_clt_read_idx, m_clt_write_idx);
        bytes_write = send( m_srvfd, m_clt_buf + m_clt_write_idx, m_clt_read_idx - m_clt_write_idx, 0 );
        log(LOG_DEBUG, __FILE__, __LINE__, "write_srv: sent %d bytes", bytes_write);
        
        if ( bytes_write == -1 )
        {
            if( errno == EAGAIN || errno == EWOULDBLOCK )
            {
                return TRY_AGAIN;   // 发送缓冲区满了，稍后重试
            }
            log( LOG_ERR, __FILE__, __LINE__, "write server socket failed, %s", strerror( errno ) );
            return IOERR;
        }
        else if ( bytes_write == 0 )
        {
            return CLOSED;
        }

        m_clt_write_idx += bytes_write; // 更新发送位置
    }
}
// 逻辑注释同上
RET_CODE conn::write_clt()
{
    int bytes_write = 0;
    while( true )
    {
        if( m_srv_read_idx <= m_srv_write_idx )
        {
            m_srv_read_idx = 0;
            m_srv_write_idx = 0;
            return BUFFER_EMPTY;
        }

        bytes_write = send( m_cltfd, m_srv_buf + m_srv_write_idx, m_srv_read_idx - m_srv_write_idx, 0 );
        if ( bytes_write == -1 )
        {
            if( errno == EAGAIN || errno == EWOULDBLOCK )
            {
                return TRY_AGAIN;
            }
            log( LOG_ERR, __FILE__, __LINE__, "write client socket failed, %s", strerror( errno ) );
            return IOERR;
        }
        else if ( bytes_write == 0 )
        {
            return CLOSED;
        }

        m_srv_write_idx += bytes_write;
    }
}
