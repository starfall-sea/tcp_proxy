#ifndef PROCESSPOOL_H
#define PROCESSPOOL_H

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/mman.h>
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
#include <vector>
#include "logger.h"
#include "fd_wrap.h"
#include "shared.h"

using std::vector;

// 子进程信息
class process
{
public:
    process() : m_pid( -1 ){}

public:
    int m_busy_ratio;   // 负载（活跃连接数）
    pid_t m_pid;        // 进程ID
    int m_pipefd[2];    // socketpair双向通道，用于父子进程通信
};

// 进程池模板类
template< typename C, typename H, typename M >
class processpool
{
private:
    processpool( int listenfd, int process_number = 8 );
public:
    static processpool< C, H, M >* create( int listenfd, int process_number = 8 )
    {
        if( !m_instance )
        {
            m_instance = new processpool< C, H, M >( listenfd, process_number );
        }
        return m_instance;
    }
    ~processpool()
    {
        delete [] m_sub_process;
    }
    void run( const vector<H>& arg );

private:
    int get_most_free_srv();    // 选择最空闲的进程
    void setup_sig_pipe();      // 设置信号通道
    void run_parent();          // 运行父进程
    void run_child( const vector<H>& arg ); // 运行子进程

private:
    static const int USER_PER_PROCESS = 65536;  // 
    static const int MAX_EVENT_NUMBER = 10000;  // 最大事件数
    int m_process_number;   // 子进程进程数
    int m_idx;              // 子进程在数组中的索引
    int m_epollfd;          // epoll文件描述符
    int m_listenfd;         // 监听套接字
    int m_stop;             // 开关状态
    SharedData* m_shared;   // 共享内存（用来传负载，消除毫秒级延迟）
    process* m_sub_process; // 子进程数组
    static processpool< C, H, M >* m_instance; // 单例模式
};

// 静态成员初始化
template< typename C, typename H, typename M >
processpool< C, H, M >* processpool< C, H, M >::m_instance = NULL;

// 全局变量和信号处理
static const int MAX_ACCEPT_PER_ROUND = 16; // 每次 worker 最多 accept 的连接数（压力越大越均衡，代价是额外通信开销）
static int EPOLL_WAIT_TIME = 5000;
static int sig_pipefd[2];

/*
信号处理函数
内部必须要保存和恢复状态，因为处理函数内部有可能改变errno状态，会导致主程序判断错误
*/ 
static void sig_handler( int sig )
{
    int save_errno = errno; // 保存当前状态
    int msg = sig;
    // 将信号值写入管道
    send( sig_pipefd[1], ( char* )&msg, 1, 0 );
    errno = save_errno; // 恢复状态
}

// 登记信号处理的函数
static void addsig( int sig, void( handler )(int), bool restart = true )
{
    struct sigaction sa;
    memset( &sa, '\0', sizeof( sa ) );
    sa.sa_handler = handler;
    /*
    自动重试被信号函数中断的函数（epoll_wait、read、write、accept等）
    （select、poll不会重试）
    */
    if( restart )
    {
        sa.sa_flags |= SA_RESTART;
    }
    /*
    掩码控制
    sa_mask中的信号会被暂时屏蔽，防止重入，
    而sigfillset会把所有信号加入sa_mask即暂时屏蔽其他所有信号
    */
    sigfillset( &sa.sa_mask );
    assert( sigaction( sig, &sa, NULL ) != -1 );
}

// 设置信号管道
template< typename C, typename H, typename M >
void processpool< C, H, M >::setup_sig_pipe()
{
    // 创建监视信号的epoll实例
    m_epollfd = epoll_create( 5 );
    assert( m_epollfd != -1 );

    int ret = socketpair( PF_UNIX, SOCK_STREAM, 0, sig_pipefd );
    assert( ret != -1 );

    setnonblocking( sig_pipefd[1] );
    add_read_fd( m_epollfd, sig_pipefd[0] );

    // 注册信号
    addsig( SIGCHLD, sig_handler ); // 子进程退出
    addsig( SIGTERM, sig_handler ); // 终止信号
    addsig( SIGINT, sig_handler );  // Ctrl+C
    addsig( SIGPIPE, SIG_IGN );     // 忽略SIGPIPE
    // 当代理服务器向后端服务器发送数据时，后端突然关闭了连接，此时我的子进程就会收到SIGPIPE
    // 而SIGPIPE的默认行为是杀死整个进程，这意味着一个后端断开连接，就会导致一个子进程崩溃
}

// 构造函数：创建进程池
template< typename C, typename H, typename M >
processpool< C, H, M >::processpool( int listenfd, int process_number ) 
    : m_process_number( process_number ), m_idx( -1 ), m_listenfd( listenfd ), m_stop( false )
{
    assert( ( process_number > 0 ) && ( process_number <= MAX_PROCESS_NUMBER ) );

    // fork之前创建共享内存
    m_shared = ( SharedData* )mmap( NULL, sizeof( SharedData ), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0 );
    assert( m_shared != MAP_FAILED );
    memset( ( void* )m_shared, 0, sizeof( SharedData ) );

    m_sub_process = new process[ process_number ];
    assert( m_sub_process );

    //创建process_number个子进程
    for( int i = 0; i < process_number; ++i )
    {
        // 创建父子通信管道（全双工通道）
        int ret = socketpair( PF_UNIX, SOCK_STREAM, 0, m_sub_process[i].m_pipefd );
        assert( ret == 0 );

        // pipefd 两端非阻塞，一次读完管道不会阻塞
        setnonblocking( m_sub_process[i].m_pipefd[0] );
        setnonblocking( m_sub_process[i].m_pipefd[1] );

        // fork子进程
        m_sub_process[i].m_pid = fork();
        assert( m_sub_process[i].m_pid >= 0 );
        
        /*
        这里为什么要关闭一端呢？
        答：1.socketpair创建的是全双工通道，两个端点都可以双向读写（不想pipe匿名通道那样严格0读、1写）
        这里关闭一端，是为了防止数据回环和明晰用法，父进程通过[0]发送命令和接收负载，而子进程则用[1]
        数据回环：子进程向[1]发送数据，本意发给父进程，但如果子进程不小心从自己的[0]端去读，可能会
        把刚刚的信息读回来。 
        2.触发EOF，自动检测对方存活，各关闭一个端点，此时只剩一个写端和一个读端，当任意一个端关闭自动向另
        一端发送EOF，另一端自动接收到了对向关闭的信息。（当一个socketpipe的所有写端都关闭会向读端发EOF）
        */
        if( m_sub_process[i].m_pid > 0 )
        {
            // 父进程：关闭写段，保留写端
            close( m_sub_process[i].m_pipefd[1] );
            m_sub_process[i].m_busy_ratio = 0;
            continue;
        }
        else
        {
            // 子进程：关闭读端，保留写端
            close( m_sub_process[i].m_pipefd[0] );
            m_idx = i;  // 记录自己在数组中的位置
            break;  // 子进程break跳出循环，防止子进程继续fork（阻止fork炸弹）
        }
    }
}

template< typename C, typename H, typename M >
int processpool< C, H, M >::get_most_free_srv()
{   
    int idx = -1;
    int min_ratio = 0x7fffffff;
    static int last_assigned = -1;   // 记录上次分配，用于轮询

    // 第一轮：找最小 ratio
    for( int i = 0; i < m_process_number; ++i )
    {
        if( m_sub_process[i].m_pid == -1 ) continue;
        int ratio = m_shared->busy_ratio[i];   // 实时读共享内存
        if( ratio < min_ratio )
        {
            min_ratio = ratio;
            idx = i;
        }
    }
    if( idx == -1 ) return -1;

    // 第二轮：在等于 min_ratio 的 worker 中，从上次分配的下一个开始轮询
    int start = ( last_assigned + 1 ) % m_process_number;
    for( int k = 0; k < m_process_number; ++k )
    {
        int i = ( start + k ) % m_process_number;
        if( m_sub_process[i].m_pid == -1 ) continue;
        if( m_shared->busy_ratio[i] == min_ratio )
        {
            last_assigned = i;
            return i;
        }
    }
    return idx;
}

// 公共接口：启动进程池
template< typename C, typename H, typename M >
void processpool< C, H, M >::run( const vector<H>& arg )
{
    if( m_idx != -1 )
    {
        run_child( arg );   // 子进程
        return;
    }
    run_parent();   // 父进程
}

// 子进程运行
template< typename C, typename H, typename M >
void processpool< C, H, M >::run_child( const vector<H>& arg )
{
    /*
    先fork再注册信号，这样可以让每个进程有独立的epoll实例来监视信号，
    不然有可能出现，子进程的信号，但父进程调用了信号处理函数
    */
    setup_sig_pipe();

    // 监听父进程管道
    int pipefd_read = m_sub_process[m_idx].m_pipefd[ 1 ];
    add_read_fd( m_epollfd, pipefd_read );

    epoll_event events[ MAX_EVENT_NUMBER ];

    // 创建连接管理器
    M* manager = new M( m_epollfd, arg[m_idx], m_shared, m_idx );
    assert( manager );

    int number = 0;
    int ret = -1;

    while( ! m_stop )
    {
        number = epoll_wait( m_epollfd, events, MAX_EVENT_NUMBER, EPOLL_WAIT_TIME );
        if ( ( number < 0 ) && ( errno != EINTR ) )
        {
            log( LOG_ERR, __FILE__, __LINE__, "%s", "epoll failure" );
            break;
        }

        // 超时回收连接，并修复死掉的连接
        if( number == 0 )
        {
            manager->recycle_conns();
            continue;
        }

        for ( int i = 0; i < number; i++ )
        {
            int sockfd = events[i].data.fd;

            // 事件1：父进程唤醒，有新连接
            if( ( sockfd == pipefd_read ) && ( events[i].events & EPOLLIN ) )
            {
                // 读空管道
                int dummy;
                while( true )
                {
                    ret = recv( sockfd, ( char* )&dummy, sizeof( dummy ), 0 );
                    if( ret <= 0 )
                    {
                        if( ret < 0 && errno == EAGAIN ) break;
                        break;
                    }
                }

                // 限额循环 accept
                int accepted = 0;
                while( accepted < MAX_ACCEPT_PER_ROUND )
                {
                    struct sockaddr_in client_address;
                    socklen_t client_addrlength = sizeof( client_address );
                    int connfd = accept( m_listenfd, ( struct sockaddr* )&client_address, &client_addrlength );

                    if( connfd < 0 )
                    {
                        if( errno == EAGAIN || errno == EWOULDBLOCK ) break;
                        log( LOG_ERR, __FILE__, __LINE__, "accept errno: %s", strerror( errno ) );
                        break;
                    }

                    int flag = 1;
                    setsockopt( connfd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof( flag ) );
                    add_read_fd( m_epollfd, connfd );

                    C* conn = manager->pick_conn( connfd );
                    if( !conn )
                    {
                        closefd( m_epollfd, connfd );
                        continue;
                    }
                    conn->init_clt( connfd, client_address );
                    accepted++;
                }

                // 达到上限：写共享内存 + 管道唤醒
                if( accepted == MAX_ACCEPT_PER_ROUND )
                {
                    m_shared->global_need_retry = 1;
                    int signal = 1;
                    send( pipefd_read, ( char* )&signal, sizeof( signal ), 0 );
                }
            }
            // 事件2：信号管道
            else if( ( sockfd == sig_pipefd[0] ) && ( events[i].events & EPOLLIN ) )
            {
                // int sig;
                char signals[1024];
                ret = recv( sig_pipefd[0], signals, sizeof( signals ), 0 );
                if( ret <= 0 )
                {
                    continue;
                }
                else
                {
                    for( int i = 0; i < ret; ++i )
                    {
                        switch( signals[i] )
                        {
                            case SIGCHLD:
                            {
                                // 回收僵尸进程
                                pid_t pid;
                                int stat;
                                while ( ( pid = waitpid( -1, &stat, WNOHANG ) ) > 0 )
                                {
                                    continue;
                                }
                                break;
                            }
                            case SIGTERM:
                            case SIGINT:
                            {
                                m_stop = true;
                                break;
                            }
                            default:
                            {
                                break;
                            }
                        }
                    }
                }
            }
            // 事件3：普通I/O事件
            else if( events[i].events & EPOLLIN )
            {
                 RET_CODE result = manager->process( sockfd, READ );
                 switch( result )
                 {
                     case CLOSED:
                     {
                        m_shared->busy_ratio[m_idx] = manager->get_used_conn_cnt();
                        break;
                     }
                     default:
                        break;
                 }
            }
            else if( events[i].events & EPOLLOUT )
            {
                 RET_CODE result = manager->process( sockfd, WRITE );
                 switch( result )
                 {
                     case CLOSED:
                     {
                        m_shared->busy_ratio[m_idx] = manager->get_used_conn_cnt();
                        break;
                     }
                     default:
                        break;
                 }
            }
            else
            {
                continue;
            }
        }
    }

    close( pipefd_read );
    close( m_epollfd );
}

// 父进程运行
template< typename C, typename H, typename M >
void processpool< C, H, M >::run_parent()
{
    setup_sig_pipe();

    // 监听所有子进程的管道
    for( int i = 0; i < m_process_number; ++i )
    {
        add_read_fd( m_epollfd, m_sub_process[i].m_pipefd[ 0 ] );
    }

    add_read_fd( m_epollfd, m_listenfd );   // ET模式
    // add_listen_fd( m_epollfd, m_listenfd );  // LT模式

    epoll_event events[ MAX_EVENT_NUMBER ];
    int new_conn = 1;
    int number = 0;
    int ret = -1;

    while( ! m_stop )
    {
        number = epoll_wait( m_epollfd, events, MAX_EVENT_NUMBER, EPOLL_WAIT_TIME );
        if ( ( number < 0 ) && ( errno != EINTR ) )
        {
            log( LOG_ERR, __FILE__, __LINE__, "%s", "epoll failure" );
            break;
        }

        for ( int i = 0; i < number; i++ )
        {
            int sockfd = events[i].data.fd;

            // 事件1：新客户端连接
            if( sockfd == m_listenfd )
            {
                // 选择最空闲的子进程
                int idx = get_most_free_srv();
                // 通知子进程
                if( idx != -1 )
                {
                    send( m_sub_process[idx].m_pipefd[0], ( char* )&new_conn, sizeof( new_conn ), 0 );
                    // send( m_sub_process[idx].m_pipefd[0], ( char* )&new_conn, sizeof( new_conn ), 0 );
                    log( LOG_INFO, __FILE__, __LINE__, "send request to child %d", idx );
                }
            }
            // 事件2：信号通道
            else if( ( sockfd == sig_pipefd[0] ) && ( events[i].events & EPOLLIN ) )
            {
                char signals[1024];
                ret = recv( sig_pipefd[0], signals, sizeof( signals ), 0 );
                if( ret <= 0 )
                {
                    continue;
                }
                else
                {
                    for( int i = 0; i < ret; ++i )
                    {
                        switch( signals[i] )
                        {
                            case SIGCHLD:
                            {
                                // 子进程退出，标记
                                pid_t pid;
                                int stat;
                                while ( ( pid = waitpid( -1, &stat, WNOHANG ) ) > 0 )
                                {
                                    for( int i = 0; i < m_process_number; ++i )
                                    {
                                        if( m_sub_process[i].m_pid == pid )
                                        {
                                            log( LOG_INFO, __FILE__, __LINE__, "child %d join", i );
                                            close( m_sub_process[i].m_pipefd[0] );
                                            m_sub_process[i].m_pid = -1;
                                        }
                                    }
                                }
                                // 检查是否所有子进程都退出了
                                m_stop = true;
                                for( int i = 0; i < m_process_number; ++i )
                                {
                                    if( m_sub_process[i].m_pid != -1 )
                                    {
                                        m_stop = false;
                                    }
                                }
                                break;
                            }
                            case SIGTERM:
                            case SIGINT:
                            // 终止所有子进程
                            {
                                log( LOG_INFO, __FILE__, __LINE__, "%s", "kill all the clild now" );
                                for( int i = 0; i < m_process_number; ++i )
                                {
                                    int pid = m_sub_process[i].m_pid;
                                    if( pid != -1 )
                                    {
                                        kill( pid, SIGTERM );
                                    }
                                }
                                break;
                            }
                            default:
                            {
                                break;
                            }
                        }
                    }
                }
            }
            else if( events[i].events & EPOLLIN )
            {
                // 读空管道
                int dummy;
                while( true )
                {
                    ret = recv( sockfd, ( char* )&dummy, sizeof( dummy ), 0 );
                    if( ret <= 0 )
                    {
                        if( ret < 0 && errno == EAGAIN ) break;
                        break;
                    }
                }

                // 检查全局 need_retry 标志（共享内存）
                if( m_shared->global_need_retry )
                {
                    m_shared->global_need_retry = 0;
                    int idx = get_most_free_srv();
                    if( idx != -1 )
                    {
                        send( m_sub_process[idx].m_pipefd[0], ( char* )&new_conn, sizeof( new_conn ), 0 );
                    }
                }
            }

        }
    }

    // 清理
    for( int i = 0; i < m_process_number; ++i )
    {
        closefd( m_epollfd, m_sub_process[i].m_pipefd[ 0 ] );
    }
    close( m_epollfd );
}

#endif
