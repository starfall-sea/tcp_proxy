#include <unistd.h>
#include <fcntl.h>
#include <sys/epoll.h>
#include "fd_wrap.h"

int setnonblocking( int fd )
{
    int old_option = fcntl( fd, F_GETFL );      // 获取当前标志
    int new_option = old_option | O_NONBLOCK;   // 加上非阻塞标志
    fcntl( fd, F_SETFL, new_option );           // 把新标志设置给fd
    return old_option;                          // 返回旧标志，用于恢复
}

void add_read_fd( int epollfd, int fd )
{
    epoll_event event;
    event.data.fd = fd;
    event.events = EPOLLIN | EPOLLET;   // 读事件 + 边缘触发
    epoll_ctl( epollfd, EPOLL_CTL_ADD, fd, &event );
    setnonblocking( fd );   // 所有epoll管理的fd都设为非阻塞
}

void add_write_fd( int epollfd, int fd )
{
    epoll_event event;
    event.data.fd = fd;
    event.events = EPOLLOUT | EPOLLET;  //写事件 + 边缘触发
    epoll_ctl( epollfd, EPOLL_CTL_ADD, fd, &event );
    setnonblocking( fd );   // 设置非阻塞
}

void add_listen_fd( int epollfd, int fd )
{
    epoll_event event;
    event.data.fd = fd;
    event.events = EPOLLIN;   // 关键：不加 EPOLLET，使用 LT 模式
    epoll_ctl( epollfd, EPOLL_CTL_ADD, fd, &event );
    setnonblocking( fd );
}

void closefd( int epollfd, int fd )
{
    epoll_ctl( epollfd, EPOLL_CTL_DEL, fd, 0 ); // 从epoll中移除
    close( fd );    // 关闭fd
}

void removefd( int epollfd, int fd )
{
    epoll_ctl( epollfd, EPOLL_CTL_DEL, fd, 0 ); // 只移除不关闭
}

void modfd( int epollfd, int fd, int ev )
{
    epoll_event event;
    event.data.fd = fd;
    event.events = ev | EPOLLET;    // 保留ET模式
    epoll_ctl( epollfd, EPOLL_CTL_MOD, fd, &event );
}
