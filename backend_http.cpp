// http_backend.cpp
// 编译: g++ -O2 -std=c++11 -o http_backend http_backend.cpp -lpthread
// 运行: ./http_backend 13579

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

static const int MAX_EVENTS = 10240;
static const int BUF_SIZE = 4096;
static const char* RESPONSE =
    "HTTP/1.1 200 OK\r\n"
    "Server: cpp-backend\r\n"
    "Content-Type: text/plain\r\n"
    "Content-Length: 2\r\n"
    "Connection: keep-alive\r\n"
    "\r\n"
    "OK";
const int RESPONSE_LEN = (int)strlen(RESPONSE);

static int set_nonblocking(int fd)
{
    int old = fcntl(fd, F_GETFL);
    fcntl(fd, F_SETFL, old | O_NONBLOCK);
    return old;
}

int main(int argc, char* argv[])
{
    signal(SIGPIPE, SIG_IGN);

    int port = (argc > 1) ? atoi(argv[1]) : 13579;

    // 创建监听 socket
    int listenfd = socket(AF_INET, SOCK_STREAM, 0);
    if (listenfd < 0) { perror("socket"); return 1; }

    int opt = 1;
    setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(port);

    if (bind(listenfd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("bind"); return 1;
    }
    if (listen(listenfd, 1024) < 0) {
        perror("listen"); return 1;
    }

    set_nonblocking(listenfd);

    // 创建 epoll
    int epfd = epoll_create1(0);
    if (epfd < 0) { perror("epoll_create1"); return 1; }

    struct epoll_event ev, events[MAX_EVENTS];
    ev.events = EPOLLIN;   // listenfd 用 LT，避免 ET 死锁
    ev.data.fd = listenfd;
    epoll_ctl(epfd, EPOLL_CTL_ADD, listenfd, &ev);

    printf("HTTP 后端已启动，监听 %d\n", port);

    char buf[BUF_SIZE];

    while (1) {
        int n = epoll_wait(epfd, events, MAX_EVENTS, -1);
        if (n < 0) {
            if (errno == EINTR) continue;
            perror("epoll_wait"); break;
        }

        for (int i = 0; i < n; i++) {
            int fd = events[i].data.fd;

            // ========== 新连接 ==========
            if (fd == listenfd) {
                while (1) {
                    struct sockaddr_in cli;
                    socklen_t len = sizeof(cli);
                    int connfd = accept(listenfd, (struct sockaddr*)&cli, &len);
                    if (connfd < 0) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                        perror("accept");
                        break;
                    }

                    // 关键：关闭 Nagle + 快速 ACK，降低延迟
                    int flag = 1;
                    setsockopt(connfd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));
                    setsockopt(connfd, IPPROTO_TCP, TCP_QUICKACK, &flag, sizeof(flag));

                    set_nonblocking(connfd);

                    ev.events = EPOLLIN | EPOLLET;   // 客户端连接用 ET
                    ev.data.fd = connfd;
                    epoll_ctl(epfd, EPOLL_CTL_ADD, connfd, &ev);
                }
                continue;
            }

            // ========== 已连接 socket ==========
            // 读取请求（ET 模式，必须循环读到 EAGAIN）
            int closed = 0;
            while (1) {
                ssize_t r = recv(fd, buf, BUF_SIZE, 0);
                if (r > 0) {
                    // 简单判断：收到 \r\n\r\n 就认为请求完整
                    // 直接回响应，不解析具体内容
                    // 如果有请求体（POST），我们不处理，直接忽略
                    // 这里简化：收到任何数据就直接响应
                    ssize_t w = send(fd, RESPONSE, RESPONSE_LEN, MSG_NOSIGNAL);
                    if (w < 0) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) {
                            // 发送缓冲区满，注册 EPOLLOUT（这里简化：直接关闭）
                            closed = 1;
                            break;
                        }
                        closed = 1;
                        break;
                    }
                    // 继续读，处理 pipelining
                } else if (r == 0) {
                    closed = 1;
                    break;
                } else {
                    if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                    closed = 1;
                    break;
                }
            }

            if (closed) {
                epoll_ctl(epfd, EPOLL_CTL_DEL, fd, NULL);
                close(fd);
            }
        }
    }

    close(listenfd);
    close(epfd);
    return 0;
}