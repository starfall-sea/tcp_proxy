# tcp_proxy

> 基于 **多进程 Master-Worker + epoll 边缘触发** 架构的高性能 TCP 反向代理服务器，支持连接池、实时负载均衡与共享内存通信。

---

## 项目介绍
`tcppxy` 是一个用 C++11 编写的轻量级反向代理服务器。它位于客户端与后端服务之间，负责将客户端的 TCP 请求转发到后端服务器，并支持多个后端实例的负载均衡。

### 核心特性

| 特性 | 说明 |
| :--- | :--- |
| 多进程模型 | Master-Worker 架构，父进程负责分发连接，子进程独立处理 I/O |
| epoll 边缘触发 | 所有客户端与后端连接使用 EPOLLET，减少事件通知次数，提升吞吐 |
| 连接池 | 每个 Worker 预先建立到后端的 TCP 长连接，避免重复三次握手 |
| 实时负载均衡 | 通过共享内存读取各 Worker 的实时连接数，采用最小连接数算法 + 轮询打破平局 |
| 共享内存通信 | mmap(MAP_SHARED \| MAP_ANONYMOUS) 存放负载数据，父子进程零延迟读取 |
| 限额 accept | Worker 每次最多 accept N 个连接后交还父进程重新仲裁，防止单 Worker 过载 |
| TCP 优化 | 启用 TCP_NODELAY 与 TCP_QUICKACK，消除 40ms 延迟确认 |
| 信号安全 | Self-Pipe 技巧处理 SIGCHLD / SIGTERM，避免异步信号竞争 |
| 连接自愈 | 后端连接断开后自动重连，连接池具备自恢复能力 |

### 性能数据

**测试环境**：本地回环（loopback），`wrk -t8 -c<并发> -d30s --latency`

| 并发 | QPS | 平均延迟 | P50 | P99 |
| :---: | :---: | :---: | :---: | :---: |
| 50 | 246,610 | 187us | 184us | 407us |
| **100** | **305,598** | **307us** | **289us** | **635us** |
| 150 | 319,293 | 444us | 421us | 880us |
| 200 | 331,554 | 595us | 556us | 1.14ms |
| **300** | **338,046** | **870us** | **799us** | **1.63ms** |

- **峰值 QPS**：33.8 万
- **最佳性价比点**：30.5 万 QPS / 307us（c=100）
- **错误率**：0.00004%（1000 万请求仅 4 个错误）

### 系统架构

```
┌─────────────────────────────────────────────────────────────┐
│                     客户端 (wrk / curl)                       │
└─────────────────────────┬───────────────────────────────────┘
                          │ TCP
                          ▼
┌─────────────────────────────────────────────────────────────┐
│  父进程 (Master)                                              │
│  ├─ epoll 监听 listenfd + 所有 Worker 的 pipefd               │
│  ├─ 新连接 → 读共享内存选最空闲 Worker → send(pipefd) 唤醒     │
│  ├─ 收到 Worker 的 need_retry → 重新仲裁                       │
│  └─ 处理 SIGCHLD / SIGTERM                                    │
└─────────────────────────┬───────────────────────────────────┘
                          │ mmap 共享内存 + socketpair 唤醒
                          ▼
┌─────────────────────────────────────────────────────────────┐
│  子进程 × N (Worker)                                          │
│  ├─ 每个 Worker 独立的 epoll 实例                              │
│  ├─ epoll 监听 pipefd + 客户端连接 + 后端连接                  │
│  ├─ 收到父进程唤醒 → 限额 accept 客户端连接                    │
│  ├─ 从连接池取出后端连接（conn 对象管理双向缓冲区）            │
│  └─ 每次 accept/close 实时更新共享内存 busy_ratio              │
└─────────────────────────┬───────────────────────────────────┘
                          │ 预建连接池
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                     后端服务器                                 │
└─────────────────────────────────────────────────────────────┘
```

### 项目结构

```
tcp_proxy/
├── CMakeLists.txt          # CMake 构建脚本
├── config.xml              # 代理配置文件
├── main.cpp                # 程序入口：配置解析、监听 socket、启动进程池
├── proc_pool.h             # 进程池模板类（Master-Worker 调度）
├── shared.h                # 共享内存结构定义
├── manager.h / manager.cpp # 连接池与 I/O 状态机
├── connection.h / connection.cpp  # 单连接的缓冲区管理与读写
├── fd_wrap.h / fd_wrap.cpp # epoll 与文件描述符封装
├── logger.h / logger.cpp   # 日志系统
├── backend_http.cpp        # 用于测试的 C++ epoll HTTP 后端
└── README.md
```

---

## 操作指南

### 环境要求

| 依赖 | 版本 |
| :--- | :--- |
| 操作系统 | Linux（内核 ≥ 2.6，支持 epoll） |
| 编译器 |  ≥ 4.8（支持 C++11） |
| 构建工具 | CMake ≥ 3.10 |
| 压测工具 | wrk（推荐）或 ab |

### 1. 安装依赖

```bash
# Ubuntu / Debian
sudo apt update
sudo apt install -y build-essential ca
# 安装 wrk 压测工具
sudo apt install -y wrk
# 或从源码编译
# git clone https://github.com/wg/wrk && cd wrk && make && sudo cp wrk /usr/local/bin/
```

### 2. 编译项目

```bash
git clone <your-repo-url> tcp_proxy
cd tcp_proxy
mkdir -p build && cd build
cmake ..
make -j4
```

编译完成后，`build/` 目录下会生成两个可执行文件：

| 可执行文件 | 说明 |
| :--- | :--- |
| `tcp_proxy` | 反向代理主程序 |
| `backend_http` |TTP 后端 |

### 3. 配置文件

`config.xml` 示例：

```xml
Listen 127.0.0.1:12345

<logical_host>
  <name>127.0.0.1</name>
  <port>13579</port>
  <conns>5</conns>
</logical_host>
<logical_host>
  <name>127.0.0.1</name>
  <port>13579</port>
  <conns>5</conns>
</logical_host>
<logical_host>
  <name>127.0.0.1</name>
  <port>13579</port>
  <conns>5</conns>
</logical_host>
```

| 字段 | 说明 |
| :--- | :--- |
| `Listen` | 代理监听的地址与端口 |
| `<logical_host>` | 一个后端逻辑主机，**每个块对应一个 Worker 进程，可按需求自行增减，注意最后要空一行，来配合xml解析** |
| `<name>` | 后端服务器 IP 或域名，自定义名称即可 |
| `<port>` | 后端服务器端口 |
| `<conns>` | 该 Worker 预建的后端连接数 |

> **提示**：`<logical_host>` 的块数决定 Worker 进程数，建议与 CPU 核心数相当。

### 4. 启动服务

#### 步骤 1：启动后端服务器

```bash
# 绑定到 CPU 8，监听 13579 端口
taskset -c 8 ./backend_http 13579
```

输出：
```
HTTP 后端已启动，监听 13579
```

#### 步骤 2：启动反向代理

```bash
# 绑定到 CPU 0-7，加载配置文件
taskset -c 0-7 ./tcp_proxy -f ../config.xml > /dev/null 2>&1 &
```

> **注意**：将输出重定向到 `/dev/null` 避免日志 `fflush` 拖慢性能。调试时可去掉重定向并加 `-x` 参数开启 DEBUG 日志。

#### 命令行参数

| 参数 | 说明 |
| :--- | :--- |
| `-f <file>` | 指定配置文件（**必需**） |
| `-x` | 开启 DEBUG 级别日志（仅调试用，性能会下降） |
| `-v` | 显示版本号 |
| `-h` | 显示帮助 |

### 5. 功能验证

```bash
# 通过代理访问
curl -v http://127.0.0.1:12345/

# 直连后端对比
curl -v http://127.0.0.1:13579/
```

预期输出：
```
< HTTP/1.1 200 OK
< Server: cpp-backend
< Content-Type: text/plain
< Content-Length: 2
<
OK
```

### 6. 性能压测

#### 单次压测

```bash
# 8 线程，100 并发，持续 30 秒，taskset锁定CPU逻辑核可以根据自己的配置来调整
taskset -c 16-23 wrk -t8 -c100 -d30s --latency http://127.0.0.1:12345/
```

#### 阶梯压测（推荐）

```bash
for c in 50 100 150 200 300; do
    echo "===== 并发 $c ====="
    taskset -c 16-23 wrk -t8 -c$c -d30s --latency http://127.0.0.1:12345/
    sleep 3
done
```

#### 直压后端对比

```bash
taskset -c 16-23 wrk -t8 -c100 -d30s --latency http://127.0.0.1:13579/
```

### 7. 停止服务

```bash
# 优雅停止
pkill tcp_proxy
pkill backend_http
```

---

## 调优建议

压测前建议调整以下系统参数：

```bash
# 提升文件描述符限制
ulimit -n 65535

# 查看当前限制
ulimit -n
```

`taskset` 绑定 CPU 避免调度抖动，同时方便使用监测工具可视化压测结果

| 进程 | 建议绑核 |
| :--- | :--- |
| `tcp_proxy` | CPU 0 ~ 7 |
| `backend_http` | CPU 8 |
| `wrk` | CPU 16 ~ 23 |

---

