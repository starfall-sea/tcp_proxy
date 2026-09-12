cmake_minimum_required(VERSION 3.10)

# 项目名
project(tcp_proxy CXX)

# C++标准，对应原来 -std=c++11
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 编译选项：开启警告，等价原 -Wall
add_compile_options(-Wall)

# 主程序 springsnail
add_executable(tcp_proxy
    main.cpp
    logger.cpp
    fd_wrap.cpp
    connection.cpp
    manager.cpp
)

# 后端测试程序 backend_http，加O2优化
add_executable(backend_http
    backend_http.cpp
)
target_compile_options(backend_http PRIVATE -O2)