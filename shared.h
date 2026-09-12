#ifndef SHARED_H
#define SHARED_H

// 最大进程数
static const int MAX_PROCESS_NUMBER = 16;

// 父子进程共享内存结构
struct SharedData
{
    volatile int busy_ratio[MAX_PROCESS_NUMBER];   // 每个 worker 的实时负载
    volatile int global_need_retry;                // 是否有 worker 需要父进程重新仲裁
};

#endif