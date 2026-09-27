package com.github.lechandonga.hpcgpufairschedulerjava.scheduler.model;

/**
 * 作业状态机：
 * PENDING -> RUNNING -> COMPLETED / FAILED
 * RUNNING -> PREEMPTED -> PENDING（被抢占后排回队列）
 * RUNNING -> PENDING（节点故障重排）
 */
public enum JobStatus {
    PENDING,
    RUNNING,
    PREEMPTED,
    COMPLETED,
    FAILED
}
