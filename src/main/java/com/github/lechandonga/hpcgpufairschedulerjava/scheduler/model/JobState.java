package com.github.lechandonga.hpcgpufairschedulerjava.scheduler.model;

/** 作业生命周期状态。 */
public enum JobState {
    /** 已受理，排队等待资源。 */
    PENDING,
    /** 已分配到节点，占用资源运行。 */
    RUNNING,
    /** 被抢占，资源已回收，回到队列等待重排。 */
    PREEMPTED,
    /** 正常结束，资源已释放。 */
    COMPLETED,
    /** 因节点故障等原因被终止（可重排时不会进入此状态）。 */
    FAILED,
    /** 提交即被拒绝，未进入队列。 */
    REJECTED
}
