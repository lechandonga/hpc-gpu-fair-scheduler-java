package com.github.lechandonga.hpcgpufairschedulerjava.scheduler.model;

/** 提交被拒绝的原因，彼此可区分。 */
public enum RejectionReason {
    /** 相同 jobId 已存在（幂等去重）。 */
    DUPLICATE_JOB,
    /** 参数非法：资源请求非正、超出单节点上限、优先级非法等。 */
    INVALID_REQUEST,
    /** 目标队列不存在。 */
    UNKNOWN_QUEUE,
    /** 集群（含未来可抢占空间）也无法满足的全有或全无放置失败。 */
    INSUFFICIENT_RESOURCES
}
