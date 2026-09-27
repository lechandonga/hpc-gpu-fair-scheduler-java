package com.github.lechandonga.hpcgpufairschedulerjava.scheduler.exception;

/**
 * 调度相关的拒绝 / 失败，reason 区分具体原因，避免笼统失败。
 */
public class SchedulingException extends RuntimeException {

    public enum Reason {
        DUPLICATE_JOB,        // 重复提交同一 jobId
        INVALID_REQUEST,      // 参数非法（资源 <= 0、队列为空等）
        UNKNOWN_QUEUE,        // 队列不存在
        INSUFFICIENT_RESOURCE,// 请求超过集群可提供的最大单节点容量，永远无法满足
        UNKNOWN_JOB,
        UNKNOWN_NODE,
        ILLEGAL_STATE         // 状态机不允许的操作（如对非运行作业做完成）
    }

    private final Reason reason;

    public SchedulingException(Reason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public Reason getReason() {
        return reason;
    }
}
