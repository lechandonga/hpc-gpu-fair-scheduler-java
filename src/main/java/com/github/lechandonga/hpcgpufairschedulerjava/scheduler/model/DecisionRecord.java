package com.github.lechandonga.hpcgpufairschedulerjava.scheduler.model;

/**
 * 一条调度决策日志，记录决策类型与依据，便于测试断言与排查。
 */
public record DecisionRecord(Type type, String detail) {

    public enum Type {
        SUBMIT_ACCEPTED,
        SUBMIT_REJECTED,
        QUEUE_SELECTED,
        JOB_SCHEDULED,
        JOB_PREEMPTED,
        JOB_REQUEUED,
        JOB_COMPLETED,
        NODE_DOWN,
        NODE_RECOVERED,
        PREEMPTION_SKIPPED
    }

    @Override
    public String toString() {
        return type + ": " + detail;
    }
}
