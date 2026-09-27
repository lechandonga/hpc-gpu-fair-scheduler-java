package com.github.lechandonga.hpcgpufairschedulerjava.scheduler.model;

/** 节点健康状态。 */
public enum NodeStatus {
    UP,
    /** 主动下线：节点上作业被明确处置（迁移重排或失败）。 */
    DOWN,
    /** 失联：心跳丢失，按故障处理。 */
    UNREACHABLE
}
