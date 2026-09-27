package com.github.lechandonga.hpcgpufairschedulerjava.scheduler.model;

/** 集群节点：容量 + 已分配账目。 */
public final class Node {

    private final String nodeId;
    private final ResourceSpec capacity;
    private ResourceSpec allocated = ResourceSpec.ZERO;
    private volatile NodeStatus status = NodeStatus.UP;

    public Node(String nodeId, ResourceSpec capacity) {
        this.nodeId = nodeId;
        this.capacity = capacity;
    }

    public String nodeId() { return nodeId; }
    public ResourceSpec capacity() { return capacity; }
    public synchronized ResourceSpec allocated() { return allocated; }
    public NodeStatus status() { return status; }
    public void status(NodeStatus s) { this.status = s; }

    public synchronized ResourceSpec free() {
        return capacity.minus(allocated);
    }

    /** 仅当状态为 UP 且剩余容量足够时占用资源，返回是否成功。 */
    public synchronized boolean tryAllocate(ResourceSpec req) {
        if (status != NodeStatus.UP) return false;
        if (!req.fitsWithin(free())) return false;
        allocated = allocated.plus(req);
        return true;
    }

    /** 释放资源；对未占用量的释放直接忽略，保证重复释放幂等。 */
    public synchronized void release(ResourceSpec req) {
        ResourceSpec next = allocated.minus(req);
        allocated = new ResourceSpec(
                Math.max(0, next.gpu()),
                Math.max(0, next.cpuCores()),
                Math.max(0, next.memoryMb()));
    }
}
