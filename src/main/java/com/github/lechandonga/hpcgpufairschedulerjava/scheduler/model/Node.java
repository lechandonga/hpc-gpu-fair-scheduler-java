package com.github.lechandonga.hpcgpufairschedulerjava.scheduler.model;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * 一个计算节点，持有总容量与已分配量，记录当前运行的作业集合。
 */
public final class Node {

    private final String nodeId;
    private final ResourceRequest capacity;
    private ResourceRequest allocated = ResourceRequest.ZERO;
    private NodeStatus status = NodeStatus.UP;
    private final Set<String> runningJobIds = new LinkedHashSet<>();

    public Node(String nodeId, ResourceRequest capacity) {
        this.nodeId = nodeId;
        this.capacity = capacity;
    }

    public String getNodeId() { return nodeId; }
    public ResourceRequest getCapacity() { return capacity; }
    public ResourceRequest getAllocated() { return allocated; }

    public NodeStatus getStatus() { return status; }
    public void setStatus(NodeStatus status) { this.status = status; }

    public ResourceRequest free() {
        return capacity.minus(allocated);
    }

    public boolean canFit(ResourceRequest need) {
        return status == NodeStatus.UP && free().canFit(need);
    }

    public void allocate(String jobId, ResourceRequest req) {
        if (!canFit(req)) {
            throw new IllegalStateException("node " + nodeId + " cannot fit " + req);
        }
        allocated = allocated.plus(req);
        runningJobIds.add(jobId);
    }

    public void release(String jobId, ResourceRequest req) {
        if (!runningJobIds.remove(jobId)) {
            throw new IllegalStateException("job " + jobId + " not running on node " + nodeId);
        }
        allocated = allocated.minus(req);
    }

    public Set<String> getRunningJobIds() {
        return Collections.unmodifiableSet(runningJobIds);
    }

    @Override
    public String toString() {
        return "Node{" + nodeId + ", status=" + status + ", alloc=" + allocated
                + "/" + capacity + "}";
    }
}
