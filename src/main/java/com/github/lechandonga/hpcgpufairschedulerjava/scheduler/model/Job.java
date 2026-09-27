package com.github.lechandonga.hpcgpufairschedulerjava.scheduler.model;

/**
 * 一个 GPU 作业。submitSeq 由调度器在接收时单调分配，用于确定性排序。
 */
public final class Job {

    private final String jobId;
    private final String queueName;
    private final int priority;          // 数值越大优先级越高
    private final ResourceRequest request;

    private long submitSeq = -1;         // 由调度器赋值
    private JobStatus status = JobStatus.PENDING;
    private String assignedNodeId;       // RUNNING 时非空
    private int preemptionCount;

    public Job(String jobId, String queueName, int priority, ResourceRequest request) {
        this.jobId = jobId;
        this.queueName = queueName;
        this.priority = priority;
        this.request = request;
    }

    public String getJobId() { return jobId; }
    public String getQueueName() { return queueName; }
    public int getPriority() { return priority; }
    public ResourceRequest getRequest() { return request; }

    public long getSubmitSeq() { return submitSeq; }
    public void setSubmitSeq(long submitSeq) { this.submitSeq = submitSeq; }

    public JobStatus getStatus() { return status; }
    public void setStatus(JobStatus status) { this.status = status; }

    public String getAssignedNodeId() { return assignedNodeId; }
    public void setAssignedNodeId(String assignedNodeId) { this.assignedNodeId = assignedNodeId; }

    public int getPreemptionCount() { return preemptionCount; }
    public void incrementPreemptionCount() { this.preemptionCount++; }

    @Override
    public String toString() {
        return "Job{" + jobId + "@" + queueName + ", prio=" + priority
                + ", req=" + request + ", status=" + status
                + (assignedNodeId != null ? ", node=" + assignedNodeId : "") + "}";
    }
}
