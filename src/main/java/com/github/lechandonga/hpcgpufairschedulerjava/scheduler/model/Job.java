package com.github.lechandonga.hpcgpufairschedulerjava.scheduler.model;

import java.util.concurrent.atomic.AtomicLong;

/** 运行时作业实例。 */
public final class Job {

    private static final AtomicLong SEQ = new AtomicLong();

    private final JobRequest request;
    /** 单调递增的受理序号，用于确定性的排队次序（同优先级先到先排）。 */
    private final long sequence;
    private volatile JobState state;
    /** 运行所在节点，未分配时为 null。 */
    private volatile String nodeId;
    /** 抢占代次：每次抢占递增，用于识别重复的抢占触发，保证幂等。 */
    private volatile int preemptionEpoch;

    public Job(JobRequest request) {
        this.request = request;
        this.sequence = SEQ.getAndIncrement();
        this.state = JobState.PENDING;
        this.preemptionEpoch = 0;
    }

    public JobRequest request() { return request; }
    public String jobId() { return request.jobId(); }
    public String queue() { return request.queue(); }
    public int priority() { return request.priority(); }
    public ResourceSpec resources() { return request.resources(); }
    public long sequence() { return sequence; }
    public JobState state() { return state; }
    public void state(JobState s) { this.state = s; }
    public String nodeId() { return nodeId; }
    public void nodeId(String n) { this.nodeId = n; }
    public int preemptionEpoch() { return preemptionEpoch; }
    public void preemptionEpoch(int e) { this.preemptionEpoch = e; }
}
