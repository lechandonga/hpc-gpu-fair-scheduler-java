package com.github.lechandonga.hpcgpufairschedulerjava.scheduler;

import com.github.lechandonga.hpcgpufairschedulerjava.scheduler.core.Scheduler;
import com.github.lechandonga.hpcgpufairschedulerjava.scheduler.model.JobRequest;
import com.github.lechandonga.hpcgpufairschedulerjava.scheduler.model.Node;
import com.github.lechandonga.hpcgpufairschedulerjava.scheduler.model.ResourceSpec;

/** 测试公共构造工具。 */
final class SchedulerTestSupport {

    private SchedulerTestSupport() {}

    /** 两队列（权重 2:1）、两节点（各 4 GPU / 16 核 / 64G）的标准集群。 */
    static Scheduler standardCluster() {
        Scheduler s = new Scheduler();
        s.addQueue("alpha", 2.0);
        s.addQueue("beta", 1.0);
        s.addNode(new Node("node-1", new ResourceSpec(4, 16, 64_000)));
        s.addNode(new Node("node-2", new ResourceSpec(4, 16, 64_000)));
        return s;
    }

    static JobRequest req(String jobId, String queue, int priority, int gpu) {
        return new JobRequest(jobId, queue, priority, new ResourceSpec(gpu, 4, 8_000));
    }
}
